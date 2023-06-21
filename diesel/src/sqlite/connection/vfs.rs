extern crate libsqlite3_sys as ffi;

use std::io::{self, ErrorKind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::memory::Memory;
use sqlite_vfs::{LockKind, OpenKind, OpenOptions, Vfs};

const SQLITE_SIZE_IN_BYTES: u64 = 8; // 8 byte

const WASM_PAGE_SIZE_IN_BYTES: u64 = 64 * 1024; // 64KB

#[derive(Default, Debug)]
pub(crate) struct PagesVfs<T: Memory> {
    lock_state: Arc<Mutex<LockState>>,
    memory: T,
}

#[derive(Debug, Default)]
struct LockState {
    read: usize,
    write: Option<bool>,
}

#[derive(Debug)]
pub(crate) struct Connection<T: Memory> {
    lock_state: Arc<Mutex<LockState>>,
    lock: LockKind,
    memory: T,
}

impl<T> Vfs for PagesVfs<T>
where
    T: Memory + std::fmt::Debug,
{
    type Handle = Connection<T>;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, io::Error> {
        // Always open the same database for now.
        if db != "main.db" {
            return Err(io::Error::new(
                ErrorKind::NotFound,
                format!("unexpected database name `{}`; expected `main.db`", db),
            ));
        }
        // Only main databases supported right now (no journal, wal, temporary, ...)
        if opts.kind != OpenKind::MainDb {
            return Err(io::Error::new(
                ErrorKind::PermissionDenied,
                "only main database supported right now (no journal, wal, ...)",
            ));
        }

        let conn = Connection {
            lock_state: self.lock_state.clone(),
            lock: LockKind::None,
            memory: self.memory.clone(),
        };

        Ok(conn)
    }

    fn delete(&self, _db: &str) -> Result<(), io::Error> {
        Ok(())
    }

    fn exists(&self, db: &str) -> Result<bool, io::Error> {
        Ok(db == "main.db" && self.memory.size() > 0)
    }

    fn temporary_name(&self) -> String {
        String::from("main.db")
    }

    fn random(&self, buffer: &mut [i8]) {
        let mut rng = ffi::Rand::new();
        rng.fill_i8(buffer);
    }

    fn sleep(&self, duration: Duration) -> Duration {
        duration
        // let now = Instant::now();
        // conn_sleep((duration.as_millis() as u32).max(1));
        // now.elapsed()
    }
}

impl<T> sqlite_vfs::DatabaseHandle for Connection<T>
where
    T: Memory,
{
    type WalIndex = sqlite_vfs::WalDisabled;

    fn size(&self) -> Result<u64, io::Error> {
        Ok(self.db_size())
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), io::Error> {
        if self.memory.size() > 0 {
            self.memory.read(offset + SQLITE_SIZE_IN_BYTES, buf);
        }
        Ok(())
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), io::Error> {
        let size = offset + buf.len() as u64;
        if size > self.db_size() {
            self.memory.write(0, &size.to_be_bytes());
        }
        self.memory.write(offset + SQLITE_SIZE_IN_BYTES, buf);
        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), io::Error> {
        // Everything is directly written to storage, so no extra steps necessary to sync.
        Ok(())
    }

    fn set_len(&mut self, size: u64) -> Result<(), io::Error> {
        let capacity = if self.memory.size() == 0 {
            0
        } else {
            self.stable_capacity() - SQLITE_SIZE_IN_BYTES
        };

        if size > capacity {
            self.stable_grow_bytes(size - capacity)?;
            self.memory.write(0, &size.to_be_bytes());
        }
        Ok(())
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, io::Error> {
        let ok = Self::lock(self, lock);
        Ok(ok)
    }

    fn reserved(&mut self) -> Result<bool, io::Error> {
        Ok(Self::reserved(self))
    }

    fn current_lock(&self) -> Result<LockKind, io::Error> {
        Ok(self.lock)
    }

    fn wal_index(&self, _readonly: bool) -> Result<Self::WalIndex, io::Error> {
        Ok(sqlite_vfs::WalDisabled::default())
    }

    fn unlock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        let ok = Self::lock(self, lock);
        Ok(ok)
    }

    fn set_chunk_size(&self, _chunk_size: usize) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn moved(&self) -> Result<bool, std::io::Error> {
        Ok(false)
    }
}

impl<T: Memory> Connection<T> {
    /// Gets capacity of the stable memory in bytes.
    fn stable_capacity(&self) -> u64 {
        self.memory.size() << 16
    }

    /// Attempts to grow the memory by adding new pages.
    fn stable_grow_bytes(&self, size: u64) -> Result<u64, io::Error> {
        let added_pages = (size as f64 / WASM_PAGE_SIZE_IN_BYTES as f64).ceil() as u64;
        let g = self.memory.grow(added_pages);
        if g == -1 {
            Err(io::Error::new(io::ErrorKind::OutOfMemory, "out of memory"))
        } else {
            Ok(g.try_into().unwrap())
        }
    }
    fn db_size(&self) -> u64 {
        if self.memory.size() == 0 {
            return 0;
        }
        let mut buf = [0u8; SQLITE_SIZE_IN_BYTES as usize];
        self.memory.read(0, &mut buf);
        u64::from_be_bytes(buf)
    }

    fn lock(&mut self, to: LockKind) -> bool {
        if self.lock == to {
            return true;
        }

        let mut lock_state = self.lock_state.lock().unwrap();

        match to {
            LockKind::None => {
                if self.lock == LockKind::Shared {
                    lock_state.read -= 1;
                } else if self.lock > LockKind::Shared {
                    lock_state.write = None;
                }
                self.lock = LockKind::None;
                true
            }

            LockKind::Shared => {
                if lock_state.write == Some(true) && self.lock <= LockKind::Shared {
                    return false;
                }

                lock_state.read += 1;
                if self.lock > LockKind::Shared {
                    lock_state.write = None;
                }
                self.lock = LockKind::Shared;
                true
            }

            LockKind::Reserved => {
                if lock_state.write.is_some() || self.lock != LockKind::Shared {
                    return false;
                }

                if self.lock == LockKind::Shared {
                    lock_state.read -= 1;
                }
                lock_state.write = Some(false);
                self.lock = LockKind::Reserved;
                true
            }

            LockKind::Pending => {
                // cannot be requested directly
                false
            }

            LockKind::Exclusive => {
                if lock_state.write.is_some() && self.lock <= LockKind::Shared {
                    return false;
                }

                if self.lock == LockKind::Shared {
                    lock_state.read -= 1;
                }

                lock_state.write = Some(true);
                if lock_state.read == 0 {
                    self.lock = LockKind::Exclusive;
                    true
                } else {
                    self.lock = LockKind::Pending;
                    false
                }
            }
        }
    }

    fn reserved(&self) -> bool {
        if self.lock > LockKind::Shared {
            return true;
        }

        let lock_state = self.lock_state.lock().unwrap();
        lock_state.write.is_some()
    }
}

impl<T: Memory> Drop for Connection<T> {
    fn drop(&mut self) {
        if self.lock != LockKind::None {
            self.lock(LockKind::None);
        }
    }
}

fn conn_sleep(ms: u32) {
    std::thread::sleep(Duration::from_secs(ms.into()));
}
