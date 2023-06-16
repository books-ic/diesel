#![allow(unsafe_code)]

use std::sync::{Arc, RwLock};

#[link(wasm_import_module = "ic0")]
extern "C" {
    pub(crate) fn stable64_size() -> u64;
    pub(crate) fn stable64_grow(additional_pages: u64) -> i64;
    pub(crate) fn stable64_read(dst: u64, offset: u64, size: u64);
    pub(crate) fn stable64_write(offset: u64, src: u64, size: u64);
}

#[derive(Clone, Copy, Default, Debug)]
pub(crate) struct Ic0StableMemory;

impl Memory for Ic0StableMemory {
    fn size(&self) -> u64 {
        // SAFETY: This is safe because of the ic0 api guarantees.
        unsafe { stable64_size() }
    }

    fn grow(&self, pages: u64) -> i64 {
        // SAFETY: This is safe because of the ic0 api guarantees.
        unsafe { stable64_grow(pages) }
    }

    fn read(&self, offset: u64, dst: &mut [u8]) {
        // SAFETY: This is safe because of the ic0 api guarantees.
        unsafe { stable64_read(dst.as_ptr() as u64, offset, dst.len() as u64) }
    }

    fn write(&self, offset: u64, src: &[u8]) {
        // SAFETY: This is safe because of the ic0 api guarantees.
        unsafe { stable64_write(offset, src.as_ptr() as u64, src.len() as u64) }
    }
}

pub(crate) trait Memory: Sync + Clone {
    /// Returns the current size of the stable memory in WebAssembly
    /// pages. (One WebAssembly page is 64Ki bytes.)
    fn size(&self) -> u64;

    /// Tries to grow the memory by new_pages many pages containing
    /// zeroes.  If successful, returns the previous size of the
    /// memory (in pages).  Otherwise, returns -1.
    fn grow(&self, pages: u64) -> i64;

    /// Copies the data referred to by offset out of the stable memory
    /// and replaces the corresponding bytes in dst.
    fn read(&self, offset: u64, dst: &mut [u8]);

    /// Copies the data referred to by src and replaces the
    /// corresponding segment starting at offset in the stable memory.
    fn write(&self, offset: u64, src: &[u8]);
}

const WASM_PAGE_SIZE: u64 = 65536;

const MAX_PAGES: u64 = i64::MAX as u64 / WASM_PAGE_SIZE;

/// A `Memory` that is based on a vector.
#[derive(Clone, Debug)]
pub(crate) struct VectorMemory(Arc<RwLock<Vec<u8>>>);

impl Default for VectorMemory {
    fn default() -> Self {
        let buffer: Vec<u8> = vec![0; 20];
        Self(Arc::new(RwLock::new(buffer)))
    }
}

impl Memory for VectorMemory {
    fn size(&self) -> u64 {
        self.0.read().unwrap().len() as u64 / WASM_PAGE_SIZE
    }

    fn grow(&self, pages: u64) -> i64 {
        let size = self.size();
        match size.checked_add(pages) {
            Some(n) => {
                if n > MAX_PAGES {
                    return -1;
                }
                self.0
                    .write()
                    .unwrap()
                    .resize((n * WASM_PAGE_SIZE) as usize, 0);
                size as i64
            }
            None => -1,
        }
    }

    fn read(&self, offset: u64, dst: &mut [u8]) {
        let n = offset
            .checked_add(dst.len() as u64)
            .expect("read: out of bounds");

        if n as usize > self.0.read().unwrap().len() {
            panic!("read: out of bounds");
        }

        dst.copy_from_slice(&self.0.read().unwrap()[offset as usize..n as usize]);
    }

    fn write(&self, offset: u64, src: &[u8]) {
        let n = offset
            .checked_add(src.len() as u64)
            .expect("write: out of bounds");

        if n as usize > self.0.read().unwrap().len() {
            panic!("write: out of bounds");
        }
        self.0.write().unwrap()[offset as usize..n as usize].copy_from_slice(src);
    }
}
