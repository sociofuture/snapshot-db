//! One contiguous memory region, sized once at startup. Allocation is a bump pointer;
//! nothing is ever freed individually. Running out is an error (restart with a bigger
//! `--memory` and redo).
//!
//! The region never moves or shrinks, so a range that is finished (a loaded table) can be
//! handed to Arrow as a zero-copy `Buffer`: the buffer keeps the whole mapping alive.

use std::io;
use std::ops::Range;
use std::ptr::NonNull;
use std::sync::Arc;

use datafusion::arrow::buffer::Buffer;
use memmap2::MmapMut;

pub const ALIGN: usize = 64;

pub struct Arena {
    /// Keeps the mapping alive for as long as any Arrow buffer points into it.
    mem: Arc<MmapMut>,
    base: *mut u8,
    len: usize,
    used: usize,
}

// SAFETY: `base` points into `mem`, which is owned by the Arena and never moved. Writers only
// touch ranges that are not yet published; published ranges are only read.
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

#[derive(Debug)]
pub struct ArenaFull {
    pub needed: usize,
    pub free: usize,
}

impl std::fmt::Display for ArenaFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "out of memory: need {} bytes, {} bytes free (restart with a larger --memory)",
            self.needed, self.free
        )
    }
}

impl Arena {
    /// Reserves `size` bytes. Pages are committed lazily by the OS on first touch,
    /// and are zero-filled.
    pub fn new(size: usize) -> io::Result<Self> {
        let mut map = MmapMut::map_anon(size)?;
        let base = map.as_mut_ptr();
        Ok(Arena { mem: Arc::new(map), base, len: size, used: 0 })
    }

    /// Marks the first `used` bytes as allocated (after the image has been restored into them).
    pub fn set_used(&mut self, used: usize) {
        assert!(used <= self.len);
        self.used = used;
    }

    pub fn capacity(&self) -> usize {
        self.len
    }

    pub fn used(&self) -> usize {
        self.used
    }

    /// Reserves `len` bytes, 64-byte aligned.
    pub fn alloc(&mut self, len: usize) -> Result<Range<usize>, ArenaFull> {
        let start = self.used.next_multiple_of(ALIGN);
        let end = start
            .checked_add(len)
            .filter(|&e| e <= self.len)
            .ok_or(ArenaFull { needed: len, free: self.len.saturating_sub(start) })?;
        self.used = end;
        Ok(start..end)
    }

    pub fn slice(&self, r: &Range<usize>) -> &[u8] {
        assert!(r.start <= r.end && r.end <= self.len);
        // SAFETY: in bounds; the mapping lives as long as `self`.
        unsafe { std::slice::from_raw_parts(self.base.add(r.start), r.end - r.start) }
    }

    pub fn slice_mut(&mut self, r: &Range<usize>) -> &mut [u8] {
        assert!(r.start <= r.end && r.end <= self.len);
        // SAFETY: in bounds; `&mut self` makes this the only access through the Arena.
        unsafe { std::slice::from_raw_parts_mut(self.base.add(r.start), r.end - r.start) }
    }

    /// Zero-copy Arrow buffer over a finished range. The caller must not write to the range
    /// afterwards (loaded tables are immutable).
    pub fn buffer(&self, r: &Range<usize>) -> Buffer {
        assert!(r.start <= r.end && r.end <= self.len);
        // SAFETY: in bounds and non-null; `owner` keeps the mapping alive for the buffer's life.
        unsafe {
            let ptr = NonNull::new_unchecked(self.base.add(r.start));
            Buffer::from_custom_allocation(ptr, r.end - r.start, self.mem.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_is_aligned_and_bounded() {
        let mut a = Arena::new(1024).unwrap();
        let r1 = a.alloc(10).unwrap();
        let r2 = a.alloc(10).unwrap();
        assert_eq!(r1.start % ALIGN, 0);
        assert_eq!(r2.start % ALIGN, 0);
        assert!(r2.start >= r1.end);
        let err = a.alloc(2000).unwrap_err();
        assert_eq!(err.needed, 2000);
    }

    #[test]
    fn buffer_outlives_arena_and_sees_the_data() {
        let mut a = Arena::new(4096).unwrap();
        let r = a.alloc(4).unwrap();
        a.slice_mut(&r).copy_from_slice(&[1, 2, 3, 4]);
        let b = a.buffer(&r);
        drop(a);
        assert_eq!(b.as_slice(), &[1, 2, 3, 4]);
    }
}
