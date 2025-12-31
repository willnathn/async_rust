use rustix::io::Errno;
use rustix::io_uring::{
    IORING_OFF_CQ_RING, IORING_OFF_SQ_RING, IORING_OFF_SQES, IoringEnterFlags, io_cqring_offsets,
    io_sqring_offsets, io_uring_enter, io_uring_params, io_uring_setup,
};
pub use rustix::io_uring::{IoringOp, io_uring_cqe, io_uring_ptr, io_uring_sqe};
use rustix::mm::{MapFlags, ProtFlags, mmap, munmap};
use std::ffi::c_void;
use std::mem::size_of;
use std::os::fd::{AsFd, OwnedFd};
use std::{ptr, u32};
use thiserror::Error;

// the point is to add to a user space queue, use 1 syscall to start processing on all and then
// read ready ones in batches. This eventually will become an internal to a UringRing which wraps
// _some_ syscalls and be pub(crate).

#[derive(Debug, Error)]
pub enum UringError {
    #[error("Submission queue is full")]
    QueueFull,
    #[error("No completions available")]
    NoCompletions,
    #[error("IO error: {0}")]
    IoError(#[from] rustix::io::Errno),
}

#[derive(Debug)]
pub struct UringRing {
    fd: OwnedFd,
    sq_head: *mut u32,
    sq_tail: *mut u32,
    sq_ring_mask: *const u32,
    sq_ring_entries: *const u32,
    sq_array: *mut u32,

    sqes: *mut io_uring_sqe,

    cq_head: *mut u32,
    cq_tail: *mut u32,
    cq_ring_mask: *const u32,
    cq_ring_entries: *const u32,
    cq_overflow: *const u32,
    cqes: *const io_uring_cqe,
    // for dropping - if we leak this is it a speed up?
    sq_ptr: *mut u8,
    sqe_ptr: *mut u8,
    cq_ptr: *mut u8,
    sq_mmap_size: usize,
    sqe_mmap_size: usize,
    cq_mmap_size: usize,
}

impl UringRing {
    pub fn new(entries: u32) -> Result<Self, UringError> {
        let mut params = io_uring_params::default();
        let fd = unsafe { io_uring_setup(entries, &mut params)? };
        let sq_off: io_sqring_offsets = params.sq_off;
        let cq_off: io_cqring_offsets = params.cq_off;

        // addr 0 means kernel chooses where our pointer goes. The sq ring buffer is an array of however many sq_entries we added * u32, offsetted by the preamble before the array (head,tail etc.). All these are copied from the man pages. the read, write protflags simply state we can read and write to this memory. the shared memflag means the kernel sees our writes, as opposed to private which is copy-on-write. Populate means we don't lazily get page faults, we get it upfront.
        // sq_off.array (points to start of array)
        let sq_mmap_size = sq_off.array as usize + params.sq_entries as usize * size_of::<u32>();
        let sq_ptr = unsafe {
            mmap(
                ptr::null_mut(),
                sq_mmap_size,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED | MapFlags::POPULATE,
                fd.as_fd(),
                IORING_OFF_SQ_RING,
            )?
        } as *mut u8;

        let sqe_mmap_size = params.sq_entries as usize * size_of::<io_uring_sqe>();
        let sqe_ptr = unsafe {
            mmap(
                ptr::null_mut(),
                sqe_mmap_size,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED | MapFlags::POPULATE,
                fd.as_fd(),
                IORING_OFF_SQES,
            )?
        } as *mut u8;

        let cq_mmap_size =
            cq_off.cqes as usize + params.cq_entries as usize * size_of::<io_uring_cqe>();
        let cq_ptr = unsafe {
            mmap(
                ptr::null_mut(),
                cq_mmap_size,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED | MapFlags::POPULATE,
                fd.as_fd(),
                IORING_OFF_CQ_RING,
            )?
        } as *mut u8;
        unsafe {
            Ok(UringRing {
                fd,
                sq_head: sq_ptr.add(sq_off.head as usize) as *mut u32,
                sq_tail: sq_ptr.add(sq_off.tail as usize) as *mut u32,
                sq_ring_mask: sq_ptr.add(sq_off.ring_mask as usize) as *const u32,
                sq_ring_entries: sq_ptr.add(sq_off.ring_entries as usize) as *const u32,
                sq_array: sq_ptr.add(sq_off.array as usize) as *mut u32,
                sqes: sqe_ptr as *mut io_uring_sqe,
                cq_head: cq_ptr.add(cq_off.head as usize) as *mut u32,
                cq_tail: cq_ptr.add(cq_off.tail as usize) as *mut u32,
                cq_ring_mask: cq_ptr.add(cq_off.ring_mask as usize) as *const u32,
                cq_ring_entries: cq_ptr.add(cq_off.ring_entries as usize) as *const u32,
                cq_overflow: cq_ptr.add(cq_off.overflow as usize) as *const u32,
                cqes: cq_ptr.add(cq_off.cqes as usize) as *const io_uring_cqe,
                sq_ptr,
                sqe_ptr,
                cq_ptr,
                sq_mmap_size,
                sqe_mmap_size,
                cq_mmap_size,
            })
        }
    }
}

impl Drop for UringRing {
    fn drop(&mut self) {
        unsafe {
            let _ = munmap(self.cq_ptr as *mut c_void, self.cq_mmap_size);
            let _ = munmap(self.sqe_ptr as *mut c_void, self.sqe_mmap_size);
            let _ = munmap(self.sq_ptr as *mut c_void, self.sq_mmap_size);
        }
    }
}

impl UringRing {
    fn cq_ready(&self) -> u32 {
        unsafe { self.cq_tail.read_volatile() - self.cq_head.read_volatile() }
    }

    fn sq_space_left(&mut self) -> u32 {
        unsafe {
            *self.sq_ring_entries - (self.sq_tail.read_volatile() - self.sq_head.read_volatile())
        }
    }

    pub fn enqueue_sqe(&mut self, sqe: io_uring_sqe) -> Result<(), UringError> {
        if self.sq_space_left() == 0 {
            return Err(UringError::QueueFull);
        }
        let tail = unsafe { self.sq_tail.read_volatile() };
        let i = unsafe { tail & *self.sq_ring_mask };
        let sqe_ptr = unsafe { &mut *self.sqes.add(i as usize) };
        *sqe_ptr = sqe;
        unsafe { self.sq_array.add(i as usize).write_volatile(i) };
        unsafe { self.sq_tail.write_volatile(tail + 1) };

        Ok(())
    }

    pub fn submit(&mut self) -> Result<usize, UringError> {
        unsafe {
            Ok(io_uring_enter(self.fd.as_fd(), u32::MAX, 1, IoringEnterFlags::GETEVENTS)? as usize)
        }
    }

    // combine the peek and advance of cqe as for our use case we want them together. Maybe get an
    // iterator here? What is the point though.
    pub fn get_cqe_batch(&self) -> Result<Vec<io_uring_cqe>, UringError> {
        let ready_count = self.cq_ready();
        if ready_count == 0 {
            return Ok(Vec::new());
        }
        let head = unsafe { self.cq_head.read_volatile() };
        let mask = unsafe { *self.cq_ring_mask };
        let mut batch = Vec::with_capacity(ready_count as usize);
        for i in 0..ready_count {
            let index = ((head + i) & mask) as usize;
            batch.push(unsafe { self.cqes.add(index).read() });
        }
        unsafe { self.cq_head.write_volatile(head + ready_count) };
        Ok(batch)
    }
}
