use crate::futures::IoFuture;
use crate::reactor::{ReactorError, ReactorJobHandle};
use crate::uring::{IoringOp, UringError, io_uring_sqe};
use rustix::fd::RawFd;
use std::ffi::c_void;

pub struct ReadFuture {
    fd: RawFd,
    buf: Box<[u8]>,
    offset: u64,
    job_id: Option<ReactorJobHandle>,
}

impl ReadFuture {
    pub fn new(fd: RawFd, offset: u64, size: usize) -> Self {
        ReadFuture {
            fd,
            buf: vec![0u8; size].into_boxed_slice(),
            offset,
            job_id: None,
        }
    }
}

impl IoFuture<Vec<u8>> for ReadFuture {
    fn make_sqe(&self) -> io_uring_sqe {
        let mut sqe: io_uring_sqe = unsafe { std::mem::zeroed() };
        sqe.opcode = IoringOp::Read;
        sqe.fd = self.fd;
        sqe.addr_or_splice_off_in.addr.ptr = self.buf.as_ptr() as *mut c_void;
        sqe.len.len = self.buf.len() as u32;
        sqe.off_or_addr2.off = self.offset;
        sqe
    }

    fn job_id(&self) -> Option<ReactorJobHandle> {
        self.job_id
    }

    fn set_job_id(&mut self, id: ReactorJobHandle) {
        self.job_id = Some(id);
    }

    fn process_result(&mut self, res: i32) -> Result<Vec<u8>, ReactorError> {
        if res < 0 {
            return Err(UringError::IoError(rustix::io::Errno::from_raw_os_error(-res)).into());
        }
        let bytes_read = res as usize;
        let buf = std::mem::take(&mut self.buf);
        let mut vec = buf.into_vec();
        vec.truncate(bytes_read);
        Ok(vec)
    }
}

impl_io_future!(ReadFuture => Vec<u8>);
