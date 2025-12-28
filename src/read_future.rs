use crate::reactor::{get_local_reactor, Reactor, ReactorError};
use crate::uring::{io_uring_ptr, io_uring_sqe, IoringOp, UringError};
use std::ffi::c_void;
use std::future::Future;
use std::os::fd::{AsRawFd, OwnedFd};
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct ReadFuture {
    fd: OwnedFd,
    buf: Box<[u8]>,
    offset: u64,
    job_id: Option<usize>,
}

// how does ownershup work? Since I own the fd noone can do another operation at the same time?
impl ReadFuture {
    pub fn new(fd: OwnedFd, offset: u64, size: usize) -> Self {
        ReadFuture {
            fd,
            buf: vec![0u8; size].into_boxed_slice(),
            offset,
            job_id: None,
        }
    }

    #[inline]
    fn make_sqe(&self) -> io_uring_sqe {
        let mut sqe: io_uring_sqe = unsafe { std::mem::zeroed() };

        sqe.opcode = IoringOp::Read;
        sqe.fd = self.fd.as_raw_fd();
        sqe.addr_or_splice_off_in.addr.ptr = self.buf.as_ptr() as *mut c_void;
        sqe.len.len = self.buf.len() as u32;
        sqe.off_or_addr2.off = self.offset;
        sqe
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

impl Future for ReadFuture {
    type Output = Result<Vec<u8>, ReactorError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let ex = get_local_reactor();
        match self.job_id {
            Some(job_id) => {
                match ex.poll_for_completion(job_id) {
                    Some(res) => {
                        // todo: turn i32 -> Result
                        return Poll::Ready(self.process_result(res));
                    }
                    None => return Poll::Pending,
                };
            }
            None => {
                let sqe = self.make_sqe();
                self.job_id = Some(ex.enqueue_sqe(sqe)?);
                Poll::Pending
            }
        }
    }
}
