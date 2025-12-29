use crate::reactor::{get_local_reactor, Reactor, ReactorError, ReactorJobHandle};
use crate::uring::{io_uring_ptr, io_uring_sqe, IoringOp, UringError};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub trait IoFuture<T> {
    fn make_sqe(&self) -> io_uring_sqe;
    fn job_id(&self) -> Option<ReactorJobHandle>;
    fn set_job_id(&mut self, id: ReactorJobHandle);
    fn process_result(&mut self, res: i32) -> Result<T, ReactorError>;
}

impl<T, F> Future for F
where
    F: IoFuture<T>,
{
    type Output = Result<T, ReactorError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let ex = get_local_reactor();
        match self.job_id() {
            Some(job_id) => {
                match ex.poll_for_completion(job_id) {
                    Some(res) => {
                        // todo: turn i32 -> Result maybe?
                        return Poll::Ready(self.process_result(res));
                    }
                    None => return Poll::Pending,
                };
            }
            None => {
                let sqe = self.make_sqe();
                self.set_job_id(ex.enqueue_sqe(sqe)?);
                Poll::Pending
            }
        }
    }
}
