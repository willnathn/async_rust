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

// due to orphan rule (must have created type or trait in this crate) I can't impl Future just
// because IoFuture is implemented. Let's use a macro instead.

#[macro_export]
macro_rules! impl_io_future {
    impl std::future::Future for $type {

        type Output = Result<$output, ReactorError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            let reactor = get_local_reactor();
            match self.job_id() {
                Some(job_id) => return Poll::Ready(self.process_result(ex.get_result(job_id))),
                None => {
                    let sqe = self.make_sqe();
                    let task_id = unsafe {
                        let executor = crate::executor::get_local_executor();
                        executor.ready_tasks.last().copied().unwrap()
                    };
                    self.set_job_id(ex.enqueue_sqe(sqe)?);
                    Poll::Pending
                }
            }
        }
}
}
