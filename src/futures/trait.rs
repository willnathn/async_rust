use crate::executor::TaskHandle;
use crate::reactor::{Reactor, ReactorError, ReactorJobHandle, get_local_reactor};
use crate::uring::{IoringOp, UringError, io_uring_ptr, io_uring_sqe};
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
    ($type:ty=>$output:ty) => {
        impl std::future::Future for $type {
            type Output = Result<$output, ReactorError>;

            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context,
            ) -> std::task::Poll<Self::Output> {
                let reactor = $crate::reactor::get_local_reactor();
                match self.job_id() {
                    Some(job_id) => {
                        return std::task::Poll::Ready(
                            self.process_result(reactor.get_result(job_id)),
                        );
                    }
                    None => {
                        let sqe = self.make_sqe();
                        self.set_job_id(reactor.enqueue_sqe(sqe, cx.waker().clone())?);
                        std::task::Poll::Pending
                    }
                }
            }
        }
    };
}
