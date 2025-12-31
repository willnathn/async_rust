// rust provides Futures, Wakers and await syntax.  A Future has a poll that returns pending or
// result.
//
// For this async function, rust does:
// async fn parent() {
//     do_a().await;
//     do_b().await;
// }
//
// // the compiler then makes something like (but different) to below. This is clearly wrong as Futures are different
// sizes so can't go in a vec. Also they would handle results properly, so the struct would be an
// enum.:
// struct ParentFuture {
//     state: usize,
//     futures: Vec<Future>,
// }
//
// impl Future for ParentFuture {
//     fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
//         // note here the waker is cx.waker, so we pass the waker down!
//         match self.futures[state].poll(cx) {
//             Poll::Pending => return Poll::Pending,
//             Poll::Ready(res) => {
//                 if self.state >= futures.len() - 1 {
//                     return Poll::Ready(res);
//                 }
//                 self.state = state + 1;
//                 return Poll::Pending;
//             }
//         }
//     }
// }
//
// the cx here contains a waker. Our reactor can call waker.wake(), which will add a task to a ready queue. We write waker.wake()!The executor then will call the poll method of the root future for all the tasks which are ready.
//
// The bit of the puzzle that confused me the most is how the ChildFuture has the Parents waker
// we will have a spawn_task and waker that does something like:
//
// struct MyWaker{
//  task_id:usize
//  }
//  impl MyWaker {
//      pub fn new(task_id:usize) {
//          let ex = get_executor();
//  }
//  impl Wake for MyWaker {
//      fn wake(self:Arc<Self>) {
//          let ex = get_executor();
//          ex.add_to_ready_queue(self.task_id);
//      }
//  }
//
//  fn spawn_task<T>(future: impl Future<Output=T> + 'static) {
//      let ex = get_executor;
//      let task_id = ex.get_next_task_id();
//      waker = MyWaker {task_id};
//      let task = Task {
//          id: task_id,
//          waker,
//          ...
//      }
//      ex.add_task(task);
//  }
//
//  this means all we need to do is keep on passing the waker around!
//
// The waker is our interface with the compiler generated root futures. It's slightly annoying, we
// don't need Arc, but we can use RawWaker ourselves.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use thiserror::Error;

use crate::{
    reactor::{Reactor, ReactorError},
    waker::TaskWaker,
};

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Exceeded max concurrent tasks")]
    TooManyTasksInProgress,
    #[error("Reactor error: {0}")]
    ReactorError(#[from] ReactorError),
    #[error("We received a None or a Some in the array when we shouldn't have")]
    OptionError,
}

#[derive(Debug, Clone, Copy)]
pub struct TaskHandle {
    index: usize,
}

impl TaskHandle {
    fn new(index: usize) -> Self {
        TaskHandle { index }
    }
    pub fn index(&self) -> usize {
        return self.index;
    }
}

//TODO: think about lifetimes here
type BoxFuture = Pin<Box<dyn Future<Output = ()>>>;

const MAX_CONCURRENT_TASKS: usize = 2048;

#[thread_local]
static mut LOCAL_EXECUTOR: *mut Executor = std::ptr::null_mut();

fn create_waker(task_handle: TaskHandle) -> Waker {
    Waker::from(Arc::new(TaskWaker::new(task_handle)))
}

pub unsafe fn make_executor() -> Result<(), ExecutorError> {
    if !LOCAL_EXECUTOR.is_null() {
        return Err(ExecutorError::TooManyTasksInProgress);
    }
    LOCAL_EXECUTOR = Box::leak(Box::new(Executor::new()?));
    Ok(())
}

pub unsafe fn get_local_executor() -> &'static mut Executor {
    if LOCAL_EXECUTOR.is_null() {
        make_executor().expect("failed to create executor");
    }
    &mut *LOCAL_EXECUTOR
}

pub struct Executor {
    futures: [Option<BoxFuture>; MAX_CONCURRENT_TASKS],
    wakers: [Option<Waker>; MAX_CONCURRENT_TASKS],
    free_handles: Vec<TaskHandle>,
    reactor: Reactor,
    pub ready_tasks: Vec<TaskHandle>,
}

impl Executor {
    pub fn new() -> Result<Self, ExecutorError> {
        Ok(Executor {
            futures: [const { None }; MAX_CONCURRENT_TASKS],
            wakers: [const { None }; MAX_CONCURRENT_TASKS],
            free_handles: (0..MAX_CONCURRENT_TASKS).map(TaskHandle::new).collect(),
            reactor: Reactor::new()?,
            ready_tasks: Vec::new(),
        })
    }

    pub fn get_next_task_id(&mut self) -> Result<TaskHandle, ExecutorError> {
        self.free_handles
            .pop()
            .ok_or(ExecutorError::TooManyTasksInProgress)
    }

    pub fn spawn_task(&mut self, future: BoxFuture) -> Result<TaskHandle, ExecutorError> {
        let task_handle = self.get_next_task_id()?;
        let index = task_handle.index();

        let waker = create_waker(task_handle);

        self.futures[index] = Some(future);
        self.wakers[index] = Some(waker);
        self.ready_tasks.push(task_handle);

        Ok(task_handle)
    }

    pub fn run(&mut self) -> Result<(), ExecutorError> {
        loop {
            while let Some(task_handle) = self.ready_tasks.pop() {
                let index = task_handle.index();

                let future = self.futures[index].as_mut().expect("future should exist for ready task");
                let waker = self.wakers[index].as_mut().expect("waker should exist for ready task");
                let mut cx = Context::from_waker(waker);

                match future.as_mut().poll(&mut cx) {
                    Poll::Ready(()) => {
                        self.futures[index] = None;
                        self.wakers[index] = None;
                        self.free_handles.push(task_handle);
                    }
                    Poll::Pending => {}
                }
            }

            self.reactor.submit()?;

            if self.futures.iter().all(|f| f.is_none()) {
                break;
            }

            self.reactor.get_completions()?;
        }

        Ok(())
    }
}
