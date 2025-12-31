use crate::executor::TaskHandle;
use crate::uring::{UringError, UringRing, io_uring_sqe};
use std::sync::Arc;
use std::task::{Wake, Waker};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReactorError {
    #[error("Executor already exists in this thread")]
    ExecutorAlreadyExists,
    #[error("No pending slots available to write pending task into")]
    NoPendingTaskSpace,
    #[error("Uring error: {0}")]
    UringError(#[from] UringError),
}
// we don't care about this leaking for now. Currently your reactor is unchangeable anyway. Why can't we borrow mutably? We should probably use some form of scope (by default global), in case for some weird reason you want to run a web server and then stop and do something else.
#[thread_local]
static mut LOCAL_REACTOR: *mut Reactor = std::ptr::null_mut();

// eventually will have some config as well here
pub unsafe fn make_reactor() -> Result<(), ReactorError> {
    if !LOCAL_REACTOR.is_null() {
        return Err(ReactorError::ExecutorAlreadyExists);
    }
    LOCAL_REACTOR = Box::leak(Box::new(Reactor::new()?));
    Ok(())
}

pub fn get_local_reactor() -> &'static mut Reactor {
    if unsafe { LOCAL_REACTOR.is_null() } {
        // get LOCAL_EX to be a pointer to the Executor instance, which lives in TLS
        unsafe { make_reactor() };
    }
    return unsafe { &mut *LOCAL_REACTOR };
}

// this is just the size of the ring buffer for io_uring.
const MAX_CONCURRENT_SQES: u32 = 1024;
// this will need tuning, how many tasks can we be waiting on?
const MAX_PENDING_TASKS: u32 = 2048;

#[derive(Debug, Clone, Copy)]
pub struct ReactorJobHandle {
    index: usize,
}
impl ReactorJobHandle {
    pub fn index(&self) -> usize {
        self.index
    }
    pub fn new(index: usize) -> Self {
        ReactorJobHandle { index }
    }
}

#[derive(Debug)]
pub struct Reactor {
    uring: UringRing,
    results: [Option<i32>; MAX_PENDING_TASKS as usize],
    free_job_ids: Vec<ReactorJobHandle>,
    wakers: [Option<Waker>; MAX_PENDING_TASKS as usize],
}

impl Reactor {
    pub fn new() -> Result<Self, ReactorError> {
        Ok(Reactor {
            results: [None; MAX_PENDING_TASKS as usize],
            free_job_ids: (0..MAX_PENDING_TASKS as usize)
                .map(ReactorJobHandle::new)
                .collect(),
            wakers: [const { None }; MAX_PENDING_TASKS as usize],
            uring: UringRing::new(MAX_CONCURRENT_SQES)?,
        })
    }
}

impl Reactor {
    // ensure we mark the io_uring_sqe user data to be the id in the array (which should be a ring
    // buffer

    // returns a usize index to the results and tasks parallel arrays.
    #[inline]
    pub fn get_new_job_id(&mut self) -> Result<ReactorJobHandle, ReactorError> {
        self.free_job_ids
            .pop()
            .ok_or(ReactorError::NoPendingTaskSpace)
    }
    pub fn enqueue_sqe(
        &mut self,
        mut sqe: io_uring_sqe,
        waker: Waker,
    ) -> Result<ReactorJobHandle, ReactorError> {
        let job_id = self.get_new_job_id()?;
        sqe.user_data.u64_ = job_id.index() as u64;
        // could check pending is None here?
        self.uring.enqueue_sqe(sqe)?;
        self.wakers[job_id.index()] = Some(waker);
        Ok(job_id)
    }
    // here we will want to get the cqes, extract the user data to find the queued task and then
    // handle them by marking ready for next round.
    pub fn get_completions(&mut self) -> Result<(), ReactorError> {
        let cqes = self.uring.get_cqe_batch()?;
        for cqe in cqes {
            let index = unsafe { cqe.user_data.u64_ } as usize;
            // if none we raise an error here
            self.results[index] = Some(cqe.res);
            self.wakers[index]
                .take()
                .expect(&format!("Expected Some got None for waker at {:?}", index))
                .wake();
        }
        Ok(())
    }

    // give the result of the cqe given the index we have.
    // this is just dropping a handle - maybe it should be tied to lifetimes
    // doesn't return a result as we know that result is Some by
    pub fn get_result(&mut self, job_id: ReactorJobHandle) -> i32 {
        let result = self.results[job_id.index()]
            .expect(&format!("Got a none for {:?} but expected Some", job_id));
        self.results[job_id.index()] = None;
        self.wakers[job_id.index()] = None;
        self.free_job_ids.push(job_id);
        result
    }

    pub fn submit(&mut self) -> Result<usize, ReactorError> {
        Ok(self.uring.submit()?)
    }
}
