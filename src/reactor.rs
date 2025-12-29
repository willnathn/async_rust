// io_uring futures register their job here, call enqueue_sqe to get their job it and then can poll the job
// id. Executors can use the ready_jobs ids to find which tasks need to wake and call submit
// whenever ready.

use crate::uring::{io_uring_sqe, UringError, UringRing};
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
// we don't care about this leaking for now. Currently your executor is unchangeable anyway. Why can't we borrow mutably?
// change the state for anything to work.
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

//TODO: we don't have to store wakers here, and it may be more efficient to just read the
//reactor.ready_jobs and call wake() in a loop in the Executor, but at some point we need to call
//waker.wake().
#[derive(Debug)]
pub struct Reactor {
    uring: UringRing,
    results: [Option<i32>; MAX_PENDING_TASKS as usize],
    free_job_ids: Vec<ReactorJobHandle>,
    ready_jobs: Vec<ReactorJobHandle>,
}

impl Reactor {
    pub fn new() -> Result<Self, UringError> {
        Ok(Reactor {
            results: [None; MAX_PENDING_TASKS as usize],
            free_job_ids: (0..MAX_PENDING_TASKS as usize)
                .map(ReactorJobHandle::new)
                .collect(),
            uring: UringRing::new(MAX_CONCURRENT_SQES)?,
            ready_jobs: Vec::with_capacity(MAX_PENDING_TASKS as usize),
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
    pub fn enqueue_sqe(&mut self, mut sqe: io_uring_sqe) -> Result<ReactorJobHandle, ReactorError> {
        // how do we register the future in tasks here?
        let job_id = self.get_new_job_id()?;
        sqe.user_data.u64_ = job_id.index() as u64;
        // could check pending is None here?
        self.uring.enqueue_sqe(sqe)?;
        Ok(job_id)
    }
    // here we will want to get the cqes, extract the user data to find the queued task and then
    // handle them by marking ready for next round.
    pub fn get_completions(&mut self) -> Result<(), UringError> {
        let cqes = self.uring.get_cqe_batch()?;
        for cqe in cqes {
            let index = unsafe { cqe.user_data.u64_ } as usize;
            // if none we raise an error here
            self.results[index] = Some(cqe.res);
            // tell thread it can poll - instead of the overhead of a vtable for waker.wake lets just push to the
            // ready queue and whatever is running the loop can just poll these.
            self.ready_jobs.push(ReactorJobHandle { index });
        }
        Ok(())
    }

    // give the result of the cqe given the index we have.
    // how do I raise an error if I have a None here? Is the error going to be horrible unless we
    // return Option<Result<u32>> can we even get an error?
    pub fn poll_for_completion(&mut self, job_id: ReactorJobHandle) -> Option<i32> {
        // unknown error here
        let result = self.results[job_id.index()];
        self.results[job_id.index()] = None;
        self.free_job_ids.push(job_id);
        result
    }
}
