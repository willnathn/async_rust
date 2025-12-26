// so we need to use UringRing in order to actually execute something. We will start with just one
// ring.

mod ring;
mod uring;
use std::{io::Error, task::Waker};

use uring::{Result, UringError, UringRing, io_uring_cqe, io_uring_sqe};

// this is just the size of the ring buffer.
const MAX_CONCURRENT_SQES: u32 = 1024;

// struct of arrays here?
pub struct UringJob {
    result: Option<rustix::io::Result<i32>>,
    waker: Option<Waker>,
}

pub struct Executor {
    uring: uring::UringRing,
    // we just need the wakers here
    head: u32,
    pending: [Option<Waker>; MAX_CONCURRENT_SQES as usize],
    // store the results here
    results: [Option<u32>; MAX_CONCURRENT_SQES as usize],
}

impl Executor {
    pub fn new() -> Result<Self, UringError> {
        Executor {
            head: 0,
            results: [None; MAX_CONCURRENT_SQES as usize],
            pending: [None; MAX_CONCURRENT_SQES as usize],
            uring: UringRing::new(MAX_CONCURRENT_SQES),
        }
    }
}

impl Executor {
    // ensure we mark the io_uring_sqe user data to be the id in the array (which should be a ring
    // buffer
    pub fn enqueue_sqe(&mut self, sqe: io_uring_sqe, waker: Waker) -> Result<u32, UringError> {
        let head = self.head;
        self.head = (head + 1) & (MAX_CONCURRENT_SQES - 1);
        sqe.user_data = head;
        self.pending[head as usize] = Some(waker);
        self.uring.enqueue_sqe(sqe)?;
        Ok(head)
    }
    // here we will want to get the cqes, extract the user data to find the queued task and then
    // handle them by marking ready for next round.
    pub fn get_completions(&mut self) -> Result<(), UringError> {
        let cqes = self.uring.get_cqe_batch();
        for cqe in cqes {
            let index = cqe.user_data;
            // if none we raise an error here
            let waker = self.pending[index];
            self.pending[index] = None;
            self.results[index] = cqe.user_data;
            // tell thread it can poll
            waker.wake();
        }
        Ok(())
    }

    // give the result of the cqe given the index we have.
    // how do I raise an error if I have a None here? Is the error going to be horrible unless we
    // return Option<Result<u32>> can we even get an error?
    pub fn poll_for_completion(&mut self, job_id: u32) -> Result<u32> {
        // unknown error here
        let result = self.results[job_id as usize].ok_or(Error)?;
        self.results[job_id as usize] = None;
        result
    }
}
