#![feature(thread_local)]
mod executor;
mod futures;
mod reactor;
mod uring;
mod waker;

use std::os::fd::AsRawFd;

use executor::{get_local_executor, make_executor};
use futures::read::ReadFuture;
use rustix::fs::{CWD, Mode, OFlags};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        make_executor()?;
    }

    let ex = unsafe { get_local_executor() };

    ex.spawn_task(Box::pin(async {
        let fd = rustix::fs::openat(CWD, "tmp.txt", OFlags::RDONLY, Mode::from_raw_mode(0o644))
            .expect("open for testing, haven't handled");
        println!(
            "{}",
            String::from_utf8(ReadFuture::new(fd.as_raw_fd(), 0, 1000).await.unwrap()).unwrap()
        )
    }))?;

    ex.run()?;

    println!("Executor finished");
    Ok(())
}
