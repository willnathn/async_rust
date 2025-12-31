#![feature(thread_local)]
mod executor;
mod futures;
mod reactor;
mod uring;
mod waker;

use executor::{get_local_executor, make_executor};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    impl_io_future!(String=>Vec<u8>);
    unsafe {
        make_executor()?;
    }

    let ex = unsafe { get_local_executor() };

    ex.spawn_task(Box::pin(async {
        println!("Hello from async task!");
    }))?;

    ex.run()?;

    println!("Executor finished");
    Ok(())
}
