#![feature(thread_local)]
mod executor;
mod read_future;
mod uring;
use executor::Executor;
fn main() {
    let ring = Executor::new();
    println!("{:?}", ring)
}
