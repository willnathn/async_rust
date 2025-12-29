#![feature(thread_local)]
mod executor;
mod futures;
mod reactor;
mod uring;
use executor::Executor;
use futures::ReadFuture;
use reactor::Reactor;
fn main() {
    let ring = Reactor::new();
    println!("{:?}", ring)
}
