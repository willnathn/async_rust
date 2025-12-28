#![feature(thread_local)]
mod reactor;
mod read_future;
mod uring;
use reactor::Reactor;
fn main() {
    let ring = Reactor::new();
    println!("{:?}", ring)
}
