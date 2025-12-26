mod executor;
fn executor_loop() {}
fn main() {
    let ring = uring::UringRing::new(1000);
    println!("{:?}", ring)
}
