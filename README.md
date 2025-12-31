rust provides Futures, Wakers and await syntax.  A Future has a poll that returns pending or
result.

For this async function, rust does:
```rust
async fn parent() {
    do_a().await;
    do_b().await;
}
```
The compiler then makes something like (but different) to below. This is clearly wrong as Futures are different
sizes so can't go in a vec. Also they would handle results properly, so the struct would be an
enum.:
```rust
struct ParentFuture {
    state: usize,
    futures: Vec<Future>,
}

impl Future for ParentFuture {
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // note here the waker is cx.waker, so we pass the waker down!
        match self.futures[state].poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(res) => {
                if self.state >= futures.len() - 1 {
                    return Poll::Ready(res);
                }
                self.state = state + 1;
                return Poll::Pending;
            }
        }
    }
}
```rust

the cx here contains a waker. Our reactor can call waker.wake(), which will add a task to a ready queue. The executor then will call the poll method of the root future for all the tasks which are ready.

The bit of the puzzle that confused me the most is how the ChildFuture has the Parents waker
we will have a spawn_task and waker that does something like:

struct MyWaker{
 task_id:usize
 }
 impl MyWaker {
     pub fn new(task_id:usize) {
         let ex = get_executor();
 }
 impl Wake for MyWaker {
     fn wake(self:Arc<Self>) {
         let ex = get_executor();
         ex.add_to_ready_queue(self.task_id);
     }
 }

 fn spawn_task<T>(future: impl Future<Output=T> + 'static) {
     let ex = get_executor;
     let task_id = ex.get_next_task_id();
     waker = MyWaker {task_id};
     let task = Task {
         id: task_id,
         waker,
         ...
     }
     ex.add_task(task);
 }

 this means all we need to do is keep on passing the waker around.

The waker is our interface with the compiler generated root futures. It's slightly annoying as it uses Arc which we don't need.


