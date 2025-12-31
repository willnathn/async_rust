# Async terminology
My definitions:
- Concurrency: Perform operations from multiple contexts at the same time.
- Parallelism: Execute multiple assembly instructions at the same time.
- Task (in C#) / Promise (js) / Future (rust) : A placeholder that will be populated with a value at some non-deterministic future time.
- task (rust, general async programming): a linear piece of encapsulated logic.
- Executor: Schedule tasks, track progress.
- Reactor: Implement non-blocking IO, so that we do not waste cpu cycles in the userspace either waiting for kernel space work or work on a different device.

# Magic and Concurrency
## The Magic Boundary
The Magic Bounday is implementation below a `just trust it` abstraction point where to develop software it is considered unnecessary, a waste of time, to understand / look at what is going on below this point, because it is so unlikely to be the source of a bug compared to the application code, because we trust the correctness below the boundary. Magic has been an enemy of my learning. It is the friend of project managers. Depending on your role and language choice, obviously, it has a different boundary.

If there is changes in the language the software is written in it is automatically magic. 

Assembly, or anything the compiler generates, is magic. 
The kernel is magic.
Normally the standard library is magic.

In Python, memory layout is magic, as are most of the packages and all performant ones.
In React, javascript is magic.

## Magic in Concurrency
Tokio is magic. Glommio is also magic, but less so. I like rust mostly because it has zero-cost abstractions. These make it easier to write code but contribute little magic. 

# Async rust fill in the blanks
Rust provides Futures, Wakers and await syntax.
Any container that implements the Future trait has a poll method that returns either pending or a result. It should be idempotent.

For this async function:
```rust
async fn parent() {
    do_a().await;
    do_b().await;
}
```
The compiler then makes something like (but different) to below. This is clearly wrong as Futures are different
sizes so can't go in a vec. Also they would handle results properly:
```rust
struct ParentFuture {
    state: usize,
    futures: Vec<Future>,
}

impl Future for ParentFuture {
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // note here the waker is cx.waker, so we pass the waker down, always waking the root waker!
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

The cx here contains a waker. A waker implements the Wake trait, which takes in an Arc to a Waker (this is annoying). The async execution implements how to mark a task as ready to poll. This is (I think) what rust intends:
```rust
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
```
So wakers allow composition of futures to the ultimate root / parent future which is the future of a task. 

# Design
## Why concurrency
In my understanding, we use concurrent (but not parallel) execution at the application level due to (not exclusively) the problem of shared ( mutable ) state. When we have any shared mutable state, reasoning about code where yielding can occur at any point in execution, or execution occurs in parallel is really hard. Cache coherence requirements mean when we change data in private caches of one processor all other caches need to update their data (cache line invalidation) from either the cache or memory. This can kill performance.

There are other reasons:
- The overhead of CPU threads is much higher than concurrent tasks, and the scope of some tasks (especially in a web server) are so short that thread spawning is comparible to executing the task.
- We waste cpu cycles spinning when waiting for IO.


## Good Design
Having read some DoD stuff and listened to some talks, podcasts, I have some opinions about design in Rust, but little comparitive practical experience. I like using arrays over vec. It shows the constraints of the implementation up front. 

## Send Sync Pin 'static 
Send: can ownership be transferred between threads.
Sync: can references be transferred between threads.
Pin: Won't move in memory.
static: lives for the lifetime of the program.

Rust gets a bad rep for async code because of the quantity of traits required to make code work. When a lifetime is unknown at runtime, the static lifetime is often required. Some of this complexity is justified, lifetimes are hard for concurrent code, but that doesn't mean memory safety suddenly doesn't matter. For multi-threaded code, mutable state is hard, so it is hardly surprising Rust can't fix this issue for us. BUT for concurrent and not parallel code it shouldn't need to be this way.

Tokio uses work stealing. This means all the data in every task must be able to be sent between threads. Apparently Tokio doesn't actually think work stealing is that effective and it certainly costs a lot.

# Future plans
- write the networking futures!
- Use scoped tasks so we need none of Send, Sync, 'static.
- Work with Rust lifetimes instead of throwing around unsafe blocks.
- Do some better scheduling.
- Write a benchmark.


