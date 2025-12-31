use crate::executor::TaskHandle;
use std::sync::Arc;
use std::task::Wake;

#[derive(Debug)]
pub struct TaskWaker {
    task_handle: TaskHandle,
}

impl TaskWaker {
    pub fn new(task_handle: TaskHandle) -> Self {
        TaskWaker { task_handle }
    }
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        unsafe {
            let ex = crate::executor::get_local_executor();
            ex.ready_tasks.push(self.task_handle);
        }
    }
}
