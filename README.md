### Executor ###

Executor is a thread pool service that exposes the ability to run a [models::Task]
across a pool of threads. Tasks may be submitted for execution on the next
available worker thread via [Pool::submit] or scheduled for execution
via [Pool::schedule] (see [models::ScheduleOp]).

The main struct of Executor is [Pool]. A Pool can be created by calling
either [new_single_thread_pool] or [new_thread_pool]:
```
extern crate executor;
use executor;

fn main() {
    let mut pool = executor::new_single_thread_pool();
    // Or..
    let mut pool = executor::new_thread_pool(1, 5, 500);
}
```
