//! Executor
//!
//! Executor is a thread pool service that exposes the ability to run a [models::Task]
//! across a pool of threads. Tasks may be submitted for execution on the next
//! available worker thread via [Pool::submit] or scheduled for execution
//! via [Pool::schedule] (see [models::ScheduleOp]).
//!
//! The main struct of Executor is [Pool]. A Pool can be created by calling
//! either [new_single_thread_pool] or [new_thread_pool]:
//! ```
//! extern crate executor;
//! use executor;
//!
//! fn main() {
//!     let mut pool = executor::new_single_thread_pool();
//!     // Or..
//!     let mut pool = executor::new_thread_pool(1, 5, 500);
//! }
//! ```
//! See [Pool]

extern crate crossbeam_channel;
extern crate chrono;
pub mod models;
mod workers;

use models::*;
use std::thread;
use std::sync::{Arc, RwLock, Mutex};
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use crossbeam_channel as cc;
use std::time;
use std::collections::HashMap;
use chrono::*;

/// Creates a new [Pool](struct.Pool.html) with a core_size of 1,
/// and a max_size of 1
pub fn new_single_thread_pool() -> Pool {
    Pool::new(1, 1, 60000)
}

/// Creates a new [Pool](struct.Pool.html) with the specified core_size,
/// max_size and max_idle.
pub fn new_thread_pool(core_size: usize, max_size: usize, max_idle: u64) -> Pool {
    Pool::new(core_size, max_size, max_idle)
}

/// Pool
///
/// A Pool is a managed group of threads used for executing any number of [Task].
/// There are two different types of threads managed by a Pool: core and non core.
/// Core threads are created by calling [start] or [submit] and will live until [stop]
/// has been called on the Pool. Non core threads are only created on a call to [submit]
/// if the number of pending tasks if greater than the number of core threads.
///
/// *Note: [submit] is designed to only spawn a single additional thread per call if and only
/// if the current number of pending tasks is greater than the # of worker threads, core
/// and non core. This means that it is possible for a Pool to function without all of its core
/// threads having been spawned.*
///
/// Additionally, tasks may be scheduled for execution via [schedule]. Scheduled tasks will only
/// ever run on core threads. This means that in order for a scheduled task to be executed, a call
/// must be first made to [start]. A call to [schedule] before [start] will thus fail. This also implies
/// scheduled tasks may not be executed at the exact time they are expected to be, due to possibly
/// having more tasks ready for execution than threads that can execute them.
pub struct Pool {
    core_size: usize,
    max_size: usize,
    max_idle: u64,
    task_channel: Channel<Arc<Task>>,
    schedule_channel: Option<Channel<ScheduleOp>>,
    threads: Vec<thread::JoinHandle<()>>
}

impl Pool {
    fn new(core_size: usize, max_size: usize, max_idle: u64) -> Self {
        Pool {
            core_size,
            max_size,
            max_idle,
            task_channel: Channel::new(),
            schedule_channel: None,
            threads: vec![]
        }
    }

    /// Returns the number of current worker threads, core and non core.
    pub fn worker_count(&self) -> usize {
        Arc::strong_count(&self.task_channel.receiver) - 1
    }

    /// Submits a new [Task] for execution on the next available worker thread.
    /// If there are no free workers and [worker_count] is less than max_size,
    /// then a new one will be spawned.
    pub fn submit(&mut self, task: Task) {
        let len = {
            let sender = &self.task_channel.sender;
            sender.send(Arc::new(task));
            sender.len()
        };

        if len > self.worker_count() {
            self.try_spawn();
        }
    }

    fn try_spawn(&mut self) -> bool {
        let threads = self.worker_count();
        if threads < self.max_size {
            let queue = self.task_channel.receiver.clone();
            let time_out = self.max_idle;
            let core = threads < self.core_size;
            let t = thread::spawn(move || {
                workers::task_runner(queue, time_out, core);
            });
            self.threads.push(t);
            return true
        }
        return false
    }

    /// Starts this pool by spawn all the core threads
    pub fn start(&mut self) {
        while self.worker_count() < self.core_size {
            self.try_spawn();
        }
    }

    /// Stops this pool by disconnecting the channels to each of the worker
    /// threads with the ability to wait until all tasks have been finished
    /// or to stop the workers immediately.
    pub fn stop(self, finish_tasks: bool) {
        let sender = self.task_channel.sender;
        drop(sender);
        if let Some(chn) = self.schedule_channel {
            let sender = chn.sender;
            drop(sender);
        }

        if !finish_tasks {
            let queue = self.task_channel.receiver;
            for _ in queue.lock().unwrap().iter() {}
        }

        for handle in self.threads {
            handle.join().expect("Thread failed to join nicely.");
        }
    }

    /// Schedules a new [ScheduleOp] for execution at some future point
    /// in time. If no worker threads exist, then an error will be returned.
    pub fn schedule(&mut self, op: ScheduleOp) -> ScheduleResult {
        if self.worker_count() == 0 {
            Err(ScheduleError::NoWorkers)
        }
        else {
            match self.schedule_channel {
                Some(ref chn) => {
                    let sender = &chn.sender;
                    let finished = op.finished.clone();
                    sender.send(op);
                    Ok(ScheduledTask::new(finished))
                },
                None => {
                    let chn = Channel::new();
                    let outbound = self.task_channel.sender.clone();
                    let inbound = chn.receiver.clone();
                    let t = thread::spawn(move || {
                        workers::task_scheduler(inbound, outbound);
                    });
                    self.threads.push(t);
                    self.schedule_channel = Some(chn);
                    self.schedule(op)
                }
            }
        }
    }
}

#[cfg(test)]
#[test]
fn pool_start_test() {
    let mut pool = ::new_single_thread_pool();
    assert_eq!(0, pool.worker_count());
    pool.start();
    assert_eq!(1, pool.worker_count());
    pool.stop(true);
}

#[test]
fn pool_submit_before_start_test() {
    let mut pool = ::new_thread_pool(5, 10, 1000);
    assert_eq!(0, pool.worker_count());
    pool.submit(Box::new(|| {
        thread::sleep_ms(100);
    }));
    thread::sleep_ms(2000);
    assert_eq!(1, pool.worker_count());
    pool.start();
    assert_eq!(5, pool.worker_count());
    thread::sleep_ms(2000);
    assert_eq!(5, pool.worker_count());
}

#[test]
fn pool_grow_and_shrink_test() {
    let mut pool = ::new_thread_pool(1, 2, 500);
    pool.start();
    for i in 0..2 {
        pool.submit(Box::new(move || {
            thread::sleep_ms(1000)
        }));
    }
    assert_eq!(2, pool.worker_count());
    thread::sleep_ms(3000);
    assert_eq!(1, pool.worker_count());
    pool.stop(false);
}

#[test]
fn schedule_before_start_test() {
    let mut pool = ::new_single_thread_pool();
    assert_eq!(0, pool.worker_count());
    match pool.schedule(ScheduleOp::now(Box::new(|| {
        thread::sleep_ms(1000);
    }))) {
        Err(ScheduleError::NoWorkers) => { assert!(true) },
        Err(_) => { assert!(false) },
        Ok(_) => { assert!(false) }
    }
}

#[test]
fn scheduled_pool_no_growth_test() {
    let mut pool = ::new_thread_pool(1, 2, 5000);
    pool.start();
    assert_eq!(1, pool.threads.len());
    let mut tasks = vec![];
    for i in 0..2 {
        tasks.push(pool.schedule(ScheduleOp::after(1000, Box::new(|| {
            thread::sleep_ms(1000)
        }))).unwrap());
    }
    assert_eq!(2, pool.threads.len());
    assert_eq!(1, pool.worker_count());
    while tasks.iter().any(|task| !task.finished()) {}
    assert_eq!(1, pool.worker_count());
    //pool.threads.len() is expected to remain at 2 because once the scheduled thread is started, it won't exit until the pool is stopped.
    assert_eq!(2, pool.threads.len());
    pool.stop(false);
}

#[test]
fn repeating_scheduled_task_test() {
    let mut pool = ::new_single_thread_pool();
    pool.start();
    let counter = Arc::new(AtomicUsize::new(0));
    let other = counter.clone();
    let task = pool.schedule(ScheduleOp::fixed_rate(500, 1000, Box::new(move || {
        other.fetch_add(1, Ordering::SeqCst);
    }))).unwrap();
    while counter.load(Ordering::SeqCst) < 3 {}
    task.cancel();
    thread::sleep_ms(3000);
    assert_eq!(true, task.finished());
    assert_eq!(3, counter.load(Ordering::SeqCst));
}