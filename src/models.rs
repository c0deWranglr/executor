use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use crossbeam_channel as cc;
use chrono::*;

/// A Task is just Box<Fn() + Send + Sync + 'static>.
pub type Task = Box<Fn() + Send + Sync + 'static>;

/// A ScheduledTask describes the status of a scheduled operation.
pub struct ScheduledTask {
    finished: Arc<AtomicBool>
}

impl ScheduledTask {
    pub fn new(finished: Arc<AtomicBool>) -> Self {
        ScheduledTask {
            finished
        }
    }

    /// Cancels this task so it will not be ran again in the future.
    pub fn cancel(&self) {
        self.finished.store(true, Ordering::Release)
    }

    /// Returns whether or not this task is finished and will never
    /// be ran again.
    pub fn finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
}

/// A ScheduleResult is just Result<[ScheduledTask], [ScheduleError]>
pub type ScheduleResult = Result<ScheduledTask, ScheduleError>;

/// Describes an error encountered when scheduling a task for execution.
#[derive(Debug)]
pub enum ScheduleError {
    NoWorkers
}

/// Essentially just a named tuple pairing a [Sender<T>] with a
/// [Arc<Mutex<Receiver<T>>>]
pub struct Channel<T> {
    pub sender: cc::Sender<T>,
    pub receiver: Arc<Mutex<cc::Receiver<T>>>
}

impl<T> Channel<T> {
    pub fn new() -> Self {
        let (sender, receiver) = cc::unbounded();
        Channel {
            sender,
            receiver: Arc::new(Mutex::new(receiver))
        }
    }
}

/// Describes an operation that should be scheduled for execution.
pub struct ScheduleOp {
    pub invocation_date: DateTime<Utc>,
    pub repeat_interval: i64,
    pub task: Arc<Task>,
    pub finished: Arc<AtomicBool>
}

impl ScheduleOp {
    /// An operation that should continue to execute after delay_ms at
    /// the specified interval_ms until cancelled.
    pub fn fixed_rate(delay_ms: u32, interval_ms: u32, task: Task) -> Self {
        ScheduleOp {
            invocation_date: Utc::now() + Duration::milliseconds(delay_ms as i64),
            repeat_interval: interval_ms as i64,
            task: Arc::new(task),
            finished: Arc::new(AtomicBool::new(false))
        }
    }

    /// An operation that will execute one time after the given delay_ms.
    pub fn after(delay_ms: u32, task: Task) -> Self {
        ScheduleOp {
            invocation_date: Utc::now() + Duration::milliseconds(delay_ms as i64),
            repeat_interval: -1,
            task: Arc::new(task),
            finished: Arc::new(AtomicBool::new(false))
        }
    }

    /// An operation that will execute on the next available worker thread.
    pub fn now(task: Task) -> Self {
        ScheduleOp {
            invocation_date: Utc::now(),
            repeat_interval: -1,
            task: Arc::new(task),
            finished: Arc::new(AtomicBool::new(false))
        }
    }

    pub fn new(task: Task, at: DateTime<Utc>, interval_ms: i64) -> Self {
        ScheduleOp {
            invocation_date: at,
            repeat_interval: interval_ms,
            task: Arc::new(task),
            finished: Arc::new(AtomicBool::new(false))
        }
    }
}