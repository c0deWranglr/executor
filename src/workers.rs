use super::*;

pub fn task_runner(queue: Arc<Mutex<cc::Receiver<Arc<Task>>>>, max_idle: u64, keep_alive: bool) {
    loop {
        let res = {
            queue.lock().unwrap().recv_timeout(time::Duration::from_millis(max_idle))
        };
        match res {
            Ok(task) => { task(); },
            Err(cc::RecvTimeoutError::Disconnected) => break,
            Err(cc::RecvTimeoutError::Timeout) => if !keep_alive { break }
        }
    }
}

pub fn task_scheduler(inbound: Arc<Mutex<cc::Receiver<ScheduleOp>>>, outbound: cc::Sender<Arc<Task>>) {
    let mut scheduled = Vec::<ScheduleOp>::new();
    loop {
        let res = { inbound.lock().unwrap().try_recv() };

        match res {
            Ok(op) => scheduled.push(op),
            Err(cc::TryRecvError::Disconnected) => break,
            _ => {}
        }

        let mut to_remove = Vec::<usize>::new();
        for (index, op) in scheduled.iter_mut().enumerate() {
            if op.finished.load(Ordering::Acquire) {
                to_remove.push(index);
                continue;
            }

            if Utc::now() >= op.invocation_date {
                outbound.send(op.task.clone());

                if op.repeat_interval < 0 {
                    op.finished.store(true, Ordering::Release);
                }
                else {
                    op.invocation_date = Utc::now() + Duration::milliseconds(op.repeat_interval as i64);
                }
            }
        }

        to_remove.reverse();
        for index in to_remove {
            scheduled.remove(index);
        }
    }
}