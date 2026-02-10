#![expect(clippy::redundant_async_block)]

use crate::{Builder, JoinHandle, Result, Runnable};

use std::{
    collections::VecDeque,
    pin::pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

fn poll_loop<R>(future: impl Future<Output = R>) -> R {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
    }
}

struct Runtime {
    queue: Arc<Mutex<VecDeque<Runnable>>>,
}

impl Runtime {
    fn new() -> Runtime {
        Runtime {
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn spawn<R: Send + 'static>(
        &self,
        task: impl Future<Output = R> + Send + 'static,
    ) -> JoinHandle<R> {
        let queue = Arc::clone(&self.queue);
        let (runnable, handle) = Builder::new()
            .catch_unwind(true)
            .spawn(task, move |r| queue.lock().unwrap().push_back(r));
        runnable.schedule();
        handle
    }

    fn spawn_local<R: 'static>(&self, task: impl Future<Output = R> + 'static) -> JoinHandle<R> {
        let queue = Arc::clone(&self.queue);
        let (runnable, handle) = Builder::new()
            .catch_unwind(true)
            .spawn_local(task, move |r| queue.lock().unwrap().push_back(r));
        runnable.schedule();
        handle
    }

    fn run<R: Send + 'static>(
        &self,
        future: impl Future<Output = R> + Send + 'static,
    ) -> Result<R> {
        let handle = self.spawn(future);
        while !handle.finished() {
            self.queue.lock().unwrap().pop_front().unwrap().run();
        }
        poll_loop(handle)
    }
}

#[test]
fn run_block() -> Result<()> {
    let rt = Runtime::new();
    let res = rt.run(async { "hewwo" })?;
    assert_eq!(res, "hewwo");
    Ok(())
}

#[test]
fn await_task() -> Result<()> {
    let rt = Runtime::new();
    let handle = rt.spawn(async { "hewwo" });
    let res = rt.run(async move { handle.await })??;
    assert_eq!(res, "hewwo");
    Ok(())
}

#[test]
fn local_future() -> Result<()> {
    let rt = Runtime::new();
    let handle = rt.spawn_local(async { "hewwo" });
    let res = rt.run(async move { handle.await })??;
    assert_eq!(res, "hewwo");
    Ok(())
}
