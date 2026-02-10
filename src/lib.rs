#![doc = include_str!("../README.md")]
#![cfg_attr(doc, feature(doc_cfg))]
#![no_std]

mod state;

mod task;
pub use task::Runnable;
use task::{RawHandle, RawJoinHandle, Task};

#[cfg(test)]
mod test;

use core::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

extern crate alloc;

use alloc::boxed::Box;

#[cfg(any(feature = "std", test))]
extern crate std;

#[cfg(feature = "std")]
use std::thread::{self, ThreadId};

use pin_project::pin_project;

use thiserror::Error;

/// A builder for a task.
pub struct Builder<M> {
    metadata: M,
    catch_unwind: bool,
}

impl Builder<()> {
    /// Construct a builder with default settings.
    #[inline]
    #[must_use]
    pub const fn new() -> Builder<()> {
        Builder {
            metadata: (),
            catch_unwind: false,
        }
    }
}

impl<M> Builder<M> {
    /// Add metadata to the builder.
    ///
    /// The default is `()`.
    #[inline]
    #[must_use]
    pub fn metadata(self, metadata: M) -> Builder<M> {
        Builder { metadata, ..self }
    }

    /// Whether panics that occur during polling should be caught.
    #[inline]
    #[must_use]
    #[cfg(feature = "std")]
    pub fn catch_unwind(self, catch_unwind: bool) -> Builder<M> {
        Builder {
            catch_unwind,
            ..self
        }
    }

    /// Spawn a task to run on an executor.
    #[inline]
    pub fn spawn<F, T, S>(self, future: F, scheduler: S) -> (Runnable<M>, JoinHandle<T, M>)
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        S: Schedule<M> + Sync + 'static,
    {
        // SAFETY: This is a valid spawn.
        unsafe { self.spawn_unchecked(future, scheduler) }
    }

    /// Spawn a task to run on the same thread it was spawned.
    #[cfg(feature = "std")]
    #[inline]
    pub fn spawn_local<F, T, S>(self, future: F, scheduler: S) -> (Runnable<M>, JoinHandle<T, M>)
    where
        F: Future<Output = T> + 'static,
        T: 'static,
        S: Schedule<M> + 'static,
    {
        #[pin_project]
        struct ThreadLocal<F> {
            #[pin]
            future: F,
            thread: ThreadId,
        }

        impl<F, R> Future for ThreadLocal<F>
        where
            F: Future<Output = R>,
        {
            type Output = R;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                assert_eq!(
                    self.thread,
                    thread::current().id(),
                    "a local future can only be run on the thread on which it was spawned"
                );

                self.project().future.poll(cx)
            }
        }

        let future = ThreadLocal {
            future,
            thread: thread::current().id(),
        };

        // SAFETY:
        unsafe { self.spawn_unchecked(future, scheduler) }
    }

    #[cfg(feature = "std")]
    fn wrap_catch_unwind<F: Future<Output = R>, R>(future: F) -> impl Future<Output = Result<R>> {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        #[pin_project]
        struct CatchUnwind<F>(#[pin] F);

        impl<F, R> Future for CatchUnwind<F>
        where
            F: Future<Output = R>,
        {
            type Output = Result<R>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let res = catch_unwind(AssertUnwindSafe(|| self.project().0.poll(cx)));
                match res {
                    Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
                    Ok(Poll::Pending) => Poll::Pending,
                    Err(err) => Poll::Ready(Err(Error::panicked(err))),
                }
            }
        }

        CatchUnwind(future)
    }

    #[cfg(not(feature = "std"))]
    fn wrap_catch_unwind<F: Future<Output = R>, R>(future: F) -> impl Future<Output = Result<R>> {
        Self::wrap_panicking(future)
    }

    fn wrap_panicking<F: Future<Output = R>, R>(future: F) -> impl Future<Output = Result<R>> {
        #[pin_project]
        struct Wrap<F>(#[pin] F);

        impl<F, R> Future for Wrap<F>
        where
            F: Future<Output = R>,
        {
            type Output = Result<R>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.project().0.poll(cx).map(Ok)
            }
        }

        Wrap(future)
    }

    /// Spawn a task to run on an executor.
    ///
    /// # Safety
    ///
    /// The returned handler must not outlive `future`, `scheduler`, or `metadata`.
    ///
    /// If the returned handler is sent between threads, `F`, `T`, `S`, and `M` must implement `Send`, and `S` and `M` must implement `Sync`.
    ///
    /// If the returned handler is shared between threads, `S` and `M` must implement `Sync`.
    #[inline]
    pub unsafe fn spawn_unchecked<F, T, S>(
        self,
        future: F,
        scheduler: S,
    ) -> (Runnable<M>, JoinHandle<T, M>)
    where
        F: Future<Output = T>,
        S: Schedule<M>,
    {
        let (runnable, handle) = if self.catch_unwind {
            Task::allocate(Self::wrap_catch_unwind(future), scheduler, self.metadata)
        } else {
            Task::allocate(Self::wrap_panicking(future), scheduler, self.metadata)
        };
        (runnable, JoinHandle { raw: handle })
    }
}

/// Spawn a task to run on an executor.
#[inline]
pub fn spawn<F, T, S>(future: F, scheduler: S) -> (Runnable, JoinHandle<T>)
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
    S: Schedule + Sync + 'static,
{
    Builder::new().spawn(future, scheduler)
}

/// Spawn a task to run on the same thread it was spawned.
#[cfg(feature = "std")]
#[inline]
pub fn spawn_local<F, T, S>(future: F, scheduler: S) -> (Runnable, JoinHandle<T>)
where
    F: Future<Output = T> + 'static,
    T: 'static,
    S: Schedule + 'static,
{
    Builder::new().spawn_local(future, scheduler)
}

/// Spawn a task to run on an executor.
///
/// # Safety
///
/// The returned handler must not outlive `future`, `scheduler`, or `metadata`.
///
/// If the returned handler is sent between threads, `F`, `T`, and `S` must implement `Send`, and `S` and `M` must implement `Sync`.
///
/// If the returned handler is shared between threads, `S` must implement `Sync`.
#[inline]
pub unsafe fn spawn_unchecked<F, T, S>(future: F, scheduler: S) -> (Runnable, JoinHandle<T>)
where
    F: Future<Output = T>,
    S: Schedule,
{
    // SAFETY: Must be ensured by caller.
    unsafe { Builder::new().spawn_unchecked(future, scheduler) }
}

impl Default for Builder<()> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// A handle to a running task.
pub struct JoinHandle<T, M = ()> {
    raw: RawJoinHandle<T, M>,
}

impl<T, M> JoinHandle<T, M> {
    /// The metadata associated with this task.
    #[inline]
    pub fn metadata(&self) -> &M {
        self.raw.metadata()
    }

    /// Cancel this task.
    #[inline]
    pub fn cancel(&self) {
        self.raw.cancel();
    }

    /// Create a handle that can be used to cancel this task.
    #[inline]
    pub fn abort_handle(&self) -> AbortHandle {
        AbortHandle {
            raw: self.raw.handle().clone(),
        }
    }

    /// Whether the future is finished.
    #[inline]
    pub fn finished(&self) -> bool {
        self.raw.finished()
    }
}

impl<T, M> Future for JoinHandle<T, M> {
    type Output = Result<T>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.raw.poll(cx)
    }
}

impl<T, M: fmt::Debug> fmt::Debug for JoinHandle<T, M> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle")
            .field("metadata", &self.metadata())
            .finish_non_exhaustive()
    }
}

// SAFETY: The value can be safely sent, and the metadata can be read.
unsafe impl<T: Send, M: Sync> Send for JoinHandle<T, M> {}

// SAFETY: `&JoinHandle` only provides access to the metadata.
unsafe impl<T, M: Sync> Sync for JoinHandle<T, M> {}

/// A handle to cancel a task.
#[derive(Clone)]
pub struct AbortHandle {
    raw: RawHandle,
}

impl AbortHandle {
    /// Cancel the associated task.
    #[inline]
    pub fn cancel(&self) {
        self.raw.cancel();
    }
}

impl fmt::Debug for AbortHandle {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AbortHandle").finish_non_exhaustive()
    }
}

// SAFETY: `AbortHandle` only accesses synchronised data.
unsafe impl Send for AbortHandle {}

// SAFETY: `AbortHandle` only accesses synchronised data.
unsafe impl Sync for AbortHandle {}

/// Internally-generated information passed to the scheduler.
#[derive(Debug, Clone, Copy)]
pub struct ScheduleInfo {
    /// Indicates whether the task was woken while it was running.
    ///
    /// This usually implies the task yielded.
    pub woken_while_running: bool,
}

/// A task scheduler.
///
/// The typical example is a `impl Fn(Runnable<M>)`.
pub trait Schedule<M = ()> {
    /// Schedule the task.
    fn schedule(&self, runnable: Runnable<M>, info: ScheduleInfo);
}

impl<F, M> Schedule<M> for F
where
    F: Fn(Runnable<M>),
{
    #[inline]
    fn schedule(&self, runnable: Runnable<M>, _: ScheduleInfo) {
        self(runnable);
    }
}

/// A scheduler that receives extra information.
#[derive(Debug)]
pub struct WithInfo<F>(pub F);

impl<F, M> Schedule<M> for WithInfo<F>
where
    F: Fn(Runnable<M>, ScheduleInfo),
{
    #[inline]
    fn schedule(&self, runnable: Runnable<M>, info: ScheduleInfo) {
        self.0(runnable, info);
    }
}

/// An error returned from a failed task.
#[derive(Debug, Error)]
pub enum Error {
    /// The task was cancelled before it could finish.
    #[error("the task was cancelled")]
    Cancelled,
    /// The task panicked.
    #[error("the task panicked")]
    Panicked {
        /// The panic payload.
        payload: Box<dyn core::any::Any + Send>,
    },
}

impl Error {
    const fn panicked(payload: Box<dyn core::any::Any + Send>) -> Error {
        Error::Panicked { payload }
    }
}

/// A result returned from a task.
pub type Result<T, E = Error> = core::result::Result<T, E>;
