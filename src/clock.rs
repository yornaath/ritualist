use chrono::{DateTime, Utc};
use std::fmt::Debug;

/// A monotonic point in time, re-exported from [`tokio::time::Instant`].
///
/// Never goes backwards and is unaffected by system-clock changes (NTP,
/// timezone), making it ideal for measuring elapsed time and scheduling.
pub type Instant = tokio::time::Instant;

/// Abstraction over the two notions of time the scheduler needs.
///
/// `Ritualist` reads time only through a `Clock` instead of calling
/// [`tokio::time::Instant::now`] or [`chrono::Utc::now`] directly, so the
/// scheduler can be driven deterministically in tests: production uses
/// [`SystemClock`], tests use [`TestClock`], which advances only when told to.
///
/// It exposes two distinct views:
///
/// * [`now`](Clock::now) — a **monotonic** [`Instant`] ("how long until the
///   next run?"), used to time ticks.
/// * [`now_utc`](Clock::now_utc) — a **wall-clock** [`DateTime<Utc>`] ("what
///   time is it?"), used for timestamps and calendar schedules.
///
/// # Implementing
///
/// Implementors are [`Debug`] + [`Send`] + [`Sync`] (shared across tasks via
/// [`Arc<dyn Clock>`](std::sync::Arc)); both methods should be cheap and
/// non-blocking. The two views may differ in source but must advance together.
///
/// # Examples
///
/// ```
/// use ritualist::clock::{Clock, SystemClock};
///
/// let clock = SystemClock;
/// let start = clock.now();
/// assert!(clock.now() >= start);
/// ```
pub trait Clock: Debug + Send + Sync {
    /// Returns the current monotonic [`Instant`] for measuring elapsed time.
    ///
    /// Never decreases across calls and is unaffected by wall-clock changes.
    fn now(&self) -> Instant;

    /// Returns the current wall-clock time as a [`DateTime<Utc>`].
    ///
    /// For timestamps and calendar schedules. Unlike [`now`](Clock::now) it can
    /// jump if the system clock is adjusted, so don't use it for durations.
    fn now_utc(&self) -> DateTime<Utc>;
}

/// The production [`Clock`] backed by the OS, delegating to
/// [`tokio::time::Instant::now`] and [`chrono::Utc::now`].
///
/// Used by [`Ritualist::new`](crate::Ritualist::new); use [`TestClock`] to
/// control time in tests.
#[derive(Debug, Default, Clone)]
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn now_utc(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[cfg(any(test, feature = "test-util"))]
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

/// A controllable [`Clock`] for deterministic tests.
///
/// Frozen on creation, advancing only on [`advance`](TestClock::advance) — both
/// views move together — so tests can step through schedules without sleeping.
/// Cheap to [`Clone`]: clones share state, so a clone handed to the scheduler
/// observes time driven from the test body.
///
/// Only available under `cfg(test)` or the `test-util` feature.
///
/// # Examples
///
/// ```
/// # use ritualist::clock::{Clock, TestClock};
/// # use chrono::Utc;
/// # use std::time::Duration;
/// let clock = TestClock::new(Utc::now());
/// let start = clock.now();
///
/// clock.advance(Duration::from_secs(60));
///
/// assert_eq!(clock.now() - start, Duration::from_secs(60));
/// ```
#[cfg(any(test, feature = "test-util"))]
#[derive(Debug, Clone)]
pub struct TestClock {
    state: Arc<Mutex<TestState>>,
}

#[cfg(any(test, feature = "test-util"))]
#[derive(Debug)]
struct TestState {
    mono: Instant,
    utc: DateTime<Utc>,
}

#[cfg(any(test, feature = "test-util"))]
impl TestClock {
    /// Creates a clock frozen at `start`.
    ///
    /// Wall-clock begins at `start`, monotonic at the current [`Instant`];
    /// neither advances until [`advance`](TestClock::advance) is called.
    pub fn new(start: DateTime<Utc>) -> Self {
        Self {
            state: Arc::new(Mutex::new(TestState {
                mono: Instant::now(),
                utc: start,
            })),
        }
    }

    /// Moves both views forward by `by`; observed by all clones.
    ///
    /// Panics if `by` is out of range for [`chrono::Duration`].
    pub fn advance(&self, by: Duration) {
        let mut s = self.state.lock().unwrap();
        s.mono += by;
        s.utc += chrono::Duration::from_std(by).expect("duration in range");
    }
}

#[cfg(any(test, feature = "test-util"))]
impl Clock for TestClock {
    fn now(&self) -> Instant {
        self.state.lock().unwrap().mono
    }
    fn now_utc(&self) -> DateTime<Utc> {
        self.state.lock().unwrap().utc
    }
}
