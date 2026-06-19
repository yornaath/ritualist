use crate::{
    ack::AckMessage,
    activity::ActivityId,
    clock::{Clock, SystemClock},
    driver::ScheduleDriver,
    schedule::{Scheduler, WithScheduler, spawn_scheduler},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc::Receiver, oneshot::Sender};

pub mod ack;
mod activation_target;
pub mod activity;
pub mod activity_spec;
pub mod clock;
mod driver;
pub mod error;
pub mod schedule;

/// Ritualist
///
/// The ritualist is the activity orchestrator where you can register activites to run at certain intervals
/// or at given dates. For now there is no persistence and it is meant to be run in the background of native apps
/// where the app wants to schedule reminders etc at given intervals.
///
/// This is a main module for the [https://www.glimtapp.io] now open sourced.
///
/// Example:
/// ```no_run
/// use ritualist::{
///     Ritualist,
///     ack::AckMessage,
///     activity_spec::{ActivitySchedule, ActivitySpec},
///     schedule::WithScheduler
/// };
/// use std::time::Duration;
/// use tokio::sync;
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// enum Activity {
///     Ping,
///     Pong,
/// }
///
/// impl Activity {
///     fn run(&self, ack: sync::oneshot::Sender<AckMessage>) {
///         let activity = *self;
///         tokio::spawn(async move {
///             match activity {
///                 Activity::Ping => println!("pinged"),
///                 Activity::Pong => println!("ponged"),
///             }
///             // Acknowledge the run so the activity gets re-scheduled.
///             let _ = ack.send(AckMessage::Done);
///         });
///     }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     // buffer = channel capacity, poll_interval = how often the scheduler ticks.
///     let mut ritualist = Ritualist::new(64, Duration::from_millis(100));
///
///     ritualist
///         .register_many(vec![
///             ActivitySpec {
///                 id: Activity::Ping,
///                 schedule: ActivitySchedule::FixedInterval {
///                     duration: Duration::from_secs(1),
///                 },
///             },
///             ActivitySpec {
///                 id: Activity::Pong,
///                 schedule: ActivitySchedule::FixedInterval {
///                     duration: Duration::from_secs(3),
///                 },
///             },
///         ])
///         .await
///         .expect("invalid activity spec");
///
///     // Take the receiving end *before* starting the scheduler.
///     let mut channel = ritualist.run().take_channel();
///
///     // Listen to activities being started
///     while let Some((activity, ack)) = channel.recv().await {
///         activity.run(ack);
///     }
/// }
/// ```
/// A cheap-to-clone handle for the state-independent operations on a ritualist.
///
/// Both [`Ritualist`] and [`RunningRitualist`] deref to this, so the shared
/// operations (register, enable, snapshot) are defined exactly once. The handle
/// is `Clone`, so it can be handed out and used to mutate the schedule
/// concurrently, including while the ritualist is running.
#[derive(Debug)]
pub struct Ritualist<T>
where
    T: ActivityId,
{
    scheduler: Scheduler<T>,
    driver: ScheduleDriver,
}

impl<T> Ritualist<T>
where
    T: ActivityId,
{
    pub fn new(buffer: usize, poll_interval: Duration) -> Ritualist<T> {
        Self::with_clock(buffer, poll_interval, Arc::new(SystemClock))
    }

    pub fn with_clock(
        buffer_size: usize,
        poll_interval: Duration,
        clock: Arc<dyn Clock>,
    ) -> Ritualist<T> {
        let scheduler = spawn_scheduler(buffer_size, clock);
        let driver = ScheduleDriver::new(buffer_size, poll_interval);

        let ritualist: Ritualist<T> = Ritualist { scheduler, driver };

        ritualist
    }

    pub fn run(mut self) -> RunningRitualist<T> {
        let schedule = self.scheduler.clone();
        let receiver = self.driver.run(schedule);

        RunningRitualist {
            receiver: Some(receiver),
            scheduler: self.scheduler,
            driver: self.driver,
        }
    }
}

impl<T: ActivityId> WithScheduler<T> for Ritualist<T> {
    fn get_scheduler(&self) -> Scheduler<T> {
        self.scheduler.clone()
    }
}

#[derive(Debug)]
pub struct RunningRitualist<T>
where
    T: ActivityId,
{
    receiver: Option<Receiver<(T, Sender<AckMessage>)>>,
    scheduler: Scheduler<T>,
    driver: ScheduleDriver,
}
impl<T> RunningRitualist<T>
where
    T: ActivityId,
{
    pub fn take_channel(&mut self) -> Receiver<(T, Sender<AckMessage>)> {
        self.receiver.take().unwrap()
    }

    pub async fn shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.driver.shutdown().await?;
        self.scheduler.shutdown().await?;
        Ok(())
    }

    pub async fn abort(self) {
        self.driver.abort();
        self.scheduler.abort();
    }
}

impl<T: ActivityId> WithScheduler<T> for RunningRitualist<T> {
    fn get_scheduler(&self) -> Scheduler<T> {
        self.scheduler.clone()
    }
}
