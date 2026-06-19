use crate::{
    ack::{Ack, AckMessage}, activity::{Activity, ActivityId}, activity_spec::{ActivitySpec, ActivitySpecError}, clock::{Clock, SystemClock}, driver::ScheduleDriver, schedule::{Scheduler, SchedulerError, spawn_scheduler}
};
use std::{ops::Deref, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::{mpsc::Receiver, oneshot::Sender},
    task::{JoinHandle, JoinSet},
};

pub mod ack;
mod activation_target;
pub mod activity;
pub mod activity_spec;
pub mod clock;
pub mod schedule;
mod driver;

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
///     let mut channel = ritualist.take_channel();
///
///     // Start the clock
///     ritualist.run();
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
    sender: tokio::sync::mpsc::Sender<Ack<T>>,
    receiver: Option<tokio::sync::mpsc::Receiver<Ack<T>>>,
    scheduler: SchedulerHandle<T>,
    scheduler_handle: JoinHandle<()>,
    poll_interval: Duration,
    cancellation_token: tokio_util::sync::CancellationToken,
}

impl<T> Ritualist<T>
where
    T: ActivityId,
{
    pub fn new(buffer: usize, poll_interval: Duration) -> Ritualist<T> {
        Self::with_clock(buffer, poll_interval, Arc::new(SystemClock))
    }

    pub fn with_clock(
        buffer: usize,
        poll_interval: Duration,
        clock: Arc<dyn Clock>,
    ) -> Ritualist<T> {
        let (sender, receiver) = tokio::sync::mpsc::channel(buffer);

        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let (scheduler, scheduler_handle) =
            spawn_scheduler(buffer, cancellation_token.clone(), clock);

        let ritualist: Ritualist<T> = Ritualist {
            sender,
            receiver: Some(receiver),
            scheduler: SchedulerHandle { scheduler },
            scheduler_handle: scheduler_handle,
            poll_interval,
            cancellation_token: cancellation_token,
        };

        ritualist
    }

    pub fn take_channel(&mut self) -> Receiver<(T, Sender<AckMessage>)> {
        self.receiver.take().unwrap()
    }

    pub fn run(self) -> RunningRitualist<T> {
        let poll_interval = self.poll_interval;
        let schedule = self.scheduler.scheduler.clone();
        let sender = self.sender.clone();

        let mut ticker = tokio::time::interval(poll_interval);
        let cancellation_token = self.cancellation_token.clone();
        let poll_token = self.cancellation_token.child_token();

        let a = ScheduleDriver::new(buffer_size, poll_interval, cancellation_token)

        let handle = tokio::spawn({
            let poll_token = poll_token.clone();
            async move {
                let mut dispatch = JoinSet::<()>::new();

                loop {
                    select! {
                        _ = poll_token.cancelled() => break,
                        _ = ticker.tick() => {
                            schedule.tick().await;

                            let due = schedule.claim_due().await;

                            for id in due {
                                let sender = sender.clone();
                                let schedule = schedule.clone();

                                dispatch.spawn(async move {
                                    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();

                                    let _ = sender.send((id, ack_tx)).await;
                                    let outcome = ack_rx.await.unwrap_or(AckMessage::Done);

                                    schedule.finish(id, outcome).await;
                                });
                            }
                        },
                        Some(_) = dispatch.join_next() => {}
                    }
                }

                let _ = tokio::time::timeout(Duration::from_secs(5), async {
                    while dispatch.join_next().await.is_some() {}
                })
                .await;

                dispatch.abort_all();

                while dispatch.join_next().await.is_some() {}
            }
        });

        RunningRitualist {
            scheduler: self.scheduler,
            scheduler_handle: self.scheduler_handle,
            polling_handle: handle,
            cancellation_token: cancellation_token,
            poll_token: poll_token.clone(),
        }
    }
}

impl<T> Deref for Ritualist<T>
where
    T: ActivityId,
{
    type Target = SchedulerHandle<T>;

    fn deref(&self) -> &Self::Target {
        &self.scheduler
    }
}

#[derive(Debug)]
pub struct RunningRitualist<T>
where
    T: ActivityId,
{
    scheduler: SchedulerHandle<T>,
    polling_handle: JoinHandle<()>,
    scheduler_handle: JoinHandle<()>,
    poll_token: tokio_util::sync::CancellationToken,
    cancellation_token: tokio_util::sync::CancellationToken,
}
impl<T> RunningRitualist<T>
where
    T: ActivityId,
{
    pub fn handle(&self) -> &JoinHandle<()> {
        &self.polling_handle
    }

    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        self.polling_handle.await
    }

    pub async fn shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.poll_token.cancel();
        self.polling_handle.await?;
        self.cancellation_token.cancel();
        self.scheduler_handle.await
    }

    pub async fn abort(self) {
        self.polling_handle.abort();
        self.scheduler_handle.abort();
    }
}

impl<T> Deref for RunningRitualist<T>
where
    T: ActivityId,
{
    type Target = SchedulerHandle<T>;

    fn deref(&self) -> &Self::Target {
        &self.scheduler
    }
}

/// Scheduler handle trait
///
/// Shared logic for scheduling methods like regsiter, set_enabled and snapshot
/// that is shared between the [`Ritualist`] and the [`RunningRitualist`]
#[derive(Debug, Clone)]
pub struct SchedulerHandle<T>
where
    T: ActivityId,
{
    scheduler: Scheduler<T>,
}

impl<T> SchedulerHandle<T>
where
    T: ActivityId,
{
    /// Schedules a new activity to run at the given interval.
    ///
    /// Hash of T: [`activity::ActivityId`] is treated as the activities unique identifier
    /// and will be used when storing the activity in the scheduler.
    ///
    /// Will return an error if the activity is already registered.
    pub async fn register(&self, spec: ActivitySpec<T>) -> Result<(), RitualistError> {
        spec.validate().map_err(RitualistError::ActivitySpecError)?;

        self.scheduler
            .register(spec)
            .await
            .map_err(RitualistError::ScheulerError)?;

        Ok(())
    }

    /// Same as register but for a batch.
    /// Ref [`SchedulerHandle::register`]
    pub async fn register_many(&self, specs: Vec<ActivitySpec<T>>) -> Result<(), RitualistError> {
        for spec in &specs {
            spec.validate().map_err(RitualistError::ActivitySpecError)?;
        }

        self.scheduler
            .register_many(specs)
            .await
            .map_err(RitualistError::ScheulerError)?;

        Ok(())
    }

    /// Reset a activitiy.
    ///
    /// Same as [`SchedulerHandle::resger`] but will overwrite and reschedule
    /// the given activitiy.
    pub async fn reset(&self, spec: ActivitySpec<T>) -> Result<(), RitualistError> {
        spec.validate().map_err(RitualistError::ActivitySpecError)?;
        self.scheduler.reset(spec).await;
        Ok(())
    }

    /// Reset a set of activities.
    ///
    /// Same as [`SchedulerHandle::register_many`] but will overwrite and reschedule
    /// the given activities.
    pub async fn reset_many(&self, specs: Vec<ActivitySpec<T>>) -> Result<(), RitualistError> {
        for spec in &specs {
            spec.validate().map_err(RitualistError::ActivitySpecError)?;
        }
        self.scheduler.reset_many(specs).await;
        Ok(())
    }

    /// Enable or disable a given task.
    ///
    /// - For interval scheduled tasks this will pause the timer.
    /// - For date scheduled tasks this will omit emitting the activity event
    ///   if the activity is disabled when it should have fired.
    pub async fn set_enabled(&self, id: T, enabled: bool) {
        self.scheduler.set_enabled(id, enabled).await;
    }

    /// Get a snapshot of the activities and their states currently in the scheduler
    /// The activities are cloned and mutating them wont affect the scheduler state.
    pub async fn snapshot(&self) -> Vec<Activity<T>> {
        self.scheduler.snapshot().await
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RitualistError {
    #[error("An error with the validation of the ActivitySpec.")]
    ActivitySpecError(ActivitySpecError),
    #[error("An error happend in the scheulder.")]
    ScheulerError(SchedulerError),
}
