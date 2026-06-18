use crate::{
    ack::{Ack, AckMessage},
    activity::{Activity, ActivityId},
    activity_spec::{ActivitySpec, ActivitySpecError},
    clock::{Clock, SystemClock},
    schedule::{Schedule, spawn_scheduler},
};
use std::{fmt::Debug, hash::Hash, sync::Arc, time::Duration};
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
#[derive(Debug)]
pub struct Ritualist<T>
where
    T: ActivityId,
{
    sender: tokio::sync::mpsc::Sender<Ack<T>>,
    receiver: Option<tokio::sync::mpsc::Receiver<Ack<T>>>,
    scheduler: Schedule<T>,
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
            scheduler: scheduler,
            scheduler_handle: scheduler_handle,
            poll_interval,
            cancellation_token: cancellation_token,
        };

        ritualist
    }

    pub async fn register(&self, spec: ActivitySpec<T>) -> Result<(), ActivitySpecError> {
        spec.validate()?;
        self.scheduler.register(spec).await;
        Ok(())
    }

    pub async fn register_many(
        &self,
        specs: Vec<ActivitySpec<T>>,
    ) -> Result<(), Vec<ActivitySpecError>> {
        let errors: Vec<ActivitySpecError> = specs
            .iter()
            .filter_map(|s| {
                if let Err(error) = s.validate() {
                    return Some(error);
                }
                None
            })
            .collect();

        if !errors.is_empty() {
            return Err(errors);
        }

        self.scheduler.register_many(specs).await;

        Ok(())
    }

    pub fn take_channel(&mut self) -> Receiver<(T, Sender<AckMessage>)> {
        self.receiver.take().unwrap()
    }

    pub fn run(self) -> RunningRitualist<T> {
        let poll_interval = self.poll_interval;
        let schedule = self.scheduler.clone();
        let sender = self.sender.clone();

        let mut ticker = tokio::time::interval(poll_interval);
        let cancellation_token = self.cancellation_token.clone();
        let poll_token = self.cancellation_token.child_token();

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
            schedule: self.scheduler.clone(),
            scheduler_handle: self.scheduler_handle,
            polling_handle: handle,
            cancellation_token: cancellation_token,
            poll_token: poll_token.clone(),
        }
    }
}

#[derive(Debug)]
pub struct RunningRitualist<T>
where
    T: ActivityId,
{
    schedule: Schedule<T>,
    polling_handle: JoinHandle<()>,
    scheduler_handle: JoinHandle<()>,
    poll_token: tokio_util::sync::CancellationToken,
    cancellation_token: tokio_util::sync::CancellationToken,
}
impl<T> RunningRitualist<T>
where
    T: Debug + PartialEq + Eq + Hash + Send + Copy + 'static,
{
    pub async fn register(&self, spec: ActivitySpec<T>) -> Result<(), ActivitySpecError> {
        spec.validate()?;
        self.schedule.register(spec).await;
        Ok(())
    }

    pub async fn register_many(
        &mut self,
        specs: Vec<ActivitySpec<T>>,
    ) -> Result<(), Vec<ActivitySpecError>> {
        let errors: Vec<ActivitySpecError> = specs
            .iter()
            .filter_map(|s| {
                if let Err(error) = s.validate() {
                    return Some(error);
                }
                None
            })
            .collect();

        if !errors.is_empty() {
            return Err(errors);
        }

        self.schedule.register_many(specs).await;

        Ok(())
    }

    pub async fn set_enabled(&self, id: T, enabled: bool) {
        self.schedule.set_enabled(id, enabled).await;
    }

    pub fn handle(&self) -> &JoinHandle<()> {
        &self.polling_handle
    }

    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        self.polling_handle.await
    }

    pub async fn snapshot(&self) -> Vec<Activity<T>> {
        self.schedule.snapshot().await
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
