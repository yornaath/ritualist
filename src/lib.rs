pub mod ack;
pub mod activity;
pub mod activity_spec;
pub mod schedule;
mod activation_target;
pub mod clock;

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

  pub fn with_clock(buffer: usize, poll_interval: Duration, clock: Arc<dyn Clock>) -> Ritualist<T> {
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

