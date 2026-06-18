use crate::{
    ack::AckMessage,
    activity::{Activity, ActivityId, ActivityState},
    activity_spec::ActivitySpec,
    clock::Clock,
};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use tokio::{
    select,
    sync::{
        mpsc::{self},
        oneshot,
    },
    task::JoinHandle,
};

#[derive(Debug)]
enum Command<T: ActivityId> {
    Register {
        spec: ActivitySpec<T>,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    RegisterMany {
        specs: Vec<ActivitySpec<T>>,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    Tick,
    SetEnabled {
        id: T,
        enabled: bool,
    },
    ClaimDue {
        reply: oneshot::Sender<Vec<T>>,
    },
    Finish {
        id: T,
        ack: AckMessage,
    },
    Snapshot {
        reply: oneshot::Sender<Vec<Activity<T>>>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct Schedule<T>
where
    T: ActivityId,
{
    tx: mpsc::Sender<Command<T>>,
}

impl<T> Schedule<T>
where
    T: ActivityId,
{
    pub async fn register(&self, spec: ActivitySpec<T>) -> Result<(), SchedulerError> {
        let (reply, rx) = oneshot::channel();
        let _ = self.tx.send(Command::Register { spec, reply }).await;
        rx.await.unwrap()
    }

    pub async fn register_many(&self, specs: Vec<ActivitySpec<T>>) -> Result<(), SchedulerError> {
        let (reply, rx) = oneshot::channel();
        let _ = self.tx.send(Command::RegisterMany { specs, reply }).await;
        rx.await.unwrap()
    }

    pub async fn tick(&self) {
        let _ = self.tx.send(Command::Tick).await;
    }

    pub async fn set_enabled(&self, id: T, enabled: bool) {
        let _ = self.tx.send(Command::SetEnabled { id, enabled }).await;
    }

    pub async fn claim_due(&self) -> Vec<T> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(Command::ClaimDue { reply }).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    pub async fn finish(&self, id: T, ack: AckMessage) {
        let _ = self.tx.send(Command::Finish { id, ack }).await;
    }

    pub async fn snapshot(&self) -> Vec<Activity<T>> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(Command::Snapshot { reply }).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }
}

struct ScheduleActor<T>
where
    T: ActivityId,
{
    activities: HashMap<T, Activity<T>>,
    clock: Arc<dyn Clock>,
}

impl<T> ScheduleActor<T>
where
    T: ActivityId,
{
    fn send(&mut self, cmd: Command<T>) {
        match cmd {
            Command::Register { spec, reply } => {
                if self.activities.contains_key(&spec.id) {
                    let _ = reply.send(Err(SchedulerError::ActivityAlreadyRegistered));
                } else {
                    self.activities
                        .insert(spec.id, Activity::new(spec, self.clock.clone()));

                    let _ = reply.send(Ok(()));
                }
            }

            Command::RegisterMany { specs, reply } => {
                for spec in specs {
                    if self.activities.contains_key(&spec.id) {
                        let _ = reply.send(Err(SchedulerError::ActivityAlreadyRegistered));
                        return;
                    }

                    self.activities
                        .insert(spec.id, Activity::new(spec, self.clock.clone()));
                }

                let _ = reply.send(Ok(()));
            }

            Command::Tick => {
                for activity in self.activities.values_mut() {
                    activity.tick();
                }
            }

            Command::SetEnabled { id, enabled } => {
                if let Some(activity) = self.activities.get_mut(&id) {
                    activity.set_enabled(enabled);
                }
            }

            Command::ClaimDue { reply } => {
                let mut ids = Vec::new();
                for activity in self.activities.values_mut() {
                    if let Some(claimed) = activity.claim_if_due() {
                        ids.push(claimed);
                    }
                }
                let _ = reply.send(ids);
            }

            Command::Finish { id, ack } => {
                if let Some(activity) = self.activities.get_mut(&id) {
                    activity.finish(ack);
                    if activity.state == ActivityState::Completed {
                        self.activities.remove(&id);
                    }
                }
            }

            Command::Snapshot { reply } => {
                let _ = reply.send(self.activities.values().cloned().collect());
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SchedulerError {
    #[error("this activity has been registered, maybe you want reset()")]
    ActivityAlreadyRegistered,
}

pub(crate) fn spawn_scheduler<T>(
    buffer: usize,
    cancellation_token: tokio_util::sync::CancellationToken,
    clock: Arc<dyn Clock>,
) -> (Schedule<T>, JoinHandle<()>)
where
    T: ActivityId,
{
    let (tx, mut rx) = mpsc::channel::<Command<T>>(buffer);

    let handle = tokio::spawn(async move {
        let mut actor = ScheduleActor {
            activities: HashMap::new(),
            clock: clock,
        };

        loop {
            select! {
                _ = cancellation_token.cancelled() => break,
                message = rx.recv() => match message {
                    Some(cmd) => actor.send(cmd),
                    None => break
                }
            }
        }
    });

    (Schedule { tx }, handle)
}
