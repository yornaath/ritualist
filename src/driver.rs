use std::{time::Duration};

use tokio::{select, task::{JoinHandle, JoinSet}};
use tokio_util::sync::CancellationToken;

use crate::{
    ack::{Ack, AckMessage},
    activity::ActivityId,
    schedule::Scheduler,
};

pub struct ScheduleDriver<T: ActivityId> {
    sender: tokio::sync::mpsc::Sender<Ack<T>>,
    receiver: Option<tokio::sync::mpsc::Receiver<Ack<T>>>,
    poll_interval: Duration,
    cancellation_token: CancellationToken,
}

impl<T: ActivityId> ScheduleDriver<T> {

    pub fn new(buffer_size: usize, poll_interval: Duration, cancellation_token: CancellationToken) -> Self {
      let (sender, receiver) = tokio::sync::mpsc::channel(buffer_size);

      ScheduleDriver{
        sender,
        receiver: Some(receiver),
        poll_interval,
        cancellation_token
      }
    }

    pub fn run(mut self, scheduler: Scheduler<T>) -> (tokio::sync::mpsc::Receiver<Ack<T>>, JoinHandle<()>){
        let poll_interval = self.poll_interval;
        let schedule = scheduler.clone();
        let sender = self.sender.clone();

        let mut ticker = tokio::time::interval(poll_interval);
        let cancellation_token = self.cancellation_token.child_token();

        let handle = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                let mut dispatch = JoinSet::<()>::new();

                loop {
                    select! {
                        _ = cancellation_token.cancelled() => break,
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

        (self.receiver.take().unwrap(), handle)
    }
}
