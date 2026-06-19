use std::time::Duration;

use tokio::{
    select,
    task::{JoinError, JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

use crate::{
    ack::{Ack, AckMessage},
    activity::ActivityId,
    schedule::Scheduler,
};

#[derive(Debug)]
pub struct ScheduleDriver {
    buffer_size: usize,
    poll_interval: Duration,
    cancellation_token: CancellationToken,
    handle: Option<JoinHandle<()>>,
}

impl ScheduleDriver {
    pub fn new(buffer_size: usize, poll_interval: Duration) -> Self {
        ScheduleDriver {
            buffer_size,
            poll_interval,
            cancellation_token: CancellationToken::new(),
            handle: None,
        }
    }

    pub async fn shutdown(self) -> Result<(), JoinError> {
        self.cancellation_token.cancel();
        if let Some(join_handle) = self.handle {
            join_handle.await
        } else {
            Ok(())
        }
    }

    pub fn abort(self) {
        if let Some(join_handle) = self.handle {
            join_handle.abort();
        }
    }

    pub fn run<T: ActivityId>(
        &mut self,
        scheduler: Scheduler<T>,
    ) -> tokio::sync::mpsc::Receiver<Ack<T>> {
        let (sender, receiver) = tokio::sync::mpsc::channel(self.buffer_size);
        let poll_interval = self.poll_interval;
        let schedule = scheduler.clone();

        let mut ticker = tokio::time::interval(poll_interval);
        let cancellation_token = self.cancellation_token.clone();

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

        self.handle = Some(handle);

        receiver
    }
}
