use ritualist::{
    Ritualist,
    ack::AckMessage,
    activity_spec::{ActivitySchedule, ActivitySpec},
};
use std::{sync::Arc, thread::Result, time::Duration};
use tokio::sync::{self, Mutex};

mod common;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum Activity {
    Ping,
    Pong,
}

impl Activity {
    pub fn run(&self, ack: sync::oneshot::Sender<AckMessage>) {
        tokio::spawn({
            let activity = self.clone();

            async move {
                match activity {
                    Activity::Ping => {
                        println!("pinged");
                    }
                    Activity::Pong => {
                        println!("ponged");
                    }
                }

                let _ = ack.send(AckMessage::Done);
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ritualist = Ritualist::new(64, Duration::from_millis(100));

    ritualist
        .register_many(vec![
            ActivitySpec {
                id: Activity::Ping,
                schedule: ActivitySchedule::FixedInterval {
                    duration: Duration::from_secs(1),
                },
            },
            ActivitySpec {
                id: Activity::Pong,
                schedule: ActivitySchedule::FixedInterval {
                    duration: Duration::from_secs(3),
                },
            },
        ])
        .await
        .expect("Could not put activities onto ritualist.");

    let mut channel = ritualist.run().take_channel();

    let listener = tokio::spawn(async move {
        while let Some((activity, ack)) = channel.recv().await {
            activity.run(ack);
        }
    });

    let _ = listener.await;

    Ok(())
}
