use chrono::{TimeDelta, Utc};
use ritualist::{
    Ritualist, WithScheduler,
    ack::AckMessage,
    activity_spec::{ActivitySchedule, ActivitySpec},
};
use std::{
    io::{self, Write},
    sync::Arc,
    thread::Result,
    time::Duration,
};
use tokio::{
    sync::{self, Mutex},
    time::sleep,
};

mod common;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum Activity {
    Ping,
}

impl Activity {
    pub fn run(&self, ack: sync::oneshot::Sender<AckMessage>) {
        tokio::spawn({
            let activity = self.clone();
            async move {
                match activity {
                    Activity::Ping => {
                        sleep(Duration::from_secs(1)).await;
                        println!(" -> pinged");
                        io::stdout().flush().unwrap();
                        let _ = ack.send(AckMessage::Done);
                    }
                }
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ritualist = Ritualist::new(64, Duration::from_millis(100));

    ritualist
        .register_many(vec![ActivitySpec {
            id: Activity::Ping,
            schedule: ActivitySchedule::Scheduled {
                date: Utc::now() + TimeDelta::seconds(3),
            },
        }])
        .await
        .expect("Could not put activities onto ritualist.");

    let mut runner = ritualist.run();
    let mut channel = runner.take_channel();

    let ritualist = Arc::new(Mutex::new(runner));

    let listener = tokio::spawn({
        async move {
            while let Some((activity, ack)) = channel.recv().await {
                activity.run(ack);
            }
        }
    });

    tokio::spawn({
        let ritualist = ritualist.clone();
        async move {
            sleep(Duration::from_secs(6)).await;

            ritualist
                .lock()
                .await
                .register(ActivitySpec {
                    id: Activity::Ping,
                    schedule: ActivitySchedule::Scheduled {
                        date: Utc::now() + TimeDelta::seconds(2),
                    },
                })
                .await
                .expect("Could not put activities onto ritualist.");
        }
    });

    common::observer(ritualist);
    let _ = listener.await;

    Ok(())
}
