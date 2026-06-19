use ritualist::{
    Ritualist, WithScheduler,
    ack::AckMessage,
    activity_spec::{ActivitySchedule, ActivitySpec},
};
use std::{sync::Arc, thread::Result, time::Duration};
use tokio::{
    sync::{self},
    time::sleep,
};

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
                        println!("pinged");
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
        .register(ActivitySpec {
            id: Activity::Ping,
            schedule: ActivitySchedule::FixedInterval {
                duration: Duration::from_secs(1),
            },
        })
        .await
        .expect("Could not put activities onto ritualist.");

    let mut ritualist = ritualist.run();
    let mut channel = ritualist.take_channel();

    let listener = tokio::spawn(async move {
        while let Some((activity, ack)) = channel.recv().await {
            activity.run(ack);
        }
    });

    // lets pause ping for 5 seconds after 6 seconds( 6x ping ticks )
    tokio::spawn({
        async move {
            sleep(Duration::from_secs(6)).await;

            println!("pausing ping for 5 sec");
            ritualist.set_enabled(Activity::Ping, false).await;
            sleep(Duration::from_secs(5)).await;
            ritualist.set_enabled(Activity::Ping, true).await;
        }
    });

    let _ = listener.await;

    Ok(())
}
