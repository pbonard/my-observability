use std::fmt::Debug;
use std::str::FromStr;

use futures::{Stream, StreamExt, future};
use k8s_openapi::api::core::v1::{Event as CoreEvent, Node};
use kube::runtime::{WatchStreamExt, watcher};
use kube::{Api, Client, Resource};
use serde::de::DeserializeOwned;
use tokio::task::JoinSet;
use tracing::{info, warn};

use nodes::Nodes;
use notify::Discord;
use pods::Pods;
use reporter::Batch;

mod events;
mod nodes;
mod notify;
mod pods;
mod reporter;
mod util;

fn env_or<T: FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// A watch that reconnects by itself, errors are only logged.
fn watch<K>(api: Api<K>, config: watcher::Config) -> impl Stream<Item = watcher::Event<K>> + Send
where
    K: Resource + Clone + DeserializeOwned + Debug + Send + 'static,
{
    watcher(api, config).default_backoff().filter_map(|event| {
        future::ready(match event {
            Ok(event) => Some(event),
            Err(e) => {
                warn!("watch restarts after error: {e}");
                None
            }
        })
    })
}

// two threads keep the watches parallel without a thread and an allocator arena per core
#[tokio::main(worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_writer(std::io::stdout)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let client = Client::try_default().await?;
    // each watch owns its own state and runs on its own task, the batch is the only thing they share
    let batch = Batch::default();
    let mut tasks = JoinSet::new();
    tasks.spawn(
        Pods::new(client.clone(), env_or("LOG_TAIL_LINES", 30), batch.clone()).run(watch(
            pods::api(client.clone()),
            watcher::Config::default().streaming_lists(),
        )),
    );
    tasks.spawn(events::run(
        watch(
            Api::<CoreEvent>::all(client.clone()),
            watcher::Config::default()
                .fields("type=Warning")
                .streaming_lists(),
        ),
        batch.clone(),
    ));
    tasks.spawn(Nodes::new(batch.clone()).run(watch(
        Api::<Node>::all(client.clone()),
        watcher::Config::default().streaming_lists(),
    )));
    tasks.spawn(reporter::run(batch, Discord::from_env()));
    info!("watching pods, warning events and nodes");

    let ended = tasks.join_next().await;
    anyhow::bail!("a task stopped: {ended:?}")
}
