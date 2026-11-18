use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use futures::{Stream, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::LogParams;
use kube::core::{ApiResource, Object};
use kube::runtime::watcher::Event;
use kube::{Api, Client, ResourceExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::time::timeout;

use crate::notify::Severity;
use crate::reporter::Batch;
use crate::util::log;

const CRASH_LOOP_RESTARTS: usize = 3;
const CRASH_LOOP_WINDOW: f64 = 3600.0;

/// (namespace, pod, container)
type ContainerKey = (String, String, String);

/// A pod with only the fields read here, serde skips the rest of the JSON without keeping it.
pub type SlimPod = Object<SlimPodSpec, SlimPodStatus>;

/// The pod API, with pods deserialized as [`SlimPod`].
pub fn api(client: Client) -> Api<SlimPod> {
    Api::all_with(client, &ApiResource::erase::<Pod>(&()))
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SlimPodSpec {
    node_name: Option<String>,
    #[serde(default)]
    containers: Vec<SlimContainer>,
}

#[derive(Clone, Debug, Deserialize)]
struct SlimContainer {
    name: String,
    resources: Option<SlimResources>,
}

#[derive(Clone, Debug, Deserialize)]
struct SlimResources {
    limits: Option<SlimLimits>,
}

#[derive(Clone, Debug, Deserialize)]
struct SlimLimits {
    memory: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SlimPodStatus {
    #[serde(default)]
    container_statuses: Vec<ContainerStatus>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContainerStatus {
    name: String,
    restart_count: i32,
    last_state: Option<ContainerState>,
}

#[derive(Clone, Debug, Deserialize)]
struct ContainerState {
    terminated: Option<Terminated>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Terminated {
    exit_code: i32,
    reason: Option<String>,
    started_at: Option<String>,
    finished_at: Option<String>,
}

#[derive(Serialize)]
struct Restart {
    namespace: String,
    pod: String,
    container: String,
    node: Option<String>,
    exit_code: i32,
    reason: Option<String>,
    started_at: Option<String>,
    finished_at: Option<String>,
    restart_count: i32,
    memory_limit: Option<f64>,
    cause: &'static str,
    crash_loop: bool,
    last_logs: String,
    #[serde(skip)]
    restarts_in_window: usize,
    #[serde(skip)]
    severity: Severity,
}

impl Restart {
    fn line(&self) -> String {
        let what = match self.cause {
            "oom" => format!("out of memory (limit {})", mib(self.memory_limit)),
            "killed" => "killed (SIGKILL)".to_string(),
            _ => format!("exited with {}", self.exit_code),
        };
        let crash_loop = if self.crash_loop {
            format!(
                "crash loop, {} restarts in the last hour · ",
                self.restarts_in_window
            )
        } else {
            String::new()
        };
        format!(
            "restart {}/{} [{}] · {crash_loop}{what} · restart #{}",
            self.namespace, self.pod, self.container, self.restart_count
        )
    }
}

/// Owned by the pod watch task alone, so what it remembers needs no lock.
pub struct Pods {
    client: Client,
    log_tail_lines: i64,
    batch: Batch,
    restart_counts: HashMap<ContainerKey, i32>,
    restart_times: HashMap<ContainerKey, VecDeque<f64>>,
    crash_loop_alerted: HashMap<ContainerKey, f64>,
    /// containers seen by a list in progress, the ones missing from it are forgotten when it is done
    listed: Option<HashSet<ContainerKey>>,
}

impl Pods {
    pub fn new(client: Client, log_tail_lines: i64, batch: Batch) -> Self {
        Self {
            client,
            log_tail_lines,
            batch,
            restart_counts: HashMap::new(),
            restart_times: HashMap::new(),
            crash_loop_alerted: HashMap::new(),
            listed: None,
        }
    }

    pub async fn run(mut self, events: impl Stream<Item = Event<SlimPod>>) {
        let mut events = std::pin::pin!(events);
        while let Some(event) = events.next().await {
            self.on_pod(&event);
        }
    }

    fn on_pod(&mut self, event: &Event<SlimPod>) {
        match event {
            Event::Init => self.listed = Some(HashSet::new()),
            Event::InitApply(pod) => {
                if let Some(listed) = self.listed.as_mut() {
                    listed.extend(container_statuses(pod).map(|cs| container_key(pod, cs)));
                }
                self.on_pod_apply(pod);
            }
            // a pod deleted while the watch was reconnecting is only missing from the new list, no Delete arrives for it
            Event::InitDone => {
                if let Some(listed) = self.listed.take() {
                    self.restart_counts.retain(|key, _| listed.contains(key));
                    self.restart_times.retain(|key, _| listed.contains(key));
                    self.crash_loop_alerted
                        .retain(|key, _| listed.contains(key));
                }
            }
            Event::Apply(pod) => self.on_pod_apply(pod),
            Event::Delete(pod) => {
                for cs in container_statuses(pod) {
                    let key = container_key(pod, cs);
                    self.restart_counts.remove(&key);
                    self.restart_times.remove(&key);
                    self.crash_loop_alerted.remove(&key);
                }
            }
        }
    }

    fn on_pod_apply(&mut self, pod: &SlimPod) {
        for cs in container_statuses(pod) {
            let key = container_key(pod, cs);
            let previous = self.restart_counts.insert(key.clone(), cs.restart_count);
            // first sight only remembers the count, earlier restarts are history
            let restarted = previous.is_some_and(|previous| cs.restart_count > previous);
            if !restarted {
                continue;
            }
            if let Some(terminated) = cs.last_state.as_ref().and_then(|s| s.terminated.as_ref()) {
                self.on_restart(pod, cs, terminated, key);
            }
        }
    }

    fn on_restart(
        &mut self,
        pod: &SlimPod,
        cs: &ContainerStatus,
        terminated: &Terminated,
        key: ContainerKey,
    ) {
        let oom = terminated.reason.as_deref() == Some("OOMKilled");
        let at = now();
        let times = self.restart_times.entry(key.clone()).or_default();
        times.push_back(at);
        while times.front().is_some_and(|t| at - t > CRASH_LOOP_WINDOW) {
            times.pop_front();
        }
        let restarts_in_window = times.len();
        let crash_loop = restarts_in_window >= CRASH_LOOP_RESTARTS
            && at - self.crash_loop_alerted.get(&key).copied().unwrap_or(0.0) > CRASH_LOOP_WINDOW;
        if crash_loop {
            self.crash_loop_alerted.insert(key.clone(), at);
        }
        let cause = match (oom, terminated.exit_code) {
            (true, _) => "oom",
            (false, 137) => "killed",
            (false, 0) => "exited",
            _ => "error",
        };
        let severity = if oom || crash_loop {
            Severity::Critical
        } else if terminated.exit_code == 137 {
            Severity::Warning
        } else {
            Severity::Info
        };
        let memory_limit = pod
            .spec
            .containers
            .iter()
            .find(|c| c.name == cs.name)
            .and_then(|c| c.resources.as_ref()?.limits.as_ref()?.memory.as_deref())
            .and_then(parse_quantity);
        let (namespace, pod_name, container) = key;
        let restart = Restart {
            namespace,
            pod: pod_name,
            container,
            node: pod.spec.node_name.clone(),
            exit_code: terminated.exit_code,
            reason: terminated.reason.clone(),
            started_at: terminated.started_at.clone(),
            finished_at: terminated.finished_at.clone(),
            restart_count: cs.restart_count,
            memory_limit,
            cause,
            crash_loop,
            restarts_in_window,
            severity,
            last_logs: String::new(),
        };
        // the log fetch runs on its own task so the pod watch keeps going meanwhile
        tokio::spawn(report_restart(
            self.client.clone(),
            self.log_tail_lines,
            restart,
            self.batch.clone(),
        ));
    }
}

/// Fetches the logs of the container that just died, logs the restart with them and adds it to the batch.
async fn report_restart(client: Client, tail_lines: i64, mut restart: Restart, batch: Batch) {
    let pods: Api<Pod> = Api::namespaced(client, &restart.namespace);
    let params = LogParams {
        container: Some(restart.container.clone()),
        previous: true,
        tail_lines: Some(tail_lines),
        ..Default::default()
    };
    restart.last_logs =
        match timeout(Duration::from_secs(5), pods.logs(&restart.pod, &params)).await {
            Ok(Ok(logs)) => logs,
            Ok(Err(e)) => format!("(previous logs unavailable: {e})"),
            Err(_) => "(previous logs unavailable: timed out)".into(),
        };
    log(
        "restart",
        restart.severity,
        Some(&restart.namespace),
        Some(&restart.pod),
        json!(&restart),
    );
    if restart.severity != Severity::Info {
        batch.push(restart.severity, restart.line());
    }
}

const UNITS: [(&str, f64); 9] = [
    ("Ki", 1024.0),
    ("Mi", 1048576.0),
    ("Gi", 1073741824.0),
    ("Ti", 1099511627776.0),
    ("k", 1e3),
    ("M", 1e6),
    ("G", 1e9),
    ("T", 1e12),
    ("m", 1e-3),
];

fn parse_quantity(quantity: &str) -> Option<f64> {
    for (unit, factor) in UNITS {
        if let Some(number) = quantity.strip_suffix(unit) {
            return number.parse::<f64>().ok().map(|n| n * factor);
        }
    }
    quantity.parse().ok()
}

fn mib(bytes: Option<f64>) -> String {
    bytes.map_or("?".into(), |b| format!("{:.0}Mi", b / 1048576.0))
}

fn now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn container_key(pod: &SlimPod, cs: &ContainerStatus) -> ContainerKey {
    (
        pod.namespace().unwrap_or_default(),
        pod.name_any(),
        cs.name.clone(),
    )
}

fn container_statuses(pod: &SlimPod) -> impl Iterator<Item = &ContainerStatus> {
    pod.status.iter().flat_map(|s| &s.container_statuses)
}
