use std::collections::HashMap;

use futures::{Stream, StreamExt};
use k8s_openapi::api::core::v1::Node;
use kube::ResourceExt;
use kube::runtime::watcher::Event;
use serde_json::json;

use crate::notify::Severity;
use crate::reporter::Batch;
use crate::util::log;

const NODE_BAD: [(&str, &str); 4] = [
    ("Ready", "False"),
    ("MemoryPressure", "True"),
    ("DiskPressure", "True"),
    ("PIDPressure", "True"),
];

/// Owned by the node watch task alone, so what it remembers needs no lock.
pub struct Nodes {
    batch: Batch,
    /// (node, condition) -> status
    conditions: HashMap<(String, String), String>,
}

impl Nodes {
    pub fn new(batch: Batch) -> Self {
        Self {
            batch,
            conditions: HashMap::new(),
        }
    }

    pub async fn run(mut self, events: impl Stream<Item = Event<Node>>) {
        let mut events = std::pin::pin!(events);
        while let Some(event) = events.next().await {
            match event {
                Event::Apply(node) | Event::InitApply(node) => self.on_node(&node),
                Event::Init | Event::InitDone | Event::Delete(_) => {}
            }
        }
    }

    fn on_node(&mut self, node: &Node) {
        let name = node.name_any();
        let conditions = node.status.as_ref().and_then(|s| s.conditions.as_ref());
        for c in conditions.into_iter().flatten() {
            let Some((_, bad_status)) = NODE_BAD.iter().find(|(kind, _)| *kind == c.type_) else {
                continue;
            };
            let previous = self
                .conditions
                .insert((name.clone(), c.type_.clone()), c.status.clone());
            let bad = c.status == *bad_status;
            // first sight only remembers the status
            let changed = previous
                .as_deref()
                .is_some_and(|previous| previous != c.status);
            if !changed {
                continue;
            }
            let severity = if bad {
                Severity::Critical
            } else {
                Severity::Resolved
            };
            let message = c.message.clone().unwrap_or_default();
            log(
                "node",
                severity,
                None,
                Some(&name),
                json!({"condition": c.type_, "from": previous, "to": c.status, "message": message}),
            );
            self.batch.alert(
                severity,
                format!("node {name}: {}={}", c.type_, c.status),
                &message,
            );
        }
    }
}
