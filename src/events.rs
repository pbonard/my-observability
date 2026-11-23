use futures::{Stream, StreamExt};
use k8s_openapi::api::core::v1::Event as CoreEvent;
use kube::runtime::watcher::Event;
use serde_json::json;

use crate::notify::Severity;
use crate::reporter::Batch;
use crate::util::log;

const ALERT_EVENT_REASONS: [&str; 8] = [
    "OOMKilling",
    "SystemOOM",
    "Evicted",
    "FailedScheduling",
    "NodeNotReady",
    "FailedMount",
    "FailedAttachVolume",
    "Missing",
];

/// Warning events need nothing remembered, each one is looked at on its own.
pub async fn run(events: impl Stream<Item = Event<CoreEvent>>, batch: Batch) {
    let mut events = std::pin::pin!(events);
    while let Some(event) = events.next().await {
        match event {
            Event::Apply(event) => on_warning(&event, &batch),
            // a list returns the events that are already there, they are history
            Event::Init | Event::InitApply(_) | Event::InitDone | Event::Delete(_) => {}
        }
    }
}

fn on_warning(event: &CoreEvent, batch: &Batch) {
    // a repeat of an event updates it, only its first time is alerted
    let new = event.series.is_none() && event.count.unwrap_or(1) <= 1;
    let object = &event.involved_object;
    let namespace = object
        .namespace
        .clone()
        .or_else(|| event.metadata.namespace.clone())
        .unwrap_or_default();
    let target = format!(
        "{}/{}",
        object.kind.as_deref().unwrap_or(""),
        object.name.as_deref().unwrap_or("")
    );
    let reason = event.reason.clone().unwrap_or_default();
    let message = event.message.clone().unwrap_or_default();
    let alert = ALERT_EVENT_REASONS.contains(&reason.as_str());
    log(
        "event",
        if alert {
            Severity::Warning
        } else {
            Severity::Info
        },
        Some(&namespace),
        Some(&target),
        json!({"reason": reason, "message": message, "count": event.count}),
    );
    if !alert || !new {
        return;
    }
    batch.alert(
        Severity::Critical,
        format!("{reason} · {namespace}/{target}"),
        &message,
    );
}
