use std::{
    fmt,
    sync::{Arc, Mutex},
};

use agent_loom_domain::{EventId, TenantId};
use agent_loom_durable_store::{
    ApplyMaintenance, DurableStore, MaintenanceCandidate, MaintenanceCursor, MaintenanceKind,
    MaintenanceQuery, QueryContext,
};
use agent_loom_runtime::{PollingActivity, PollingFuture, PollingJob, PollingJobError};

use crate::identity::{command_context, derived_id};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaintenancePollingConfig {
    pub page_size: u32,
    pub stale_after_micros: u64,
}

impl Default for MaintenancePollingConfig {
    fn default() -> Self {
        Self {
            page_size: 100,
            stale_after_micros: 5 * 60 * 1_000_000,
        }
    }
}

pub struct MaintenancePollingJob {
    store: Arc<dyn DurableStore>,
    tenant_id: TenantId,
    cursor: Mutex<Option<MaintenanceCursor>>,
    config: MaintenancePollingConfig,
}

impl fmt::Debug for MaintenancePollingJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaintenancePollingJob")
            .field("tenant_id", &self.tenant_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl MaintenancePollingJob {
    pub fn new(
        store: Arc<dyn DurableStore>,
        tenant_id: TenantId,
        config: MaintenancePollingConfig,
    ) -> Self {
        Self {
            store,
            tenant_id,
            cursor: Mutex::new(None),
            config,
        }
    }
}

impl PollingJob for MaintenancePollingJob {
    fn concurrency_limit(&self) -> u32 {
        1
    }

    fn run_once(&self, _slot: u32) -> PollingFuture<'_> {
        Box::pin(async move {
            let after = self
                .cursor
                .lock()
                .map_err(|_| polling_error("maintenance cursor lock is unavailable"))?
                .to_owned();
            let page = self
                .store
                .scan_maintenance(
                    &QueryContext {
                        tenant_id: self.tenant_id,
                        actor_ref: "agent-loom-maintenance".to_owned(),
                        authoritative: true,
                    },
                    MaintenanceQuery {
                        after,
                        limit: self.config.page_size,
                        stale_after_micros: self.config.stale_after_micros,
                    },
                )
                .await
                .map_err(|error| polling_error(error.safe_message))?;
            let scanned = page.candidates.len();
            let mut completed = 0_u64;
            let mut failed = 0_u64;
            let mut last_failure = None;
            for candidate in page.candidates {
                let identity = maintenance_identity(candidate);
                let context = command_context(
                    self.tenant_id,
                    candidate.run_id,
                    "agent-loom-maintenance",
                    "apply_maintenance",
                    &identity,
                    identity.as_bytes(),
                )
                .map_err(polling_error)?;
                let command = ApplyMaintenance {
                    candidate,
                    event_id: EventId::from_bytes(derived_id("event", &identity)),
                };
                match self.store.apply_maintenance(&context, command).await {
                    Ok(Some(_)) => completed += 1,
                    Ok(None) => {}
                    Err(error) => {
                        failed += 1;
                        last_failure = Some(error.safe_message);
                    }
                }
            }
            *self
                .cursor
                .lock()
                .map_err(|_| polling_error("maintenance cursor lock is unavailable"))? =
                page.next_cursor;
            if scanned == 0 {
                Ok(PollingActivity::Idle)
            } else {
                Ok(PollingActivity::Progress {
                    completed,
                    failed,
                    last_failure,
                })
            }
        })
    }
}

fn maintenance_identity(candidate: MaintenanceCandidate) -> String {
    format!(
        "maintenance/{}/{}/{}/{}",
        maintenance_kind(candidate.kind()),
        candidate.run_id,
        hex(candidate.target_id_bytes()),
        candidate.expected_revision
    )
}

const fn maintenance_kind(kind: MaintenanceKind) -> &'static str {
    match kind {
        MaintenanceKind::RunDeadline => "run-deadline",
        MaintenanceKind::WaitTimeout => "wait-timeout",
        MaintenanceKind::ToolStale => "tool-stale",
        MaintenanceKind::AgentStale => "agent-stale",
    }
}

fn hex(bytes: [u8; 16]) -> String {
    use fmt::Write as _;
    let mut value = String::with_capacity(32);
    for byte in bytes {
        let _ = write!(value, "{byte:02x}");
    }
    value
}

fn polling_error(message: impl Into<String>) -> PollingJobError {
    PollingJobError {
        safe_message: message.into(),
    }
}
