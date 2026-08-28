use std::{fmt, str::FromStr};

use agent_loom_domain::{
    ScheduleConcurrencyPolicy, ScheduleMisfirePolicy, ScheduleSnapshot, TenantId, UnixMicros,
};
use agent_loom_durable_store::{AdvanceSchedule, DueScheduleQuery, QueryContext};
use agent_loom_runtime::{PollingActivity, PollingFuture, PollingJob, PollingJobError};
use jiff_cron::{
    Schedule,
    jiff::{Timestamp, tz::TimeZone},
};

use crate::{AppState, api::dispatch_schedule_fire};

const MAX_CATCH_UP_LIMIT: u32 = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchedulePollingConfig {
    pub page_size: u32,
}

impl Default for SchedulePollingConfig {
    fn default() -> Self {
        Self { page_size: 100 }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct DispatchPlan {
    fires: Vec<UnixMicros>,
    next_fire_at: UnixMicros,
}

pub struct SchedulePollingJob {
    state: AppState,
    tenant_id: TenantId,
    config: SchedulePollingConfig,
}

impl fmt::Debug for SchedulePollingJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchedulePollingJob")
            .field("tenant_id", &self.tenant_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl SchedulePollingJob {
    pub const fn new(state: AppState, tenant_id: TenantId, config: SchedulePollingConfig) -> Self {
        Self {
            state,
            tenant_id,
            config,
        }
    }
}

impl PollingJob for SchedulePollingJob {
    fn concurrency_limit(&self) -> u32 {
        1
    }

    fn run_once(&self, _slot: u32) -> PollingFuture<'_> {
        Box::pin(async move {
            let query_context = QueryContext {
                tenant_id: self.tenant_id,
                actor_ref: "agent-loom-schedule-scanner".to_owned(),
                authoritative: true,
            };
            let page = self
                .state
                .store
                .scan_due_schedules(
                    &query_context,
                    DueScheduleQuery {
                        limit: self.config.page_size,
                    },
                )
                .await
                .map_err(|error| polling_error(error.safe_message))?;
            if page.schedules.is_empty() {
                return Ok(PollingActivity::Idle);
            }

            let mut completed = 0;
            let mut failed = 0;
            let mut last_failure = None;
            let now = page.database_now;
            for schedule in page.schedules {
                let result =
                    dispatch_due_schedule(&self.state, &query_context, &schedule, now).await;
                match result {
                    Ok(true) => completed += 1,
                    Ok(false) => {}
                    Err(message) => {
                        failed += 1;
                        last_failure = Some(message);
                    }
                }
            }
            Ok(PollingActivity::Progress {
                completed,
                failed,
                last_failure,
            })
        })
    }
}

async fn dispatch_due_schedule(
    state: &AppState,
    query_context: &QueryContext,
    schedule: &ScheduleSnapshot,
    now: UnixMicros,
) -> Result<bool, String> {
    let plan = if schedule.concurrency_policy == ScheduleConcurrencyPolicy::Forbid
        && state
            .store
            .has_active_schedule_runs(query_context, schedule.schedule_id)
            .await
            .map_err(|error| error.safe_message)?
    {
        let mut blocked = schedule.clone();
        blocked.misfire_policy = ScheduleMisfirePolicy::Skip;
        dispatch_plan(&blocked, now)?
    } else {
        dispatch_plan(schedule, now)?
    };
    for fire_at in &plan.fires {
        dispatch_schedule_fire(state, schedule, *fire_at).await?;
    }
    state
        .store
        .advance_schedule(
            query_context,
            AdvanceSchedule {
                schedule_id: schedule.schedule_id,
                expected_version: schedule.version,
                expected_next_fire_at: schedule.next_fire_at,
                last_fire_at: plan.fires.last().copied(),
                next_fire_at: plan.next_fire_at,
            },
        )
        .await
        .map_err(|error| error.safe_message)
}

pub(crate) fn validate_schedule_definition(expression: &str, timezone: &str) -> Result<(), String> {
    if expression.split_ascii_whitespace().count() != 5 {
        return Err("cron_expression must be a five-field Cron expression".to_owned());
    }
    parse_schedule(expression)?;
    TimeZone::get(timezone).map_err(|_| "timezone must be a valid IANA timezone".to_owned())?;
    Ok(())
}

pub(crate) fn next_fire_after(
    expression: &str,
    timezone: &str,
    after: UnixMicros,
) -> Result<UnixMicros, String> {
    let schedule = parse_schedule(expression)?;
    let timezone =
        TimeZone::get(timezone).map_err(|_| "timezone must be a valid IANA timezone".to_owned())?;
    next_after(&schedule, timezone, after)
}

fn dispatch_plan(schedule: &ScheduleSnapshot, now: UnixMicros) -> Result<DispatchPlan, String> {
    if schedule.next_fire_at.get() > now.get() {
        return Ok(DispatchPlan {
            fires: Vec::new(),
            next_fire_at: schedule.next_fire_at,
        });
    }
    let parsed = parse_schedule(&schedule.cron_expression)?;
    let timezone = TimeZone::get(&schedule.timezone)
        .map_err(|_| "persisted Schedule timezone is invalid".to_owned())?;
    let cursor = Timestamp::from_microsecond(schedule.next_fire_at.get())
        .map_err(|_| "persisted Schedule cursor is outside the supported range".to_owned())?
        .to_zoned(timezone.clone());
    if !parsed.includes(cursor) {
        return Ok(DispatchPlan {
            fires: Vec::new(),
            next_fire_at: next_after(&parsed, timezone, now)?,
        });
    }
    match schedule.misfire_policy {
        ScheduleMisfirePolicy::Skip => Ok(DispatchPlan {
            fires: Vec::new(),
            next_fire_at: next_after(&parsed, timezone, now)?,
        }),
        ScheduleMisfirePolicy::FireOnce => Ok(DispatchPlan {
            fires: vec![schedule.next_fire_at],
            next_fire_at: next_after(&parsed, timezone, now)?,
        }),
        ScheduleMisfirePolicy::CatchUp => {
            let mut fires = Vec::new();
            let mut cursor = schedule.next_fire_at;
            let limit = schedule.catch_up_limit.clamp(1, MAX_CATCH_UP_LIMIT);
            while cursor.get() <= now.get() && fires.len() < limit as usize {
                fires.push(cursor);
                cursor = next_after(&parsed, timezone.clone(), cursor)?;
            }
            Ok(DispatchPlan {
                fires,
                next_fire_at: cursor,
            })
        }
    }
}

fn parse_schedule(expression: &str) -> Result<Schedule, String> {
    Schedule::from_str(&format!("0 {expression} *"))
        .map_err(|_| "cron_expression must be a valid five-field Cron expression".to_owned())
}

fn next_after(
    schedule: &Schedule,
    timezone: TimeZone,
    after: UnixMicros,
) -> Result<UnixMicros, String> {
    let timestamp = Timestamp::from_microsecond(after.get())
        .map_err(|_| "Schedule timestamp is outside the supported range".to_owned())?;
    schedule
        .after(timestamp.to_zoned(timezone))
        .next()
        .map(|value| UnixMicros::new(value.timestamp().as_microsecond()))
        .ok_or_else(|| "Cron expression has no later fire time".to_owned())
}

fn polling_error(message: impl Into<String>) -> PollingJobError {
    PollingJobError {
        safe_message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_loom_domain::{JsonPayload, ScheduleId, ScheduleStatus, WorkflowVersionId};

    fn snapshot(policy: ScheduleMisfirePolicy, next_fire_at: i64, limit: u32) -> ScheduleSnapshot {
        ScheduleSnapshot {
            tenant_id: TenantId::from_bytes([1; 16]),
            schedule_id: ScheduleId::from_bytes([2; 16]),
            workflow_version_id: WorkflowVersionId::from_bytes([3; 16]),
            cron_expression: "* * * * *".to_owned(),
            timezone: "UTC".to_owned(),
            input: JsonPayload::from_validated_bytes(b"{}".to_vec()),
            status: ScheduleStatus::Active,
            misfire_policy: policy,
            concurrency_policy: ScheduleConcurrencyPolicy::Allow,
            catch_up_limit: limit,
            next_fire_at: UnixMicros::new(next_fire_at),
            last_fire_at: None,
            version: 0,
            created_at: UnixMicros::new(1),
            updated_at: UnixMicros::new(1),
        }
    }

    #[test]
    fn misfire_policies_skip_fire_once_and_bound_catch_up() {
        let now = UnixMicros::new(10 * 60 * 1_000_000);
        let first = 7 * 60 * 1_000_000;
        let skip = dispatch_plan(&snapshot(ScheduleMisfirePolicy::Skip, first, 1), now).unwrap();
        assert!(skip.fires.is_empty());
        assert_eq!(skip.next_fire_at.get(), 11 * 60 * 1_000_000);

        let once =
            dispatch_plan(&snapshot(ScheduleMisfirePolicy::FireOnce, first, 1), now).unwrap();
        assert_eq!(once.fires, vec![UnixMicros::new(first)]);
        assert_eq!(once.next_fire_at.get(), 11 * 60 * 1_000_000);

        let catch_up =
            dispatch_plan(&snapshot(ScheduleMisfirePolicy::CatchUp, first, 2), now).unwrap();
        assert_eq!(
            catch_up.fires,
            vec![UnixMicros::new(first), UnixMicros::new(8 * 60 * 1_000_000)]
        );
        assert_eq!(catch_up.next_fire_at.get(), 9 * 60 * 1_000_000);
    }

    #[test]
    fn iana_timezone_preserves_dst_fold_and_skips_gap() {
        let fold_start = "2022-11-06T05:30:00Z".parse::<Timestamp>().unwrap();
        let first = next_fire_after(
            "0 * * * *",
            "America/Chicago",
            UnixMicros::new(fold_start.as_microsecond()),
        )
        .unwrap();
        let second = next_fire_after("0 * * * *", "America/Chicago", first).unwrap();
        assert_eq!(second.get() - first.get(), 60 * 60 * 1_000_000);
        let zone = TimeZone::get("America/Chicago").unwrap();
        assert_eq!(
            Timestamp::from_microsecond(first.get())
                .unwrap()
                .to_zoned(zone.clone())
                .to_string(),
            "2022-11-06T01:00:00-05:00[America/Chicago]"
        );
        assert_eq!(
            Timestamp::from_microsecond(second.get())
                .unwrap()
                .to_zoned(zone)
                .to_string(),
            "2022-11-06T01:00:00-06:00[America/Chicago]"
        );

        let gap_start = "2022-03-12T12:00:00Z".parse::<Timestamp>().unwrap();
        let gap = next_fire_after(
            "30 2 * * *",
            "America/Chicago",
            UnixMicros::new(gap_start.as_microsecond()),
        )
        .unwrap();
        assert_eq!(
            Timestamp::from_microsecond(gap.get())
                .unwrap()
                .to_zoned(TimeZone::get("America/Chicago").unwrap())
                .to_string(),
            "2022-03-13T03:30:00-05:00[America/Chicago]"
        );
    }
}
