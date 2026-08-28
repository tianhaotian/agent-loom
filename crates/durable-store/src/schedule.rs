use agent_loom_domain::{
    JsonPayload, ScheduleId, ScheduleMisfirePolicy, ScheduleSnapshot, UnixMicros, WorkflowVersionId,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSchedule {
    pub schedule_id: ScheduleId,
    pub workflow_version_id: WorkflowVersionId,
    pub cron_expression: String,
    pub timezone: String,
    pub input: JsonPayload,
    pub misfire_policy: ScheduleMisfirePolicy,
    pub catch_up_limit: u32,
    pub next_fire_at: UnixMicros,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DueScheduleQuery {
    pub limit: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueSchedulePage {
    pub database_now: UnixMicros,
    pub schedules: Vec<ScheduleSnapshot>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdvanceSchedule {
    pub schedule_id: ScheduleId,
    pub expected_version: u64,
    pub expected_next_fire_at: UnixMicros,
    pub last_fire_at: Option<UnixMicros>,
    pub next_fire_at: UnixMicros,
}
