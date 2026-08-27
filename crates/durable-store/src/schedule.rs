use agent_loom_domain::{JsonPayload, ScheduleId, WorkflowVersionId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSchedule {
    pub schedule_id: ScheduleId,
    pub workflow_version_id: WorkflowVersionId,
    pub cron_expression: String,
    pub timezone: String,
    pub input: JsonPayload,
}
