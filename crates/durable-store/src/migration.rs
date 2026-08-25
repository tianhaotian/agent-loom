#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddedMigration {
    pub logical_id: &'static str,
    pub logical_model_version: u64,
    pub sql: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MigrationState {
    Applying,
    Applied,
    Failed,
}
