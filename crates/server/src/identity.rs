use std::time::{SystemTime, UNIX_EPOCH};

use agent_loom_domain::{
    CommandId, CorrelationId, Digest, IdempotencyKey, RunId, ScopeKey, TenantId,
};
use agent_loom_durable_store::CommandContext;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

pub(crate) fn random_id() -> [u8; 16] {
    *Uuid::new_v4().as_bytes()
}

pub(crate) fn derived_id(namespace: &str, value: &str) -> [u8; 16] {
    let digest: [u8; 32] = Sha256::digest(format!("{namespace}/{value}").as_bytes()).into();
    let mut id = [0; 16];
    id.copy_from_slice(&digest[..16]);
    id
}

pub(crate) fn hash_bytes(bytes: &[u8]) -> Digest {
    Digest::from_bytes(Sha256::digest(bytes).into())
}

pub(crate) fn tenant_id(key: &str) -> TenantId {
    TenantId::from_bytes(derived_id("tenant", key))
}

pub(crate) fn now_micros() -> i64 {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_micros());
    i64::try_from(micros).unwrap_or(i64::MAX)
}

pub(crate) fn decode_id(value: &str) -> Result<[u8; 16], &'static str> {
    if value.len() != 32 {
        return Err("identifier must contain 32 hexadecimal characters");
    }
    let mut bytes = [0; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&value[offset..offset + 2], 16)
            .map_err(|_| "identifier contains non-hexadecimal characters")?;
    }
    Ok(bytes)
}

pub(crate) fn command_context(
    tenant_id: TenantId,
    run_id: RunId,
    actor: &str,
    scope: &str,
    identity: &str,
    request: &[u8],
) -> Result<CommandContext, &'static str> {
    Ok(CommandContext {
        tenant_id,
        command_id: CommandId::from_bytes(derived_id("command", identity)),
        correlation_id: CorrelationId::from_bytes(derived_id("correlation", &run_id.to_string())),
        actor_ref: actor.to_owned(),
        scope: ScopeKey::parse(scope).map_err(|_| "invalid command scope")?,
        idempotency_key: IdempotencyKey::parse(identity.to_owned())
            .map_err(|_| "invalid idempotency key")?,
        request_hash: hash_bytes(request),
    })
}
