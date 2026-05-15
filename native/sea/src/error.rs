// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Kernel-error → `napi::Error` mapping.
//!
//! The kernel returns a richly-typed [`Error`](databricks_sql_kernel::Error)
//! with `code`, `sql_state`, `error_code`, `vendor_code`, `http_status`,
//! `retryable`, and `query_id` fields. The napi `Error` type only
//! carries `status` + `reason` directly — to attach the extra fields
//! as own-properties on the JS error object we'd need an `Env`
//! reference, which `#[napi] async fn` bodies don't have access to
//! cheaply.
//!
//! Compromise (one helper, DRY): encode the structured metadata into
//! the `reason` field as a JSON envelope prefixed with a sentinel
//! `__databricks_error__:` token. The TS adapter detects the sentinel,
//! parses the payload, and reconstructs the typed error class
//! (`DBSQLError`, `AuthError`, …). Plain-string errors from the
//! binding's own code paths fall through the sentinel detection
//! unchanged.
//!
//! Round 3 may switch to the `Env::create_error` + own-properties
//! pattern once we have a stable point in each entry where `env: Env`
//! is available (likely by wrapping the async glue in a sync entry
//! point that calls `tokio::spawn` after capturing `env`).

use databricks_sql_kernel::{Error as KernelError, ErrorCode};
use napi::{Error as NapiError, Status};

/// Sentinel that tells the TS adapter the `reason` string is a JSON
/// envelope rather than a plain message. Has to be ASCII-only so it
/// survives any `String` round-trip the napi layer might do.
pub(crate) const ERROR_SENTINEL: &str = "__databricks_error__:";

/// Map a kernel [`Error`] into a `napi::Error`. Preserves the kernel
/// `ErrorCode` (mapped to the closest napi `Status`), and stuffs the
/// remaining structured fields into a JSON envelope on the reason so
/// the TS layer can reconstruct the typed error class.
pub(crate) fn napi_err_from_kernel(e: KernelError) -> NapiError {
    let status = status_from_kernel_code(e.code);

    // Build a minimal JSON envelope. We hand-build it (no serde_json
    // dep) — the field set is small and fixed, and avoiding serde
    // keeps the crate dep graph trim.
    let mut envelope = String::with_capacity(e.message.len() + 128);
    envelope.push_str(ERROR_SENTINEL);
    envelope.push('{');
    push_json_str_field(&mut envelope, "code", error_code_str(e.code));
    envelope.push(',');
    push_json_str_field(&mut envelope, "message", &e.message);
    if let Some(s) = &e.sql_state {
        envelope.push(',');
        push_json_str_field(&mut envelope, "sqlState", s);
    }
    if let Some(ec) = &e.error_code {
        envelope.push(',');
        push_json_str_field(&mut envelope, "errorCode", ec);
    }
    if let Some(vc) = e.vendor_code {
        envelope.push(',');
        envelope.push_str("\"vendorCode\":");
        envelope.push_str(&vc.to_string());
    }
    if let Some(hs) = e.http_status {
        envelope.push(',');
        envelope.push_str("\"httpStatus\":");
        envelope.push_str(&hs.to_string());
    }
    if e.retryable {
        envelope.push_str(",\"retryable\":true");
    }
    if let Some(qid) = &e.query_id {
        envelope.push(',');
        push_json_str_field(&mut envelope, "queryId", qid);
    }
    envelope.push('}');

    NapiError::new(status, envelope)
}

/// Map kernel `ErrorCode` → napi `Status`. The status is mostly
/// cosmetic on the napi side (the TS layer dispatches on `code` from
/// the envelope); we pick the closest match so unwrapped errors still
/// look reasonable in raw napi consumers.
fn status_from_kernel_code(code: ErrorCode) -> Status {
    match code {
        ErrorCode::InvalidArgument | ErrorCode::InvalidStatementHandle => Status::InvalidArg,
        ErrorCode::Cancelled => Status::Cancelled,
        _ => Status::GenericFailure,
    }
}

/// String tag for each kernel `ErrorCode` — stable across kernel
/// versions because v0's `ErrorCode` is `#[non_exhaustive]` and we
/// pattern-match exhaustively against the known set.
fn error_code_str(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::InvalidArgument => "InvalidArgument",
        ErrorCode::Unauthenticated => "Unauthenticated",
        ErrorCode::PermissionDenied => "PermissionDenied",
        ErrorCode::NotFound => "NotFound",
        ErrorCode::ResourceExhausted => "ResourceExhausted",
        ErrorCode::Unavailable => "Unavailable",
        ErrorCode::Timeout => "Timeout",
        ErrorCode::Cancelled => "Cancelled",
        ErrorCode::DataLoss => "DataLoss",
        ErrorCode::Internal => "Internal",
        ErrorCode::InvalidStatementHandle => "InvalidStatementHandle",
        ErrorCode::NetworkError => "NetworkError",
        ErrorCode::SqlError => "SqlError",
        // Forward-compat: ErrorCode is `#[non_exhaustive]`. Any new
        // variant the kernel adds in v0.x lands here until we mirror
        // it in this match. The TS layer treats Unknown as a generic
        // failure.
        _ => "Unknown",
    }
}

/// Append `"key":"value"` to the JSON buffer, escaping the value's
/// `"` and `\` characters and control chars to keep the envelope
/// JSON-parseable. The narrow set of escapes is sufficient for the
/// human-readable error messages the kernel produces (no embedded
/// binary blobs, no Unicode surrogate pairs).
fn push_json_str_field(out: &mut String, key: &str, value: &str) {
    out.push('"');
    out.push_str(key);
    out.push_str("\":\"");
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}
