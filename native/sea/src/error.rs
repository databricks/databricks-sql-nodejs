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

//! Minimal kernel-error → `napi::Error` mapping.
//!
//! Round 1b: just preserves the kernel error message and translates
//! the kernel's [`ErrorCode`] into a small set of napi statuses. Round
//! 2 will add a full taxonomy (sqlState, vendorCode, retryable, …)
//! attached as own-properties on the JS error object via
//! `Env::create_error` (pattern #7 in the napi-rs patterns doc).

use databricks_sql_kernel::{Error as KernelError, ErrorCode};
use napi::{Error as NapiError, Status};

/// Map a kernel `Error` into a `napi::Error`. The kernel `ErrorCode`
/// is used to pick a sensible napi `Status`; the kernel message is
/// preserved verbatim as the error reason.
///
/// Round 1b has no callers — the scaffold doesn't return any kernel
/// errors yet. Round 2's `Database::open()` is the first consumer.
#[allow(dead_code)]
pub(crate) fn napi_err_from_kernel(e: KernelError) -> NapiError {
    let status = match e.code {
        ErrorCode::InvalidArgument | ErrorCode::InvalidStatementHandle => {
            Status::InvalidArg
        }
        ErrorCode::Cancelled => Status::Cancelled,
        // Everything else collapses to `GenericFailure`; Round 2
        // refines this with sqlState / vendorCode / category own-
        // properties on a JS error object.
        _ => Status::GenericFailure,
    };
    NapiError::new(status, e.message)
}
