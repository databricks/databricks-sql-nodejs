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

//! Shared helpers — one place for the `catch_unwind` wrapping that
//! every async entry point goes through (pattern #8 in the napi-rs
//! patterns doc). One helper, called once per entry point — DRY.
//!
//! Why a helper rather than a macro: helper + `async move {}` reads
//! better at call sites and keeps the stack trace shallow when a panic
//! actually fires (a macro would expand into the caller's body).

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures::FutureExt;
use napi::{Error as NapiError, Result as NapiResult, Status};

/// Run `fut` and convert any panic the future raises into a
/// `napi::Error` so the JS caller sees a rejected promise instead of
/// the Node process aborting.
///
/// `catch_unwind` does not catch `std::process::abort`, double-panic,
/// or allocator OOM — those still bring down the process. That's by
/// design: a corrupted process state isn't something we can pretend to
/// recover from.
pub(crate) async fn guarded<F, T>(fut: F) -> NapiResult<T>
where
    F: Future<Output = NapiResult<T>>,
{
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(res) => res,
        Err(panic) => Err(NapiError::new(
            Status::GenericFailure,
            format!("panic in native binding: {}", panic_payload_msg(panic)),
        )),
    }
}

/// Best-effort downcast of a panic payload to a human-readable string.
/// `panic!("…")` produces `&'static str` or `String`; the rest fall
/// through to a generic marker so the JS caller still sees *something*.
fn panic_payload_msg(p: Box<dyn Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = p.downcast_ref::<String>() {
        return s.clone();
    }
    "non-string panic payload".to_string()
}
