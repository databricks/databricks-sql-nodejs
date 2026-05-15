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

//! Captured tokio `Handle` for napi-rs's process-global runtime.
//!
//! Per the napi-rs patterns doc (pattern #2): the first time any
//! `#[napi] async fn` runs, we are guaranteed to be on napi-rs's tokio
//! runtime. We snapshot the current `Handle` then and stash a clone in
//! a process-static `OnceCell`. Every subsequent kernel construction
//! reads the captured handle and hands a clone to the kernel, so
//! Drop-time cleanup (which runs on the V8 GC thread, *outside* any
//! tokio context) can still `spawn` cleanup tasks onto the same
//! runtime napi-rs is driving.
//!
//! `Handle::current()` MUST NOT be called from a synchronous JS-thread
//! entry point or from module init — both run before napi-rs has
//! constructed its runtime and would panic. `get()` returns `None` in
//! that case so callers can surface a useful error rather than abort.

use once_cell::sync::OnceCell;
use tokio::runtime::Handle;

static RUNTIME_HANDLE: OnceCell<Handle> = OnceCell::new();

/// Capture the current tokio runtime handle on first call, return a
/// reference to the captured clone on subsequent calls.
///
/// MUST be called from inside a `#[napi] async fn` body (or any other
/// tokio runtime context); otherwise `Handle::current()` panics on the
/// very first call. Subsequent calls are infallible and lock-free.
///
/// Round 1b has no async entry points that exercise this yet; Round 2
/// will call it from `Database::open()` and other `#[napi] async fn`s.
#[allow(dead_code)]
pub(crate) fn get_handle() -> &'static Handle {
    RUNTIME_HANDLE.get_or_init(Handle::current)
}

/// Non-panicking accessor — returns `None` if `get_handle()` has not
/// been called yet. Drop impls and other GC-thread call sites use this
/// to short-circuit cleanup when no async entry point has ever run
/// (i.e. there is no kernel state that needs closing either).
pub(crate) fn try_get_handle() -> Option<&'static Handle> {
    RUNTIME_HANDLE.get()
}
