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

//! Opaque `Database` wrapper around the kernel's `Session` handle.
//!
//! Round 1b: scaffold only — `constructor` stores options and returns
//! immediately. Round 2 will add `open()` (calling `Session::open`),
//! `statement()`, `close()`, etc.
//!
//! The kernel collapses ADBC's `Database` + `Connection` into a single
//! `Session`. We keep the wrapper name `Database` on the JS side
//! because that matches the existing Node driver's mental model; the
//! actual session lives inside this struct.

use databricks_sql_kernel::Session;

use crate::runtime;

/// JS-visible constructor options. Round 2 will populate this with
/// real fields (host, warehouseId, auth, …); for the scaffold it is
/// intentionally empty so the JS smoke test can call `new Database({})`
/// without TypeScript complaining about unknown properties.
#[napi(object)]
pub struct DatabaseOptions {
    /// Workspace host URL (e.g. `https://workspace.databricks.com`).
    /// Optional in Round 1b; Round 2 makes it required.
    pub host: Option<String>,
    /// Warehouse id. Optional in Round 1b; Round 2 makes it required.
    pub warehouse_id: Option<String>,
}

/// Opaque database handle on the JS side.
///
/// Holds `Option<Session>` so `close()` (Round 2) can `.take()` the
/// session out and `.await` an async close, leaving `inner = None`.
/// The `Drop` impl checks `inner` to decide whether to schedule a
/// fire-and-forget close on the captured tokio runtime.
#[napi]
pub struct Database {
    // TODO(round-2): populate this from `Session::open(config).await`
    // inside an `open()` async method (or directly inside the
    // constructor via a factory pattern). For now it stays `None` so
    // Drop has nothing to clean up.
    inner: Option<Session>,
}

#[napi]
impl Database {
    /// Construct a new database handle. Round 1b: the options are
    /// stashed for diagnostic purposes only — no network call.
    #[napi(constructor)]
    pub fn new(_options: DatabaseOptions) -> Self {
        Database { inner: None }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Pattern #5 from the napi-rs patterns doc: spawn cleanup on
        // the captured runtime handle. We only enter this branch if
        // the JS user dropped the handle without calling `close()`
        // first (which Round 2 will provide). For Round 1b there is
        // nothing to clean up, but the pattern is in place so the
        // Round-2 work is a one-line addition.
        let Some(session) = self.inner.take() else {
            return;
        };
        let Some(handle) = runtime::try_get_handle() else {
            // No async entry point has ever run, so there cannot be a
            // live `Session` either — but the destructor of `Session`
            // itself uses the kernel's own borrowed handle, so we
            // simply let it run.
            drop(session);
            return;
        };
        // The kernel's `SessionInner::Drop` already spawns a
        // fire-and-forget `delete_session` on its own captured runtime
        // handle. To stay on napi-rs's runtime explicitly (so Round 2
        // can add binding-side cleanup steps before the kernel drop),
        // hop onto a tokio task and let the kernel destructor run
        // there. We do NOT call `Session::close().await` because that
        // method enters a tracing span (`EnteredSpan` is `!Send`) and
        // therefore cannot cross an `await` boundary inside a `spawn`.
        handle.spawn(async move {
            drop(session);
        });
    }
}
