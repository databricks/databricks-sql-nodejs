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

//! Opaque `Connection` wrapper around the kernel's `Session`.
//!
//! The kernel collapses ADBC's `Database` + `Connection` into a single
//! `Session`. We keep the wrapper name `Connection` on the JS side because
//! that matches the existing Node driver's mental model.
//!
//! M0 surface (Round 2):
//! - `Connection.executeStatement(sql, options)` — builds a kernel
//!   `Statement`, sets the spec, awaits `execute()`, wraps the result
//!   in a JS-visible `Statement` opaque handle.
//! - `Connection.close()` — explicit async close. Drop schedules a
//!   fire-and-forget close on the captured runtime handle if explicit
//!   close was never called.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use databricks_sql_kernel::Session;

use crate::error::napi_err_from_kernel;
use crate::runtime;
use crate::statement::Statement;
use crate::util::guarded;

/// JS-visible per-execute options. M0 only carries
/// initialCatalog / initialSchema / sessionConfig — parameters and
/// per-statement overrides land in M1.
#[napi(object)]
pub struct ExecuteOptions {
    /// Default catalog applied to this statement via session conf.
    pub initial_catalog: Option<String>,
    /// Default schema applied to this statement via session conf.
    pub initial_schema: Option<String>,
    /// Per-statement session conf overrides (forwarded to SEA
    /// `parameters` / Thrift `confOverlay`).
    pub session_config: Option<HashMap<String, String>>,
}

/// Opaque connection handle wrapping a kernel `Session`.
///
/// `inner` is `Arc<Mutex<Option<Session>>>` so:
/// - the Drop impl can clone the `Arc` and `.take()` the session on a
///   background tokio task without holding `&mut self` (which Drop is
///   forbidden from doing across an `await`),
/// - `executeStatement` can share immutable access to the session via
///   the `Arc<SessionInner>` clones the kernel makes internally
///   (`Session::statement()` only needs `&self`).
#[napi]
pub struct Connection {
    pub(crate) inner: Arc<Mutex<Option<Session>>>,
}

#[napi]
impl Connection {
    /// Execute a SQL statement and return a Statement handle that
    /// streams batches via `fetchNextBatch()`.
    #[napi]
    pub async fn execute_statement(
        &self,
        sql: String,
        options: ExecuteOptions,
    ) -> napi::Result<Statement> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let session = guard.as_ref().ok_or_else(|| {
                napi::Error::new(napi::Status::InvalidArg, "connection already closed")
            })?;

            // Build a per-statement spec on the kernel's mutable
            // Statement. Session conf overrides surface through the
            // statement_conf overlay; M0 has no parameter binding.
            let mut stmt = session.statement();
            stmt.spec().sql(sql);

            let mut overlay: HashMap<String, String> =
                options.session_config.unwrap_or_default();
            if let Some(catalog) = options.initial_catalog {
                overlay.insert("default_catalog".to_string(), catalog);
            }
            if let Some(schema) = options.initial_schema {
                overlay.insert("default_schema".to_string(), schema);
            }
            if !overlay.is_empty() {
                stmt.spec().statement_conf(overlay);
            }

            let executed = stmt.execute().await.map_err(napi_err_from_kernel)?;
            // Pass the parent kernel `Statement` into the JS wrapper.
            // Dropping it here would invalidate the executed handle
            // via the shared ValidityFlag — see `StatementInner` docs
            // in `statement.rs` for the rationale.
            Ok(Statement::from_executed(stmt, executed))
        })
        .await
    }

    /// Explicit close. Marks the connection wrapper as closed so
    /// subsequent calls on this `Connection` return `InvalidArg`, then
    /// schedules a fire-and-forget server-side close on the runtime.
    ///
    /// **Why fire-and-forget and not `Session::close().await`:** the
    /// kernel's `Session::close(self).await` body holds a
    /// `tracing::EnteredSpan` (a `!Send` type) across an `.await`, so
    /// the future is not `Send`. napi-rs's `execute_tokio_future` glue
    /// rejects non-`Send` futures, and `Handle::spawn` does too. The
    /// kernel's `SessionInner::Drop` already spawns the
    /// `delete_session` RPC on the same runtime handle the napi
    /// binding captured, so dropping the value is functionally
    /// equivalent — the difference is that JS callers can't observe a
    /// `delete_session` failure from `close()`. Tracked as a kernel-
    /// side follow-up (clone the span rather than entering it) in
    /// Round 3 findings.
    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let _taken = {
                let mut guard = inner.lock().await;
                guard.take()
            };
            // `_taken` drops here. Kernel's `SessionInner::Drop`
            // spawns `delete_session` on its captured handle.
            Ok(())
        })
        .await
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Fire-and-forget close on the captured runtime. If `close()`
        // was already called, `inner` holds `None` and the spawned
        // task is a trivial no-op.
        let Some(handle) = runtime::try_get_handle() else {
            // No async entry point ever ran — there's nothing to close.
            return;
        };
        let inner = Arc::clone(&self.inner);
        handle.spawn(async move {
            // Drop the session value on the runtime. The kernel's
            // `SessionInner::Drop` already spawns a fire-and-forget
            // `delete_session` against its own captured handle. We do
            // NOT call `Session::close().await` here because that
            // method holds a `tracing::EnteredSpan` (`!Send`) across
            // its body, which would conflict with `Handle::spawn`'s
            // `Send` bound on the future.
            let _taken = {
                let mut guard = inner.lock().await;
                guard.take()
            };
            // `_taken` drops here; kernel's SessionInner::Drop fires.
        });
    }
}
