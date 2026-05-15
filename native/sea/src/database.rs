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

//! `openSession()` — the binding's session-construction entry point.
//!
//! The kernel collapses ADBC's `Database` + `Connection` into a single
//! `Session`. The TS adapter layer reconstructs a `DBSQLClient` /
//! `Database` wrapper on top of this binding, so the napi surface itself
//! stays flat: one free function, one opaque `Connection` class.
//!
//! Rationale for a free function over a static class method:
//! - napi-rs v2's static-method codegen for async functions returning a
//!   `#[napi]` struct is fragile — the runtime registration sometimes
//!   omits the method from the class object. Free `#[napi]` functions
//!   go through a different, more stable codegen path.
//! - There is no kernel-side `Database` state to wrap; everything
//!   meaningful lives on `Session`. A wrapper class with no fields adds
//!   a JS object allocation per session for no benefit.

use std::sync::Arc;
use tokio::sync::Mutex;

use databricks_sql_kernel::{AuthConfig, Session};

use crate::connection::Connection;
use crate::error::napi_err_from_kernel;
use crate::runtime;
use crate::util::guarded;

/// JS-visible options for opening a Databricks SQL session over PAT.
///
/// M0 supports PAT only — `token` is required. OAuth M2M / U2M variants
/// land in M1 along with a discriminated-union shape on the JS side.
#[napi(object)]
pub struct ConnectionOptions {
    /// Workspace host, e.g. `adb-…azuredatabricks.net`. The kernel
    /// normalises this — bare hostnames get `https://` prepended.
    pub host_name: String,
    /// JDBC-style HTTP path, e.g. `/sql/1.0/warehouses/abc123`. The
    /// kernel parses out the warehouse id.
    pub http_path: String,
    /// Personal access token. Must be non-empty (the kernel rejects
    /// empty PATs at session construction).
    pub token: String,
}

/// Open a Databricks SQL session over PAT auth and return an opaque
/// `Connection` wrapping the kernel `Session`.
///
/// The JS-visible name is `openSession` (napi-rs converts snake_case
/// to camelCase for free functions).
#[napi]
pub async fn open_session(options: ConnectionOptions) -> napi::Result<Connection> {
    guarded(async move {
        // Cache the napi-rs tokio Handle on the very first async call
        // so Drop impls (which run on the V8 GC thread, outside any
        // tokio context) can still `spawn` cleanup tasks onto the
        // runtime that's driving this future.
        let _ = runtime::get_handle();

        // SessionConfig is `#[non_exhaustive]` — go through the
        // builder, which is the only public path that constructs it.
        // `http_path()` is the convenience setter that maps a bare
        // hostname + `/sql/1.0/warehouses/{id}` path into the kernel's
        // `ConnectionConfig`.
        let session = Session::builder()
            .http_path(options.host_name, options.http_path)
            .auth(AuthConfig::Pat {
                token: options.token,
            })
            .open()
            .await
            .map_err(napi_err_from_kernel)?;
        Ok(Connection {
            inner: Arc::new(Mutex::new(Some(session))),
        })
    })
    .await
}
