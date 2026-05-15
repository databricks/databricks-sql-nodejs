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

//! Opaque `Connection` wrapper.
//!
//! Round 1b: scaffold only. The kernel collapses ADBC's per-connection
//! state into the `Session` handle held by `Database` (see
//! `database.rs`). The JS-side `Connection` exists for API parity with
//! the existing Node driver but is currently a thin marker; Round 2
//! decides whether to keep it as a pass-through on `Database` or to
//! attach per-connection scoping (e.g. default catalog/schema overrides).

/// JS-visible connection options. Empty in Round 1b; Round 2 may add
/// per-connection scope fields (catalog, schema, session config map).
#[napi(object)]
pub struct ConnectionOptions {}

/// Opaque connection handle. Round 1b: marker only; no kernel state.
#[napi]
pub struct Connection {}

#[napi]
impl Connection {
    /// Construct a new connection handle. Round 1b is a no-op shell;
    /// Round 2 will wire it to `Database`'s `Session` (likely via an
    /// async `Database::connect()` factory rather than a JS-side
    /// `new Connection()`).
    #[napi(constructor)]
    pub fn new(_options: ConnectionOptions) -> Self {
        Connection {}
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Round 1b: nothing to clean up. Round 2 will populate this
        // with the same `runtime::get_handle().spawn(...)` pattern as
        // `Database::drop`.
    }
}
