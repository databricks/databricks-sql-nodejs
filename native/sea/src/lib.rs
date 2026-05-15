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

//! `databricks-sea-native` — napi-rs binding crate for the Databricks
//! SQL Node.js driver's SEA (Statement Execution API) path.
//!
//! Round 2 surface: `Database.open` → `Connection.execute_statement`
//! → `Statement.fetch_next_batch` / `schema` / `cancel` / `close`.
//! Results cross the FFI as Arrow IPC bytes (see `result.rs`); the
//! TS adapter decodes them via `apache-arrow`.

#![deny(unsafe_op_in_unsafe_fn)]

#[macro_use]
extern crate napi_derive;

pub(crate) mod connection;
pub(crate) mod database;
pub(crate) mod error;
pub(crate) mod logger;
pub(crate) mod result;
pub(crate) mod runtime;
pub(crate) mod statement;
pub(crate) mod util;

/// Returns the native binding's crate version (`CARGO_PKG_VERSION`).
///
/// Originally the round-1b smoke test; kept as a cheap "is the binding
/// loaded?" probe for the JS-side loader's structured diagnostics.
#[napi]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
