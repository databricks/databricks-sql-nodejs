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

//! Opaque `Statement` wrapper around the kernel's `ExecutedStatement`.
//!
//! M0 surface (Round 2):
//! - `Statement.fetchNextBatch() -> Option<ArrowBatch>` — drives
//!   `ResultStream::next_batch().await`, serialises the borrowed
//!   `RecordBatch` to Arrow IPC bytes, returns them to JS.
//! - `Statement.schema() -> ArrowSchema` — returns the cached schema
//!   from the kernel side, serialised as a schema-only IPC payload.
//! - `Statement.cancel()` / `Statement.close()` — forwards to
//!   `ExecutedStatement::cancel/close` via the
//!   `ExecutedStatementHandle` trait. Drop fires-and-forgets close
//!   if not already explicitly closed.

use std::sync::Arc;
use tokio::sync::Mutex;

use arrow_ipc::writer::StreamWriter;
use databricks_sql_kernel::{ExecutedStatement, ExecutedStatementHandle, ResultBatch};

use crate::error::napi_err_from_kernel;
use crate::result::{ArrowBatch, ArrowSchema};
use crate::runtime;
use crate::util::guarded;

/// Opaque executed-statement handle.
///
/// `inner` is wrapped in `Arc<Mutex<Option<…>>>` so:
/// - `fetch_next_batch` can `await` `ResultStream::next_batch` which
///   requires `&mut ExecutedStatement` (via `result_stream_mut`),
/// - `cancel` / `close` (which take `&self` on the kernel side via the
///   `ExecutedStatementHandle` trait) can run concurrently with each
///   other from a JS perspective without panicking,
/// - `Drop` can hand the inner handle off to a tokio task without
///   touching `&mut self` across an `await`.
#[napi]
pub struct Statement {
    inner: Arc<Mutex<Option<ExecutedStatement>>>,
}

impl Statement {
    /// Crate-internal constructor — called from
    /// `Connection::execute_statement` once the kernel hands back the
    /// `ExecutedStatement`.
    pub(crate) fn from_executed(executed: ExecutedStatement) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(executed))),
        }
    }
}

#[napi]
impl Statement {
    /// Pull the next batch of results. Returns `None` when the stream
    /// is exhausted. The returned `ArrowBatch.ipcBytes` is a complete
    /// Arrow IPC stream (schema header + 1 record-batch message)
    /// suitable for handing to `apache-arrow`'s `RecordBatchReader`.
    #[napi]
    pub async fn fetch_next_batch(&self) -> napi::Result<Option<ArrowBatch>> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let mut guard = inner.lock().await;
            let executed = guard.as_mut().ok_or_else(|| {
                napi::Error::new(napi::Status::InvalidArg, "statement already closed")
            })?;

            let stream = executed.result_stream_mut();
            // Capture the schema before borrowing the next batch — we
            // include the schema header in every IPC payload so the
            // JS-side consumer can decode each batch independently
            // without carrying state across calls.
            let schema = stream.schema();
            let maybe_batch = stream.next_batch().await.map_err(napi_err_from_kernel)?;
            let Some(batch) = maybe_batch else {
                return Ok(None);
            };
            // `ResultBatch` is `#[non_exhaustive]`; v0 only ever
            // yields `Arrow`. The error arm exists for forward
            // compat — v1+ may add ColumnarThrift / JsonRows / etc.,
            // and we want the binding to surface that as a typed
            // error rather than silently misbehaving.
            let record_batch = match batch {
                ResultBatch::Arrow(rb) => rb,
                _ => {
                    return Err(napi::Error::new(
                        napi::Status::GenericFailure,
                        "non-Arrow ResultBatch variant — binding needs upgrade",
                    ));
                }
            };
            let bytes = encode_ipc_stream(&schema, Some(record_batch))?;
            Ok(Some(ArrowBatch {
                ipc_bytes: bytes.into(),
            }))
        })
        .await
    }

    /// Result schema as an Arrow IPC payload (schema header only, no
    /// record-batch message). Available before any batches have been
    /// fetched.
    #[napi]
    pub async fn schema(&self) -> napi::Result<ArrowSchema> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let executed = guard.as_ref().ok_or_else(|| {
                napi::Error::new(napi::Status::InvalidArg, "statement already closed")
            })?;
            let schema = executed.schema();
            let bytes = encode_ipc_stream(&schema, None)?;
            Ok(ArrowSchema {
                ipc_bytes: bytes.into(),
            })
        })
        .await
    }

    /// Server-side cancel. No-op if already finished.
    #[napi]
    pub async fn cancel(&self) -> napi::Result<()> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let executed = guard.as_ref().ok_or_else(|| {
                napi::Error::new(napi::Status::InvalidArg, "statement already closed")
            })?;
            executed.cancel().await.map_err(napi_err_from_kernel)
        })
        .await
    }

    /// Explicit close. Awaits the server-side close so the JS caller
    /// can observe failures.
    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            // Take the handle out so `Drop` knows there's nothing left
            // to clean up.
            let executed = {
                let mut guard = inner.lock().await;
                guard.take()
            };
            if let Some(executed) = executed {
                executed.close().await.map_err(napi_err_from_kernel)?;
            }
            Ok(())
        })
        .await
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        let Some(handle) = runtime::try_get_handle() else {
            return;
        };
        let inner = Arc::clone(&self.inner);
        handle.spawn(async move {
            // Drop the executed statement on the runtime. The kernel's
            // `ExecutedStatement::Drop` already spawns a fire-and-forget
            // `close_statement` against its own captured handle, so we
            // just need to ensure the value is dropped inside a tokio
            // context (the kernel's Drop reads `runtime_handle.clone()`
            // and spawns; that handle is the same one we captured here).
            let _taken = {
                let mut guard = inner.lock().await;
                guard.take()
            };
        });
    }
}

/// Encode an Arrow schema (and optional one record batch) as an IPC
/// stream payload. Used for both `schema()` (schema only) and
/// `fetchNextBatch()` (schema + one batch). Returning a self-contained
/// IPC stream per call is wasteful header-wise but lets the JS adapter
/// stay stateless — it decodes each `ipcBytes` independently via the
/// same `apache-arrow` `RecordBatchReader` path.
fn encode_ipc_stream(
    schema: &arrow_schema::SchemaRef,
    batch: Option<&arrow_array::RecordBatch>,
) -> napi::Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))?;
        if let Some(rb) = batch {
            writer
                .write(rb)
                .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))?;
        }
        writer
            .finish()
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))?;
    }
    Ok(buf)
}
