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

import { TGetInfoType, TGetInfoValue } from '../../thrift/TCLIService_types';

/**
 * `getInfo` (JDBC `DatabaseMetaData` / ODBC `SQLGetInfo`) is a Thrift-protocol
 * concept: the Thrift backend forwards `TGetInfoReq` to the server's `getInfo`
 * RPC. The SEA REST protocol and the Rust kernel have **no** equivalent
 * endpoint, so — exactly as JDBC does for `DatabaseMetaData` — we synthesize
 * the values client-side.
 *
 * The Databricks Thrift server itself answers only three `TGetInfoType`s and
 * rejects every other value; we mirror that surface byte-for-byte so the SEA
 * path is a drop-in equivalent:
 *
 *   | TGetInfoType        | Thrift server | SEA (here)        |
 *   |---------------------|---------------|-------------------|
 *   | CLI_SERVER_NAME (13)| "Spark SQL"   | "Spark SQL"        |
 *   | CLI_DBMS_NAME   (17)| "Spark SQL"   | "Spark SQL"        |
 *   | CLI_DBMS_VER    (18)| "3.1.1"       | "3.1.1"            |
 *   | (any other)         | error         | undefined → error  |
 */

/** Canonical DBMS product name — identical to the Thrift server's value. */
export const SEA_DBMS_NAME = 'Spark SQL';

/** Server-name answer — identical to the Thrift server's value. */
export const SEA_SERVER_NAME = 'Spark SQL';

/**
 * DBMS version string. Mirrors the constant the Databricks Thrift server
 * reports for `CLI_DBMS_VER` (the HiveServer2-compat Spark SQL version, not
 * the DBR release). Kept in lock-step with Thrift for parity; if the server
 * ever changes it the comparator's GET_INFO suite flags the drift.
 */
export const SEA_DBMS_VERSION = '3.1.1';

/**
 * Synthesize the `TGetInfoValue` for a `getInfo` request on the SEA path.
 * Returns `undefined` for any `TGetInfoType` the (Thrift) server does not
 * answer — the caller surfaces that as an error, matching Thrift's
 * reject-unsupported-info-type behaviour.
 */
export function seaServerInfoValue(infoType: number): TGetInfoValue | undefined {
  switch (infoType) {
    case TGetInfoType.CLI_SERVER_NAME:
      return new TGetInfoValue({ stringValue: SEA_SERVER_NAME });
    case TGetInfoType.CLI_DBMS_NAME:
      return new TGetInfoValue({ stringValue: SEA_DBMS_NAME });
    case TGetInfoType.CLI_DBMS_VER:
      return new TGetInfoValue({ stringValue: SEA_DBMS_VERSION });
    default:
      return undefined;
  }
}
