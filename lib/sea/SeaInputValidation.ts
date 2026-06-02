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

import Int64 from 'node-int64';
import { DBSQLParameter, DBSQLParameterValue } from '../DBSQLParameter';
import ParameterError from '../errors/ParameterError';

/**
 * Reject a parameter value that cannot be bound as a scalar. Arrays and plain
 * objects stringify to garbage (e.g. `[1,2,3]` → `"1,2,3"`) that the server
 * fails to coerce — on the Thrift path the operation never returns to
 * FINISHED (a DoS hazard), and on SEA it surfaces an opaque server error. We
 * fail fast at bind time instead, mirroring the kernel's compound-type
 * rejection. `DBSQLParameter`, `Int64`, `Date`, and JS primitives are allowed.
 *
 * Parameter-marker counting and arity validation are intentionally NOT done
 * here: the kernel's `statement::params` codec owns that check and binds
 * exactly one placeholder style per statement, so duplicating the SQL-walk
 * JS-side would only risk drift. The driver's sole bind-time job is this
 * cheap, type-shape gate before the value crosses the napi boundary.
 */
export default function assertBindableValue(value: DBSQLParameter | DBSQLParameterValue, label: string): void {
  if (value instanceof DBSQLParameter) return;
  if (value === null || value === undefined) return;
  if (Array.isArray(value)) {
    throw new ParameterError(
      `${label} is an array; compound types (ARRAY/MAP/STRUCT) are not bindable as a parameter value`,
    );
  }
  if (typeof value === 'object' && !(value instanceof Date) && !(value instanceof Int64)) {
    throw new ParameterError(
      `${label} is an object; only scalar values (string/number/bigint/boolean), Date, and Int64 are bindable`,
    );
  }
}
