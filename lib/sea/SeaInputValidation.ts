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
 * Coerce an empty-string metadata argument to `undefined`.
 *
 * The kernel's `Identifier` / `LikePattern` reject empty strings with
 * `InvalidArgument`, whereas the Thrift backend forwards `""` to the server
 * which treats it as "unspecified" (match-all / session default). To keep the
 * SEA metadata surface behaviourally identical to Thrift, the SEA adapter
 * maps `""` → `undefined` before crossing the napi boundary so the kernel
 * sees "argument omitted" rather than "empty identifier".
 */
export function emptyToUndefined(value: string | undefined | null): string | undefined {
  return value == null || value === '' ? undefined : value;
}

/**
 * Walk a SQL string counting `?` parameter markers, ignoring markers inside
 * string literals (`'...'`, `"..."`), backtick-quoted identifiers, and
 * comments (`-- ...`, `/* ... *​/`). Mirrors the kernel's
 * `statement::params::count_parameter_markers` state machine so the JS-side
 * arity check matches what the kernel binds.
 */
export function countParameterMarkers(sql: string): number {
  let count = 0;
  let i = 0;
  const n = sql.length;
  type State = 'normal' | 'single' | 'double' | 'backtick' | 'line' | 'block';
  let state: State = 'normal';
  while (i < n) {
    const c = sql[i];
    const next = i + 1 < n ? sql[i + 1] : '';
    switch (state) {
      case 'normal':
        if (c === '?') {
          count += 1;
        } else if (c === "'") {
          state = 'single';
        } else if (c === '"') {
          state = 'double';
        } else if (c === '`') {
          state = 'backtick';
        } else if (c === '-' && next === '-') {
          state = 'line';
          i += 1;
        } else if (c === '/' && next === '*') {
          state = 'block';
          i += 1;
        }
        break;
      case 'single':
        if (c === "'" && next === "'") i += 1; // escaped ''
        else if (c === "'") state = 'normal';
        break;
      case 'double':
        if (c === '"' && next === '"') i += 1; // escaped ""
        else if (c === '"') state = 'normal';
        break;
      case 'backtick':
        if (c === '`') state = 'normal';
        break;
      case 'line':
        if (c === '\n') state = 'normal';
        break;
      case 'block':
        if (c === '*' && next === '/') {
          state = 'normal';
          i += 1;
        }
        break;
    }
    i += 1;
  }
  return count;
}

/**
 * Reject a parameter value that cannot be bound as a scalar. Arrays and plain
 * objects stringify to garbage (e.g. `[1,2,3]` → `"1,2,3"`) that the server
 * fails to coerce — on the Thrift path the operation never returns to
 * FINISHED (a DoS hazard), and on SEA it surfaces an opaque server error. We
 * fail fast at bind time instead, mirroring the kernel's compound-type
 * rejection. `DBSQLParameter`, `Int64`, `Date`, and JS primitives are allowed.
 */
export function assertBindableValue(value: DBSQLParameter | DBSQLParameterValue, label: string): void {
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
