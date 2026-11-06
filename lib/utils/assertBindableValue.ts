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
import { DBSQLParameter, DBSQLParameterType, DBSQLParameterValue } from '../DBSQLParameter';
import ParameterError from '../errors/ParameterError';

/**
 * Backend-neutral bind-time gate shared by the Thrift and kernel backends.
 *
 * Rejects a parameter value that cannot be bound as a scalar. Arrays and plain
 * objects stringify to garbage (e.g. `[1,2,3]` → `"1,2,3"`) that the server
 * fails to coerce — on the Thrift path the operation never returns to
 * FINISHED (a DoS hazard), and on the kernel path it surfaces an opaque server
 * error. We fail fast at bind time instead, with the same error on both
 * backends. `DBSQLParameter`, `Int64`, `Date`, and JS primitives are allowed.
 *
 * Parameter-marker counting and arity validation are intentionally NOT done
 * here: that is owned by the server (Thrift) or the kernel's
 * `statement::params` codec, so duplicating the SQL-walk JS-side would only
 * risk drift. This is purely a cheap type-shape check.
 */
export default function assertBindableValue(value: DBSQLParameter | DBSQLParameterValue, label: string): void {
  if (value instanceof DBSQLParameter) {
    // An explicit VOID type ignores the wrapped value (binds NULL), so there is
    // nothing to validate. Otherwise validate the unwrapped value: a
    // non-scalar wrapped in `DBSQLParameter` hits the same STRING fallback.
    if (value.type === DBSQLParameterType.VOID) return;
    // A nested `DBSQLParameter` is not a valid wrapped value: the outer
    // parameter would fall through to the STRING default with an object value.
    if ((value.value as unknown) instanceof DBSQLParameter) {
      throw new ParameterError(`${label} wraps a nested DBSQLParameter; the wrapped value must be a scalar`);
    }
    assertBindableValue(value.value, label);
    return;
  }
  if (value === null || value === undefined) return;
  if (Array.isArray(value)) {
    throw new ParameterError(
      `${label} is an array; compound types (ARRAY/MAP/STRUCT) are not bindable as a parameter value`,
    );
  }
  if (typeof value === 'object') {
    if (value instanceof Date || value instanceof Int64) return;
    throw new ParameterError(
      `${label} is an object; only scalar values (string/number/bigint/boolean), Date, and Int64 are bindable`,
    );
  }
  // Allow-list the remaining primitives; rejects e.g. functions and symbols.
  const kind = typeof value;
  if (kind !== 'string' && kind !== 'number' && kind !== 'bigint' && kind !== 'boolean') {
    throw new ParameterError(
      `${label} is a ${kind}; only scalar values (string/number/bigint/boolean), Date, and Int64 are bindable`,
    );
  }
}
