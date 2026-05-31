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

import { DBSQLParameter, DBSQLParameterValue } from '../DBSQLParameter';
import ParameterError from '../errors/ParameterError';
import { SeaNativeTypedValueInput } from './SeaNativeLoader';

/**
 * Derive `(precision,scale)` from a decimal value string for the SEA
 * `DECIMAL(p,s)` type name — the kernel param codec requires the
 * parenthesised form (plain `"DECIMAL"` is rejected) so it can preserve
 * the caller's fractional digits. `"99.99"` ⇒ `"4,2"`; `"-123"` ⇒ `"3,0"`.
 * Clamped to the Databricks max precision of 38.
 */
function decimalPrecisionScale(v: string): string {
  const digits = (v.match(/\d/g) ?? []).length;
  const dot = v.indexOf('.');
  const scale = dot < 0 ? 0 : (v.slice(dot + 1).match(/\d/g) ?? []).length;
  const precision = Math.min(Math.max(digits, 1), 38);
  return `${precision},${Math.min(scale, precision)}`;
}

/**
 * Reduce a `DBSQLParameter | DBSQLParameterValue` to the napi
 * `TypedValueInput` (`{ sqlType, value? }`) the kernel's positional-param
 * codec (`parse_typed_value`) accepts. Reuses `DBSQLParameter.toSparkParameter`
 * — the same type-inference + value-stringification the Thrift backend uses —
 * then adapts the type name to the codec's expectations:
 * - DECIMAL → `DECIMAL(p,s)` (parenthesised form required)
 * - INTERVAL * → `INTERVAL` (the codec's single interval type name)
 * - a missing value ⇒ SQL NULL (`parse_typed_value` maps `value: None` to NULL).
 */
function toTypedValueInput(value: DBSQLParameter | DBSQLParameterValue): SeaNativeTypedValueInput {
  const param = value instanceof DBSQLParameter ? value : new DBSQLParameter({ value });
  const spark = param.toSparkParameter();
  const stringValue = spark.value?.stringValue ?? undefined;

  // NULL: no value (and `VOID` ignores any type), matching toSparkParameter's
  // type/value-less shape for null/undefined.
  if (stringValue === undefined || stringValue === null) {
    return { sqlType: 'VOID' };
  }

  let sqlType = spark.type ?? 'STRING';
  const upper = sqlType.toUpperCase();
  if (upper === 'DECIMAL') {
    sqlType = `DECIMAL(${decimalPrecisionScale(stringValue)})`;
  } else if (upper.startsWith('INTERVAL')) {
    sqlType = 'INTERVAL';
  }
  return { sqlType, value: stringValue };
}

/**
 * Convert the public `ordinalParameters` option into the napi
 * `positionalParams` array. Returns `undefined` when no positional params
 * were supplied (so the caller can keep the minimal no-options call shape).
 *
 * Named parameters are not yet bindable on the SEA path — the kernel napi
 * surface (`ExecuteOptions.positionalParams`) exposes positional only — so a
 * caller passing `namedParameters` is rejected with a clear `ParameterError`
 * rather than silently ignored.
 */
export function buildSeaPositionalParams(
  ordinalParameters?: Array<DBSQLParameter | DBSQLParameterValue>,
): Array<SeaNativeTypedValueInput> | undefined {
  if (ordinalParameters === undefined || ordinalParameters.length === 0) {
    return undefined;
  }
  return ordinalParameters.map(toTypedValueInput);
}
