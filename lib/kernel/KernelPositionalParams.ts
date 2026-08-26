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
import { KernelNativeRawParameterInput } from './KernelNativeLoader';
import assertBindableValue from './KernelInputValidation';

/**
 * Derive `(precision,scale)` from a decimal value string for the kernel
 * `DECIMAL(p,s)` type name — the kernel param codec requires the parenthesised
 * form (plain `"DECIMAL"` is rejected) so it preserves the caller's fractional
 * digits. `"99.99"` ⇒ `"4,2"`; `"-123"` ⇒ `"3,0"`; `"0.00001"` ⇒ `"5,5"`.
 *
 * Precision is significant integer digits (insignificant leading zeros
 * stripped, matching Spark's decimal-literal rule) plus the fractional scale.
 * The value is NOT clamped: a non-numeric value, or one that needs precision >
 * the Databricks maximum of 38, throws `ParameterError` at bind time rather
 * than emitting an inconsistent `DECIMAL(38,…)` type alongside an
 * unrepresentable value (which the kernel would reject or silently truncate).
 */
function decimalPrecisionScale(v: string): string {
  // Accept an optional sign + plain decimal numeral only. Exponential form
  // ("1e+21"), empty, and non-numeric ("abc") are rejected with a clear error
  // instead of mis-deriving (p,s) and surfacing an opaque kernel failure.
  const m = /^[+-]?(\d*)(?:\.(\d+))?$/.exec(v.trim());
  const intPart = m?.[1] ?? '';
  const fracPart = m?.[2] ?? '';
  if (!m || (intPart === '' && fracPart === '')) {
    throw new ParameterError(
      `DECIMAL parameter value "${v}" is not a plain decimal numeral (expected [+-]?digits[.digits]).`,
    );
  }
  const scale = fracPart.length;
  // Significant integer digits: drop insignificant leading zeros ("007" → 1,
  // "0" / "" → 0) so e.g. 0.00001 is DECIMAL(5,5), not DECIMAL(6,5).
  const significantIntDigits = intPart.replace(/^0+/, '').length;
  const precision = Math.max(significantIntDigits + scale, 1);
  if (precision > 38) {
    throw new ParameterError(
      `DECIMAL parameter value "${v}" needs precision ${precision}, exceeding the Databricks maximum of 38. ` +
        'Round/scale the value or bind it as a STRING.',
    );
  }
  return `${precision},${scale}`;
}

/**
 * Reduce a `DBSQLParameter | DBSQLParameterValue` to the napi
 * `RawParameterInput` (`{ name?, sqlType, value? }`) accepted by the kernel's
 * raw-parameter path. Reuses `DBSQLParameter.toSparkParameter` — the same
 * type-inference + value-stringification the Thrift backend uses — then adapts
 * the type name where required:
 * - DECIMAL → `DECIMAL(p,s)` (parenthesised form required)
 * - a missing value ⇒ SQL NULL.
 */
function toRawParameterInput(value: DBSQLParameter | DBSQLParameterValue): KernelNativeRawParameterInput {
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
  }
  return { sqlType, value: stringValue };
}

/**
 * Convert the public `ordinalParameters` option into the napi
 * `rawParams` array (1-based `?` placeholders). Returns `undefined`
 * when none were supplied, so the caller can keep the minimal no-options
 * call shape.
 */
export function buildKernelPositionalParams(
  ordinalParameters?: Array<DBSQLParameter | DBSQLParameterValue>,
): Array<KernelNativeRawParameterInput> | undefined {
  if (ordinalParameters === undefined || ordinalParameters.length === 0) {
    return undefined;
  }
  return ordinalParameters.map((value, i) => {
    assertBindableValue(value, `ordinalParameters[${i}]`);
    return toRawParameterInput(value);
  });
}

/**
 * Convert the public `namedParameters` option (`Record<name, value>`) into
 * the napi `rawParams` array (`:name` placeholders). Each value reuses the
 * same `toRawParameterInput` mapping (DECIMAL → DECIMAL(p,s), NULL → VOID, …),
 * then carries its name. Returns `undefined` when none were supplied.
 */
export function buildKernelNamedParams(
  namedParameters?: Record<string, DBSQLParameter | DBSQLParameterValue>,
): Array<KernelNativeRawParameterInput> | undefined {
  if (namedParameters === undefined || Object.keys(namedParameters).length === 0) {
    return undefined;
  }
  return Object.keys(namedParameters).map((name) => {
    assertBindableValue(namedParameters[name], `namedParameters[${name}]`);
    return { name, ...toRawParameterInput(namedParameters[name]) };
  });
}
