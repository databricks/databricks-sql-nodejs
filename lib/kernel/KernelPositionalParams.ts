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
import { KernelNativeRawParameterInput } from './KernelNativeLoader';
import assertBindableValue from '../utils/assertBindableValue';

/**
 * Reduce a `DBSQLParameter | DBSQLParameterValue` to the napi
 * `RawParameterInput` (`{ name?, sqlType, value? }`) accepted by the kernel's
 * raw-parameter path. Reuses `DBSQLParameter.toSparkParameter` — the same
 * type-inference + value-stringification (including DECIMAL → `DECIMAL(p,s)`)
 * the Thrift backend uses; a missing value ⇒ SQL NULL.
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

  return { sqlType: spark.type ?? 'STRING', value: stringValue };
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
