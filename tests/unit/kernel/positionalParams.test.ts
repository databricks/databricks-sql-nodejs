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

import { expect } from 'chai';
import { buildKernelPositionalParams, buildKernelNamedParams } from '../../../lib/kernel/KernelPositionalParams';
import { DBSQLParameter, DBSQLParameterType } from '../../../lib/DBSQLParameter';
import ParameterError from '../../../lib/errors/ParameterError';

describe('KernelPositionalParams.buildKernelPositionalParams', () => {
  it('returns undefined for no params (keeps the no-options fast path)', () => {
    expect(buildKernelPositionalParams(undefined)).to.equal(undefined);
    expect(buildKernelPositionalParams([])).to.equal(undefined);
  });

  it('infers types from raw values, matching DBSQLParameter rules', () => {
    expect(buildKernelPositionalParams([42, 'hello', true])).to.deep.equal([
      { sqlType: 'INTEGER', value: '42' },
      { sqlType: 'STRING', value: 'hello' },
      { sqlType: 'BOOLEAN', value: 'TRUE' },
    ]);
  });

  const decimal = (value: string) => () =>
    buildKernelPositionalParams([new DBSQLParameter({ type: DBSQLParameterType.DECIMAL, value })]);

  it('emits DECIMAL in the parenthesised DECIMAL(p,s) form the kernel codec requires', () => {
    expect(decimal('99.99')()).to.deep.equal([{ sqlType: 'DECIMAL(4,2)', value: '99.99' }]);
    expect(decimal('-123')()).to.deep.equal([{ sqlType: 'DECIMAL(3,0)', value: '-123' }]);
  });

  it('excludes insignificant leading zeros from precision (Spark decimal-literal rule)', () => {
    // 0.00001 → DECIMAL(5,5), not DECIMAL(6,5) (the integer "0" is not significant).
    expect(decimal('0.00001')()).to.deep.equal([{ sqlType: 'DECIMAL(5,5)', value: '0.00001' }]);
    // "007.50" → significant int "7" (1) + scale 2 ⇒ DECIMAL(3,2).
    expect(decimal('007.50')()).to.deep.equal([{ sqlType: 'DECIMAL(3,2)', value: '007.50' }]);
    // "0" ⇒ DECIMAL(1,0) (minimum precision 1).
    expect(decimal('0')()).to.deep.equal([{ sqlType: 'DECIMAL(1,0)', value: '0' }]);
  });

  it('rejects a DECIMAL that needs precision > 38 instead of clamping-and-sending', () => {
    // 40 integer digits can't fit DECIMAL(38,…); clamping the type while sending
    // the full value is internally inconsistent — reject at bind time instead.
    expect(decimal('1234567890123456789012345678901234567890')).to.throw(
      ParameterError,
      /exceeding the Databricks maximum of 38/,
    );
  });

  it('rejects a non-numeric / exponential DECIMAL value', () => {
    expect(decimal('1e+21')).to.throw(ParameterError, /not a plain decimal numeral/);
    expect(decimal('abc')).to.throw(ParameterError, /not a plain decimal numeral/);
    expect(decimal('')).to.throw(ParameterError, /not a plain decimal numeral/);
  });

  it('collapses every INTERVAL subtype to the kernel codec\'s single "INTERVAL" type name', () => {
    expect(
      buildKernelPositionalParams([
        new DBSQLParameter({ type: DBSQLParameterType.INTERVALMONTH, value: '13' }),
        new DBSQLParameter({ type: DBSQLParameterType.INTERVALDAY, value: '1 02:03:04' }),
      ]),
    ).to.deep.equal([
      { sqlType: 'INTERVAL', value: '13' },
      { sqlType: 'INTERVAL', value: '1 02:03:04' },
    ]);
  });

  it('maps NULL to a value-less VOID input', () => {
    expect(buildKernelPositionalParams([null])).to.deep.equal([{ sqlType: 'VOID' }]);
  });

  it('honours explicit DATE / TIMESTAMP types', () => {
    expect(
      buildKernelPositionalParams([
        new DBSQLParameter({ type: DBSQLParameterType.DATE, value: '2024-01-15' }),
        new DBSQLParameter({ type: DBSQLParameterType.TIMESTAMP, value: '2024-01-15 10:30:00' }),
      ]),
    ).to.deep.equal([
      { sqlType: 'DATE', value: '2024-01-15' },
      { sqlType: 'TIMESTAMP', value: '2024-01-15 10:30:00' },
    ]);
  });

  it('binds TIMESTAMP_NTZ natively and TIMESTAMP_LTZ as TIMESTAMP (Spark has no distinct LTZ type)', () => {
    expect(
      buildKernelPositionalParams([
        new DBSQLParameter({ type: DBSQLParameterType.TIMESTAMP_NTZ, value: '2024-01-15 10:30:00' }),
        new DBSQLParameter({ type: DBSQLParameterType.TIMESTAMP_LTZ, value: '2024-01-15 10:30:00' }),
      ]),
    ).to.deep.equal([
      { sqlType: 'TIMESTAMP_NTZ', value: '2024-01-15 10:30:00' },
      { sqlType: 'TIMESTAMP', value: '2024-01-15 10:30:00' },
    ]);
  });
});

describe('KernelPositionalParams.buildKernelNamedParams', () => {
  it('returns undefined for no named params', () => {
    expect(buildKernelNamedParams(undefined)).to.equal(undefined);
    expect(buildKernelNamedParams({})).to.equal(undefined);
  });

  it('emits {name, sqlType, value} triples, reusing the same type mapping', () => {
    expect(
      buildKernelNamedParams({
        n: 42,
        s: 'hello',
        d: new DBSQLParameter({ type: DBSQLParameterType.DECIMAL, value: '99.99' }),
      }),
    ).to.deep.include.members([
      { name: 'n', sqlType: 'INTEGER', value: '42' },
      { name: 's', sqlType: 'STRING', value: 'hello' },
      { name: 'd', sqlType: 'DECIMAL(4,2)', value: '99.99' },
    ]);
  });

  it('maps a named NULL to a value-less VOID input (with the name)', () => {
    expect(buildKernelNamedParams({ x: null })).to.deep.equal([{ name: 'x', sqlType: 'VOID' }]);
  });
});
