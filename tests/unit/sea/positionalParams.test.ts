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
import { buildSeaPositionalParams, buildSeaNamedParams } from '../../../lib/sea/SeaPositionalParams';
import { DBSQLParameter, DBSQLParameterType } from '../../../lib/DBSQLParameter';

describe('SeaPositionalParams.buildSeaPositionalParams', () => {
  it('returns undefined for no params (keeps the no-options fast path)', () => {
    expect(buildSeaPositionalParams(undefined)).to.equal(undefined);
    expect(buildSeaPositionalParams([])).to.equal(undefined);
  });

  it('infers types from raw values, matching DBSQLParameter rules', () => {
    expect(buildSeaPositionalParams([42, 'hello', true])).to.deep.equal([
      { sqlType: 'INTEGER', value: '42' },
      { sqlType: 'STRING', value: 'hello' },
      { sqlType: 'BOOLEAN', value: 'TRUE' },
    ]);
  });

  it('emits DECIMAL in the parenthesised DECIMAL(p,s) form the kernel codec requires', () => {
    expect(
      buildSeaPositionalParams([new DBSQLParameter({ type: DBSQLParameterType.DECIMAL, value: '99.99' })]),
    ).to.deep.equal([{ sqlType: 'DECIMAL(4,2)', value: '99.99' }]);
    expect(
      buildSeaPositionalParams([new DBSQLParameter({ type: DBSQLParameterType.DECIMAL, value: '-123' })]),
    ).to.deep.equal([{ sqlType: 'DECIMAL(3,0)', value: '-123' }]);
  });

  it('maps NULL to a value-less VOID input', () => {
    expect(buildSeaPositionalParams([null])).to.deep.equal([{ sqlType: 'VOID' }]);
  });

  it('honours explicit DATE / TIMESTAMP types', () => {
    expect(
      buildSeaPositionalParams([
        new DBSQLParameter({ type: DBSQLParameterType.DATE, value: '2024-01-15' }),
        new DBSQLParameter({ type: DBSQLParameterType.TIMESTAMP, value: '2024-01-15 10:30:00' }),
      ]),
    ).to.deep.equal([
      { sqlType: 'DATE', value: '2024-01-15' },
      { sqlType: 'TIMESTAMP', value: '2024-01-15 10:30:00' },
    ]);
  });

  it('honours explicit TIMESTAMP_NTZ / TIMESTAMP_LTZ types (kernel param codec)', () => {
    expect(
      buildSeaPositionalParams([
        new DBSQLParameter({ type: DBSQLParameterType.TIMESTAMP_NTZ, value: '2024-01-15 10:30:00' }),
        new DBSQLParameter({ type: DBSQLParameterType.TIMESTAMP_LTZ, value: '2024-01-15 10:30:00' }),
      ]),
    ).to.deep.equal([
      { sqlType: 'TIMESTAMP_NTZ', value: '2024-01-15 10:30:00' },
      { sqlType: 'TIMESTAMP_LTZ', value: '2024-01-15 10:30:00' },
    ]);
  });

  it('routes a Date with explicit TIMESTAMP_NTZ type as NTZ (not the default TIMESTAMP)', () => {
    const d = new Date('2024-01-15T10:30:00.000Z');
    expect(
      buildSeaPositionalParams([new DBSQLParameter({ type: DBSQLParameterType.TIMESTAMP_NTZ, value: d })]),
    ).to.deep.equal([{ sqlType: 'TIMESTAMP_NTZ', value: d.toISOString() }]);
  });
});

describe('SeaPositionalParams.buildSeaNamedParams', () => {
  it('returns undefined for no named params', () => {
    expect(buildSeaNamedParams(undefined)).to.equal(undefined);
    expect(buildSeaNamedParams({})).to.equal(undefined);
  });

  it('emits {name, sqlType, value} triples, reusing the same type mapping', () => {
    expect(
      buildSeaNamedParams({
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
    expect(buildSeaNamedParams({ x: null })).to.deep.equal([{ name: 'x', sqlType: 'VOID' }]);
  });
});
