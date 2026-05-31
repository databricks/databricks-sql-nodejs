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
import { buildSeaPositionalParams } from '../../../lib/sea/SeaPositionalParams';
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
});
