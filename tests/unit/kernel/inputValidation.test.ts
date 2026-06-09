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
import Int64 from 'node-int64';
import assertBindableValue from '../../../lib/kernel/KernelInputValidation';
import { DBSQLParameter, DBSQLParameterType } from '../../../lib/DBSQLParameter';
import ParameterError from '../../../lib/errors/ParameterError';

describe('KernelInputValidation.assertBindableValue', () => {
  it('accepts scalars, Date, Int64, bigint, null, and DBSQLParameter', () => {
    expect(() => assertBindableValue(42, 'p')).to.not.throw();
    expect(() => assertBindableValue('x', 'p')).to.not.throw();
    expect(() => assertBindableValue(true, 'p')).to.not.throw();
    expect(() => assertBindableValue(BigInt(10), 'p')).to.not.throw();
    expect(() => assertBindableValue(null, 'p')).to.not.throw();
    expect(() => assertBindableValue(new Date(), 'p')).to.not.throw();
    expect(() => assertBindableValue(new Int64(5), 'p')).to.not.throw();
    expect(() =>
      assertBindableValue(new DBSQLParameter({ type: DBSQLParameterType.INTEGER, value: 1 }), 'p'),
    ).to.not.throw();
  });

  it('rejects arrays (compound types)', () => {
    expect(() => assertBindableValue([1, 2, 3] as never, 'ordinalParameters[0]')).to.throw(ParameterError, /array/);
  });

  it('rejects plain objects', () => {
    expect(() => assertBindableValue({ a: 1 } as never, 'p')).to.throw(ParameterError, /object/);
  });
});
