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
import { version, getSeaNative } from '../../lib/sea/SeaNativeLoader';

describe('SEA native binding — smoke test', () => {
  it('loads the .node artifact and returns version()', () => {
    const v = version();
    // Round 1b: the native crate is at 0.1.0. Match the shape rather
    // than the literal so the test does not need updating on every
    // version bump.
    expect(v).to.match(/^\d+\.\d+\.\d+$/);
  });

  it('exposes the Database opaque class', () => {
    const binding = getSeaNative() as unknown as { Database: new (opts: object) => object };
    expect(typeof binding.Database).to.equal('function');
    const db = new binding.Database({});
    expect(db).to.be.an('object');
  });

  it('exposes the Connection opaque class', () => {
    const binding = getSeaNative() as unknown as { Connection: new (opts: object) => object };
    expect(typeof binding.Connection).to.equal('function');
    const conn = new binding.Connection({});
    expect(conn).to.be.an('object');
  });
});
