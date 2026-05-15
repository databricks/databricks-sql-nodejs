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
    expect(v).to.match(/^\d+\.\d+\.\d+$/);
  });

  it('exposes the openSession factory function', () => {
    const binding = getSeaNative() as unknown as { openSession: Function };
    expect(typeof binding.openSession).to.equal('function');
  });

  it('exposes the Connection opaque class', () => {
    const binding = getSeaNative() as unknown as { Connection: Function };
    expect(typeof binding.Connection).to.equal('function');
  });

  it('exposes the Statement opaque class', () => {
    const binding = getSeaNative() as unknown as { Statement: Function };
    expect(typeof binding.Statement).to.equal('function');
  });
});
