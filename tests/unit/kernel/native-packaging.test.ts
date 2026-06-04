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
import { readFileSync } from 'fs';
import { join } from 'path';

// Guards the napi-rs router's per-platform npm package names. A misconfigured
// `npmName` once baked the M0 triple into the prefix for *every* platform
// (e.g. `@databricks/kernel-native-linux-x64-gnu-darwin-arm64`, and the doubled
// `@databricks/kernel-native-linux-x64-gnu-linux-x64-gnu`), so a published
// install would never resolve a `.node`. The canonical name is
// `@databricks/sql-kernel-<triple>` (see native/kernel/README.md and the
// KernelNativeLoader load-failure hint).
describe('SEA native binding — packaging (native/kernel/index.js)', () => {
  // Resolved from the repo root (the cwd for `npm test`) so the test does not
  // depend on the module system's `__dirname`.
  const indexJs = readFileSync(join(process.cwd(), 'native/kernel/index.js'), 'utf8');

  // Every `require('@databricks/...')` fallback in the generated router.
  const required = Array.from(indexJs.matchAll(/require\('(@databricks\/[^']+)'\)/g)).map((m) => m[1]);

  it('declares at least one @databricks/* npm fallback', () => {
    expect(required.length, 'no @databricks/* require() found in the router').to.be.greaterThan(0);
  });

  it('every npm fallback uses the canonical @databricks/sql-kernel-<triple> name', () => {
    const triple = /^@databricks\/sql-kernel-[a-z0-9]+(-[a-z0-9]+)*$/;
    for (const name of required) {
      expect(name, `unexpected SEA native package name: ${name}`).to.match(triple);
    }
  });

  it('contains no garbled / doubled triple prefix', () => {
    expect(indexJs, 'router still references the garbled sea-native prefix').to.not.contain('sea-native');
    expect(indexJs, 'router still doubles the linux-x64-gnu triple').to.not.contain('linux-x64-gnu-linux-x64-gnu');
  });

  it('resolves the M0 linux-x64-gnu triple to @databricks/sql-kernel-linux-x64-gnu', () => {
    expect(required, 'M0 supported triple package missing from the router').to.include(
      '@databricks/sql-kernel-linux-x64-gnu',
    );
  });
});
