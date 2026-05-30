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
import { tryGetSeaNative } from '../../../lib/sea/SeaNativeLoader';

// On a CI runner whose triple is supposed to have a published binding
// (M0 = linux-x64-gnu) a missing binding is a hard failure — a silent
// skip there would mask a broken build / packaging regression. On every
// other platform (and on dev machines) the binding is optional, so we
// skip.
function bindingIsExpected(): boolean {
  return process.env.CI === 'true' && process.platform === 'linux' && process.arch === 'x64';
}

describe('SEA native binding — smoke test', function smoke() {
  const binding = tryGetSeaNative();

  if (binding === undefined) {
    if (bindingIsExpected()) {
      it('fails loudly: the binding must load on the linux-x64 CI runner', () => {
        expect.fail(
          'SEA native binding failed to load on a linux-x64 CI runner where ' +
            '@databricks/sql-kernel-linux-x64-gnu is expected. Run `npm run build:native` or check packaging.',
        );
      });
      return;
    }
    // Optional dependency absent on this platform — skip rather than fail.
    // eslint-disable-next-line no-invalid-this
    this.pending = true;
    it.skip('SEA native binding not available on this platform');
    return;
  }

  it('returns a semver version()', () => {
    expect(binding.version()).to.match(/^\d+\.\d+\.\d+$/);
  });

  it('exposes the full binding surface the driver depends on', () => {
    // Guards against kernel-side renames: if the kernel drops/renames a
    // free function or class, this fails instead of staying green.
    expect(binding.version, 'version()').to.be.a('function');
    expect(binding.openSession, 'openSession()').to.be.a('function');
    expect(binding.Connection, 'Connection class').to.be.a('function');
    expect(binding.Statement, 'Statement class').to.be.a('function');
  });
});
