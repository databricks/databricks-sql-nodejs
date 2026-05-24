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

describe('SEA native binding — smoke test', function smoke() {
  const binding = tryGetSeaNative();
  if (binding === undefined) {
    // The binding is an optional dependency. On platforms where the
    // .node artifact isn't installed (CI matrix entries without a
    // corresponding sea-native package, dev machines that haven't
    // run `npm run build:native`, etc.), skip the suite rather than
    // fail the build.
    // eslint-disable-next-line no-invalid-this
    this.pending = true;
    it.skip('SEA native binding not available on this platform');
    return;
  }

  it('returns a semver version()', () => {
    expect(binding.version()).to.match(/^\d+\.\d+\.\d+$/);
  });
});
