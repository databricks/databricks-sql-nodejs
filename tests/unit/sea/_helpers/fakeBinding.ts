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

import { SeaNativeBinding, SeaNativeConnection } from '../../../../lib/sea/SeaNativeLoader';

export interface RecordedCall {
  method: string;
  args: unknown[];
}

export interface FakeBinding {
  binding: SeaNativeBinding;
  calls: RecordedCall[];
}

/**
 * Build a fake `SeaNativeBinding` that records every `openSession` call
 * and returns a `Connection` whose `close()` is also recorded. Shared
 * across the SEA auth unit-test files (PAT / M2M / U2M / future flows)
 * so the closure shape lives in exactly one place.
 *
 * No real native code runs — the fake is structural-typing-only.
 */
export function makeFakeBinding(): FakeBinding {
  const calls: RecordedCall[] = [];

  const fakeConnection = {
    async executeStatement() {
      throw new Error('not used in this test');
    },
    async close() {
      calls.push({ method: 'connection.close', args: [] });
    },
  };

  const binding: SeaNativeBinding = {
    version() {
      return 'fake-binding';
    },
    async openSession(opts: Parameters<SeaNativeBinding['openSession']>[0]) {
      calls.push({ method: 'openSession', args: [opts] });
      return fakeConnection as unknown as SeaNativeConnection;
    },
    // Index the binding type for the napi class constructor types; the
    // loader exports Connection/Statement as type aliases, so `typeof
    // Connection` is illegal and bare `Function` has no construct signature.
    Connection: function FakeConnection() {} as unknown as SeaNativeBinding['Connection'],
    Statement: function FakeStatement() {} as unknown as SeaNativeBinding['Statement'],
  };

  return { binding, calls };
}
