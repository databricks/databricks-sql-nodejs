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

/**
 * Assert the napi `ConnectionOptions` an adapter built (or forwarded to the
 * binding) equal `expectedRest` once the always-present `customHeaders` is
 * set aside.
 *
 * Every kernel connection carries a `customHeaders` entry for the connector
 * `User-Agent` (appended unconditionally — see `buildKernelHttpOptions`,
 * mirroring the Python connector). Its exact value is environment-dependent
 * (driver version / Node version / OS), so a plain `deep.equal` of the whole
 * options object can't pin it. This helper deep-equals everything *except*
 * `customHeaders`, then asserts `customHeaders` is exactly the connector
 * `User-Agent` (the default case where the caller set no extra headers /
 * `userAgentEntry`). The full header/UA composition is covered exhaustively
 * by `connectionOptions.test.ts`.
 */
export default function expectNativeConnectionOptions(actual: unknown, expectedRest: Record<string, unknown>): void {
  const { customHeaders, ...rest } = actual as Record<string, unknown> & {
    customHeaders?: Array<{ name: string; value: string }>;
  };
  expect(rest).to.deep.equal(expectedRest);
  expect(customHeaders, 'customHeaders').to.be.an('array').with.lengthOf(1);
  expect(customHeaders?.[0].name).to.equal('User-Agent');
  expect(customHeaders?.[0].value).to.match(/NodejsDatabricksSqlConnector\//);
}
