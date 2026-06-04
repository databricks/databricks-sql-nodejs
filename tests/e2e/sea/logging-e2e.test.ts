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

// End-to-end proof that kernel (Rust) logs and Node-driver logs land in the
// SAME `DBSQLLogger` sink — here, one file. Goes through the full public
// `DBSQLClient` surface (not the raw binding) so the `SeaBackend` →
// `installKernelLogBridge` → napi `initKernelLogging` wiring is exercised.

import { expect } from 'chai';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { tryGetSeaNative } from '../../../lib/sea/SeaNativeLoader';
import { DBSQLClient } from '../../../lib';
import DBSQLLogger from '../../../lib/DBSQLLogger';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import config from '../utils/config';
import { InternalConnectionOptions } from '../../../lib/contracts/InternalConnectionOptions';

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('SEA — unified kernel + driver logging', function unifiedLogging() {
  // Live-warehouse round-trip plus async log flush.
  this.timeout(60_000);

  const binding = tryGetSeaNative();
  if (binding === undefined) {
    it.skip('SEA native binding not available on this platform');
    return;
  }

  it('routes kernel (Rust) logs into the same DBSQLLogger file as driver logs', async () => {
    const logFile = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'dbsql-kernel-log-')), 'unified.log');
    // debug so the kernel's per-statement lifecycle events cross the bridge.
    const logger = new DBSQLLogger({ level: LogLevel.debug, filepath: logFile });
    const client = new DBSQLClient({ logger });

    await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
      // Route through the kernel backend (internal opt-in flag).
      ...({ useSEA: true } as InternalConnectionOptions),
    });
    const session = await client.openSession();
    const operation = await session.executeStatement('SELECT 1');
    await operation.fetchAll();
    await operation.close();
    await session.close();
    await client.close();

    // winston's file transport + the batched kernel bridge are async; give them
    // a beat to flush before reading the file back.
    await delay(1_500);

    const contents = fs.readFileSync(logFile, 'utf8');
    const lines = contents.split('\n').filter((l) => l.trim().length > 0);

    const kernelLines = lines.filter((l) => l.includes('[kernel '));
    const driverLines = lines.filter((l) => !l.includes('[kernel '));

    // Both layers present in the one file → unified.
    expect(driverLines.length, 'expected driver-origin log lines').to.be.greaterThan(0);
    expect(kernelLines.length, 'expected kernel-origin ([kernel …) log lines').to.be.greaterThan(0);
    // The kernel target tag is preserved.
    expect(contents).to.match(/\[kernel databricks::sql::kernel\]/);

    fs.rmSync(path.dirname(logFile), { recursive: true, force: true });
  });
});
