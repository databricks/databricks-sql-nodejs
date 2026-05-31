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
import SeaBackend from '../../../lib/sea/SeaBackend';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';

interface LoggedLine {
  level: LogLevel;
  message: unknown;
}

/** Minimal IClientContext stub that records logger calls. */
function fakeContext(sink: LoggedLine[]) {
  const logger = {
    log(level: LogLevel, message: unknown) {
      sink.push({ level, message });
    },
  };
  return { getLogger: () => logger } as any;
}

function patOpts(extra: Partial<ConnectionOptions> = {}): ConnectionOptions {
  return {
    host: 'example.cloud.databricks.com',
    path: '/sql/1.0/warehouses/abc',
    token: 'dapi-fake-pat',
    ...extra,
  } as ConnectionOptions;
}

describe('SeaBackend — TLS verification log', () => {
  it('logs a debug note when server-cert verification is left at the default (disabled)', async () => {
    const lines: LoggedLine[] = [];
    const backend = new SeaBackend({ context: fakeContext(lines), nativeBinding: {} as any });

    await backend.connect(patOpts());

    const notes = lines.filter((l) => l.level === LogLevel.debug);
    expect(notes).to.have.lengthOf(1);
    expect(String(notes[0].message)).to.match(/verification is DISABLED/);
  });

  it('logs a debug note when checkServerCertificate is explicitly false', async () => {
    const lines: LoggedLine[] = [];
    const backend = new SeaBackend({ context: fakeContext(lines), nativeBinding: {} as any });

    await backend.connect(patOpts({ checkServerCertificate: false }));

    expect(lines.filter((l) => l.level === LogLevel.debug)).to.have.lengthOf(1);
  });

  it('does NOT log when checkServerCertificate is true', async () => {
    const lines: LoggedLine[] = [];
    const backend = new SeaBackend({ context: fakeContext(lines), nativeBinding: {} as any });

    await backend.connect(patOpts({ checkServerCertificate: true }));

    expect(lines.filter((l) => l.level === LogLevel.debug)).to.have.lengthOf(0);
  });
});
