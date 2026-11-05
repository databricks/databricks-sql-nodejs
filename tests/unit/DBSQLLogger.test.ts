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

import fs from 'fs';
import os from 'os';
import path from 'path';
import { expect } from 'chai';
import DBSQLLogger from '../../lib/DBSQLLogger';
import { LogLevel } from '../../lib/contracts/IDBSQLLogger';

describe('DBSQLLogger level subscription', () => {
  it('getLevel reflects the configured level', () => {
    const logger = new DBSQLLogger({ level: LogLevel.warn });
    expect(logger.getLevel()).to.equal(LogLevel.warn);
  });

  it('onLevelChange fires subscribers with the new level on setLevel', () => {
    const logger = new DBSQLLogger({ level: LogLevel.info });
    const seen: LogLevel[] = [];
    logger.onLevelChange((level) => seen.push(level));

    logger.setLevel(LogLevel.debug);
    logger.setLevel(LogLevel.error);

    expect(seen).to.deep.equal([LogLevel.debug, LogLevel.error]);
    // setLevel still updates the logger's own level.
    expect(logger.getLevel()).to.equal(LogLevel.error);
  });

  it('the returned unsubscribe stops further notifications', () => {
    const logger = new DBSQLLogger({ level: LogLevel.info });
    const seen: LogLevel[] = [];
    const unsubscribe = logger.onLevelChange((level) => seen.push(level));

    logger.setLevel(LogLevel.debug);
    unsubscribe();
    logger.setLevel(LogLevel.warn);

    expect(seen).to.deep.equal([LogLevel.debug]);
  });

  it('a throwing subscriber does not break setLevel or other subscribers', () => {
    const logger = new DBSQLLogger({ level: LogLevel.info });
    const seen: LogLevel[] = [];
    logger.onLevelChange(() => {
      throw new Error('boom');
    });
    logger.onLevelChange((level) => seen.push(level));

    expect(() => logger.setLevel(LogLevel.debug)).to.not.throw();
    expect(seen).to.deep.equal([LogLevel.debug]);
    expect(logger.getLevel()).to.equal(LogLevel.debug);
  });
});

describe('DBSQLLogger file transport path resolution', () => {
  let tmpDir: string;
  let logger: DBSQLLogger | undefined;
  let fileSettled: Promise<void> | undefined;

  // The winston File transport opens its write stream asynchronously after the
  // constructor returns. Subscribe to `open`/`error` right away so teardown can
  // wait for it to settle before deleting the directory — otherwise a deferred
  // open could fail with ENOENT and surface as an uncaught error later.
  function createLogger(filepath: string): DBSQLLogger {
    const instance = new DBSQLLogger({ filepath });
    const file = instance.transports.file;
    fileSettled = file
      ? new Promise<void>((resolve) => {
          file.once('open', () => resolve());
          file.once('error', () => resolve());
        })
      : undefined;
    return instance;
  }

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbsql-logger-'));
  });

  afterEach(async () => {
    if (fileSettled) {
      await fileSettled;
    }
    logger?.logger.close();
    logger = undefined;
    fileSettled = undefined;
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('writes to a default file inside the directory when filepath is an existing directory', () => {
    logger = createLogger(tmpDir);

    expect(logger.transports.file).to.not.be.undefined;
    expect(logger.transports.file?.dirname).to.equal(tmpDir);
    expect(logger.transports.file?.filename).to.equal('databricks-sql-nodejs.log');
  });

  it('treats a non-existent filepath with a trailing separator as a directory', () => {
    const dir = path.join(tmpDir, 'nested', 'logs');
    logger = createLogger(`${dir}${path.sep}`);

    expect(logger.transports.file).to.not.be.undefined;
    expect(logger.transports.file?.dirname).to.equal(dir);
    expect(logger.transports.file?.filename).to.equal('databricks-sql-nodejs.log');
    expect(fs.statSync(dir).isDirectory()).to.be.true;
  });

  it('uses a non-existent filepath unchanged', () => {
    const filepath = path.join(tmpDir, 'custom.log');
    logger = createLogger(filepath);

    expect(logger.transports.file).to.not.be.undefined;
    expect(logger.transports.file?.dirname).to.equal(tmpDir);
    expect(logger.transports.file?.filename).to.equal('custom.log');
  });

  it('uses an existing file path unchanged', () => {
    const filepath = path.join(tmpDir, 'existing.log');
    fs.writeFileSync(filepath, '');
    logger = createLogger(filepath);

    expect(logger.transports.file?.dirname).to.equal(tmpDir);
    expect(logger.transports.file?.filename).to.equal('existing.log');
  });
});
