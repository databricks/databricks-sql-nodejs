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

import { execFileSync } from 'child_process';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { expect } from 'chai';
import sinon from 'sinon';
import winston from 'winston';
import DBSQLLogger, { LOG_FILE_UNAVAILABLE_WARNING_CODE } from '../../lib/DBSQLLogger';
import { LogLevel } from '../../lib/contracts/IDBSQLLogger';

async function waitForFileContent(file: string, text: string, timeoutMs = 2000): Promise<string> {
  const deadline = Date.now() + timeoutMs;
  const read = () => (fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : '');
  while (!read().includes(text) && Date.now() < deadline) {
    // eslint-disable-next-line no-await-in-loop
    await new Promise((resolve) => {
      setTimeout(resolve, 20);
    });
  }
  return read();
}

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

  // The File transport opens its stream asynchronously; teardown waits for that
  // before closing the logger and deleting the directory.
  function createLogger(filepath: string, level?: LogLevel): DBSQLLogger {
    const instance = new DBSQLLogger({ filepath, level });
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

  it('writes log lines to the default file inside the directory', async () => {
    logger = createLogger(tmpDir);
    logger.transports.console.silent = true;

    await logger.log(LogLevel.info, 'hello from DBSQLLogger test');

    const content = await waitForFileContent(path.join(tmpDir, 'databricks-sql-nodejs.log'), 'hello from');
    expect(content).to.contain('hello from DBSQLLogger test');
  });

  it('resolves an existing directory with a trailing separator to the default file inside it', () => {
    logger = createLogger(`${tmpDir}${path.sep}`);

    expect(logger.transports.file?.dirname).to.equal(tmpDir);
    expect(logger.transports.file?.filename).to.equal('databricks-sql-nodejs.log');
  });

  it('keeps writing to an existing file when filepath has a trailing separator', () => {
    const filepath = path.join(tmpDir, 'logs');
    fs.writeFileSync(filepath, '');
    logger = createLogger(`${filepath}${path.sep}`);

    expect(logger.transports.file?.dirname).to.equal(tmpDir);
    expect(logger.transports.file?.filename).to.equal('logs');
  });

  it('treats a non-existent filepath with a trailing separator as a file, as in 3.0.0', () => {
    const filepath = path.join(tmpDir, 'logs');
    logger = createLogger(`${filepath}${path.sep}`);

    expect(logger.transports.file?.dirname).to.equal(tmpDir);
    expect(logger.transports.file?.filename).to.equal('logs');
    expect(fs.statSync(filepath).isFile()).to.be.true;
  });

  it('does not open a FIFO synchronously', async function fifoTest() {
    if (process.platform === 'win32') {
      this.skip();
    }
    const fifo = path.join(tmpDir, 'pipe.log');
    execFileSync('mkfifo', [fifo]);
    // A non-blocking reader lets any writer open the FIFO without waiting. Keep
    // it open until winston's own open completes, or that open (and the test
    // process) would hang.
    const readerFd = fs.openSync(fifo, fs.constants.O_RDONLY | fs.constants.O_NONBLOCK);
    const openSync = sinon.spy(fs, 'openSync');
    try {
      logger = createLogger(fifo);
    } finally {
      openSync.restore();
    }
    await fileSettled;
    fs.closeSync(readerFd);

    expect(openSync.called).to.be.false;
    expect(logger.transports.file?.filename).to.equal('pipe.log');
  });

  describe('when the log file cannot be opened', () => {
    let emitWarning: sinon.SinonStub;

    beforeEach(() => {
      emitWarning = sinon.stub(process, 'emitWarning');
    });

    afterEach(() => {
      emitWarning.restore();
    });

    const cases = [
      {
        name: 'a parent path is a regular file',
        filepath: (dir: string) => {
          fs.writeFileSync(path.join(dir, 'not-a-dir'), '');
          return path.join(dir, 'not-a-dir', 'driver.log');
        },
      },
      {
        name: 'the resolved log file is a directory (EISDIR)',
        filepath: (dir: string) => {
          fs.mkdirSync(path.join(dir, 'databricks-sql-nodejs.log'));
          return dir;
        },
      },
    ];

    for (const { name, filepath } of cases) {
      it(`falls back to console only with a process warning when ${name}`, () => {
        logger = createLogger(filepath(tmpDir), LogLevel.error);

        expect(logger.transports.file).to.be.undefined;
        expect(emitWarning.calledOnce).to.be.true;
        expect(emitWarning.firstCall.args[0]).to.contain('logging to console only');
        expect(emitWarning.firstCall.args[1]).to.deep.equal({ code: LOG_FILE_UNAVAILABLE_WARNING_CODE });
      });
    }

    it('falls back to console only for a read-only log file (EACCES)', function readOnlyLogFileTest() {
      if (process.platform === 'win32' || process.getuid?.() === 0) {
        this.skip();
      }
      const filepath = path.join(tmpDir, 'readonly.log');
      fs.writeFileSync(filepath, '');
      fs.chmodSync(filepath, 0o444);
      try {
        logger = createLogger(filepath, LogLevel.error);

        expect(logger.transports.file).to.be.undefined;
        expect(emitWarning.calledOnce).to.be.true;
        expect(emitWarning.firstCall.args[0]).to.contain('EACCES');
        expect(emitWarning.firstCall.args[0]).to.contain('logging to console only');
        expect(emitWarning.firstCall.args[1]).to.deep.equal({ code: LOG_FILE_UNAVAILABLE_WARNING_CODE });
      } finally {
        fs.chmodSync(filepath, 0o600);
      }
    });

    it('keeps delivering console logs after the fallback', async () => {
      fs.mkdirSync(path.join(tmpDir, 'databricks-sql-nodejs.log'));
      const seen: string[] = [];
      const consoleLog = sinon.stub(winston.transports.Console.prototype, 'log').callsFake((...args: unknown[]) => {
        seen.push((args[0] as { message: string }).message);
        (args[1] as () => void)();
      });
      try {
        logger = createLogger(tmpDir);
        // Far more than the logger's stream buffers hold; an unopened file
        // transport used to stall delivery to every transport at that point.
        const lines = 1000;
        for (let i = 0; i < lines; i += 1) {
          logger.logger.info(`line ${i} ${'x'.repeat(100)}`);
        }
        const deadline = Date.now() + 1500;
        while (seen.length < lines && Date.now() < deadline) {
          // eslint-disable-next-line no-await-in-loop
          await new Promise((resolve) => {
            setTimeout(resolve, 20);
          });
        }

        expect(seen).to.have.length(lines);
        expect(seen[lines - 1]).to.match(new RegExp(`^line ${lines - 1} `));
      } finally {
        consoleLog.restore();
      }
    });
  });

  it('uses a non-existent filepath unchanged', () => {
    const filepath = path.join(tmpDir, 'custom.log');
    logger = createLogger(filepath);

    expect(logger.transports.file).to.not.be.undefined;
    expect(logger.transports.file?.dirname).to.equal(tmpDir);
    expect(logger.transports.file?.filename).to.equal('custom.log');
  });

  it('creates missing parent directories and still attaches the file transport', () => {
    const nestedDir = path.join(tmpDir, 'a', 'b');
    const filepath = path.join(nestedDir, 'driver.log');
    logger = createLogger(filepath);

    expect(logger.transports.file).to.not.be.undefined;
    expect(logger.transports.file?.dirname).to.equal(nestedDir);
    expect(logger.transports.file?.filename).to.equal('driver.log');
    expect(fs.statSync(nestedDir).isDirectory()).to.be.true;
    expect(fs.existsSync(filepath)).to.be.true;
  });

  it('does not mkdir an existing directory (mkdir of a Windows drive root throws EPERM)', () => {
    const mkdirSync = sinon.stub(fs, 'mkdirSync').throws(Object.assign(new Error('EPERM'), { code: 'EPERM' }));
    try {
      logger = createLogger(path.join(tmpDir, 'driver.log'));
    } finally {
      mkdirSync.restore();
    }

    expect(mkdirSync.called).to.be.false;
    expect(logger.transports.file?.dirname).to.equal(tmpDir);
    expect(logger.transports.file?.filename).to.equal('driver.log');
  });

  it('uses an existing file path unchanged', () => {
    const filepath = path.join(tmpDir, 'existing.log');
    fs.writeFileSync(filepath, '');
    logger = createLogger(filepath);

    expect(logger.transports.file?.dirname).to.equal(tmpDir);
    expect(logger.transports.file?.filename).to.equal('existing.log');
  });
});
