import { expect } from 'chai';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { DBSQLClient } from '../../lib';
import DBSQLLogger from '../../lib/DBSQLLogger';
import { LogLevel } from '../../lib/contracts/IDBSQLLogger';
import config from './utils/config';

const delay = (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

// Runs a small Thrift workload with the given logger, collecting any errors emitted
// by the logger / its file transport or thrown as uncaught exceptions meanwhile.
async function runWorkload(logger: DBSQLLogger): Promise<Array<Error>> {
  const errors: Array<Error> = [];
  const onError = (error: Error) => {
    errors.push(error);
  };
  process.on('uncaughtException', onError);
  logger.logger.on('error', onError);
  logger.transports.file?.on('error', onError);

  try {
    const client = new DBSQLClient({ logger });
    await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
    });
    const session = await client.openSession({
      initialCatalog: config.catalog,
      initialSchema: config.schema,
    });
    const operation = await session.executeStatement('SELECT 1');
    await operation.fetchAll();
    await operation.close();
    await session.close();
    await client.close();

    // winston's file transport writes asynchronously
    await delay(1_500);
  } finally {
    process.removeListener('uncaughtException', onError);
  }

  return errors;
}

function listRegularFiles(dir: string): Array<string> {
  return fs
    .readdirSync(dir)
    .map((name) => path.join(dir, name))
    .filter((p) => fs.statSync(p).isFile());
}

describe('DBSQLLogger file destination (LOGGING-002)', function loggingSuite() {
  this.timeout(120_000);

  const tempDirs: Array<string> = [];

  afterEach(() => {
    while (tempDirs.length > 0) {
      const dir = tempDirs.pop() as string;
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  function makeTempDir() {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbsql-log-'));
    tempDirs.push(dir);
    return dir;
  }

  it('directory log destination resolves to a log file inside it', async () => {
    const dir = makeTempDir();
    const logger = new DBSQLLogger({ level: LogLevel.debug, filepath: dir });

    const errors = await runWorkload(logger);

    expect(errors, `unexpected logger errors: ${errors.map((e) => e.message).join('; ')}`).to.be.empty;
    const files = listRegularFiles(dir);
    expect(files.length, 'expected a log file to be created inside the directory').to.be.greaterThan(0);
    const contents = files.map((f) => fs.readFileSync(f, 'utf8')).join('');
    expect(contents.trim().length, 'expected driver log lines inside the directory').to.be.greaterThan(0);
  });

  it('file log destination writes logs to exactly that file', async () => {
    const dir = makeTempDir();
    const logFile = path.join(dir, 'explicit.log');
    const logger = new DBSQLLogger({ level: LogLevel.debug, filepath: logFile });

    const errors = await runWorkload(logger);

    expect(errors).to.be.empty;
    expect(listRegularFiles(dir)).to.deep.equal([logFile]);
    expect(fs.readFileSync(logFile, 'utf8').trim().length).to.be.greaterThan(0);
  });
});
