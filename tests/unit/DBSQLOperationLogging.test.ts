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
import { TOperationType } from '../../thrift/TCLIService_types';
import DBSQLOperation from '../../lib/DBSQLOperation';
import Status from '../../lib/dto/Status';
import IDBSQLLogger, { LogLevel } from '../../lib/contracts/IDBSQLLogger';
import IOperationBackend from '../../lib/contracts/IOperationBackend';
import { OperationState, OperationStatus } from '../../lib/contracts/OperationStatus';
import { ResultMetadata } from '../../lib/contracts/ResultMetadata';
import OperationStateError, { OperationStateErrorCode } from '../../lib/errors/OperationStateError';
import ClientContextStub from './.stubs/ClientContextStub';
import { createOperationForTest } from './.stubs/createOperationForTest';

const PER_FETCH = /^Fetched chunk of size: \d+ from operation with id/;
const SUMMARY = /^Result set of operation with id: \S+ (closed|cancelled) \(rows: \d+, fetches: \d+\)$/;

interface Record {
  level: LogLevel;
  message: string;
}

class RecordingLogger implements IDBSQLLogger {
  public records: Record[] = [];

  public log(level: LogLevel, message: string) {
    this.records.push({ level, message });
  }

  public matching(re: RegExp): Record[] {
    return this.records.filter((r) => re.test(r.message));
  }
}

class LeveledRecordingLogger extends RecordingLogger {
  constructor(public level: LogLevel) {
    super();
  }

  public getLevel(): LogLevel {
    return this.level;
  }
}

// Serves `totalRows` rows, up to `limit` per fetch.
class RowsBackend implements IOperationBackend {
  public readonly id = 'op-1';

  public readonly operationType = TOperationType.EXECUTE_STATEMENT;

  private served = 0;

  public waitError?: OperationStateError;

  constructor(private readonly totalRows: number, private readonly resultSet = true) {}

  public hasResultSet() {
    return this.resultSet;
  }

  public async fetchChunk({ limit }: { limit: number }) {
    const count = Math.min(limit, this.totalRows - this.served);
    const rows = Array.from({ length: count }, (_, i) => ({ id: this.served + i }));
    this.served += count;
    return rows;
  }

  public async hasMore() {
    return this.served < this.totalRows;
  }

  public async waitUntilReady() {
    if (this.waitError) {
      throw this.waitError;
    }
  }

  public async status(): Promise<OperationStatus> {
    return { state: OperationState.Succeeded, hasResultSet: this.resultSet };
  }

  public async getResultMetadata(): Promise<ResultMetadata> {
    return {} as ResultMetadata;
  }

  public async cancel() {
    return Status.success();
  }

  public async close() {
    return Status.success();
  }
}

function createOperation(logger: IDBSQLLogger, backend: IOperationBackend) {
  const context = new ClientContextStub();
  context.logger = logger;
  return new DBSQLOperation({ backend, context });
}

async function fetchOneRowAtATime(operation: DBSQLOperation, stopAfterRows?: number) {
  let rows = 0;
  for (;;) {
    // eslint-disable-next-line no-await-in-loop
    rows += (await operation.fetchChunk({ maxRows: 1, disableBuffering: true })).length;
    // eslint-disable-next-line no-await-in-loop
    if ((stopAfterRows !== undefined && rows >= stopAfterRows) || !(await operation.hasMoreRows())) {
      return rows;
    }
  }
}

describe('DBSQLOperation result set logging', () => {
  it('logs no per-fetch record at debug and one summary at close', async () => {
    const logger = new LeveledRecordingLogger(LogLevel.debug);
    const operation = createOperation(logger, new RowsBackend(5));

    expect(await fetchOneRowAtATime(operation)).to.equal(5);
    await operation.close();

    expect(logger.matching(PER_FETCH)).to.have.length(0);
    expect(logger.matching(SUMMARY)).to.deep.equal([
      { level: LogLevel.debug, message: 'Result set of operation with id: op-1 closed (rows: 5, fetches: 5)' },
    ]);
  });

  it('logs one trace record per fetch and still one summary at trace', async () => {
    const logger = new LeveledRecordingLogger(LogLevel.trace);
    const operation = createOperation(logger, new RowsBackend(5));

    await fetchOneRowAtATime(operation);
    await operation.close();

    const perFetch = logger.matching(PER_FETCH);
    expect(perFetch).to.have.length(5);
    expect(perFetch.every((r) => r.level === LogLevel.trace)).to.be.true;
    expect(logger.matching(SUMMARY)).to.have.length(1);
  });

  it('follows a runtime level change for the per-fetch record', async () => {
    const logger = new LeveledRecordingLogger(LogLevel.debug);
    const operation = createOperation(logger, new RowsBackend(5));

    await operation.fetchChunk({ maxRows: 1, disableBuffering: true });
    logger.level = LogLevel.trace;
    await operation.fetchChunk({ maxRows: 1, disableBuffering: true });

    expect(logger.matching(PER_FETCH)).to.have.length(1);
  });

  it('reports what a partially read result set did', async () => {
    const logger = new LeveledRecordingLogger(LogLevel.debug);
    const operation = createOperation(logger, new RowsBackend(100));

    expect(await fetchOneRowAtATime(operation, 2)).to.equal(2);
    await operation.close();

    expect(logger.matching(SUMMARY).map((r) => r.message)).to.deep.equal([
      'Result set of operation with id: op-1 closed (rows: 2, fetches: 2)',
    ]);
  });

  it('counts rows across multi-row fetches and fetchAll', async () => {
    const logger = new LeveledRecordingLogger(LogLevel.debug);
    const operation = createOperation(logger, new RowsBackend(25));

    expect(await operation.fetchAll({ maxRows: 10 })).to.have.length(25);
    await operation.close();

    expect(logger.matching(SUMMARY).map((r) => r.message)).to.deep.equal([
      'Result set of operation with id: op-1 closed (rows: 25, fetches: 3)',
    ]);
  });

  it('logs the summary once when a result set is cancelled and then closed', async () => {
    const logger = new LeveledRecordingLogger(LogLevel.debug);
    const operation = createOperation(logger, new RowsBackend(10));

    await operation.fetchChunk({ maxRows: 3, disableBuffering: true });
    await operation.cancel();
    await operation.close();
    await operation.cancel();

    expect(logger.matching(SUMMARY).map((r) => r.message)).to.deep.equal([
      'Result set of operation with id: op-1 cancelled (rows: 3, fetches: 1)',
    ]);
  });

  it('logs the summary once when close is called twice', async () => {
    const logger = new LeveledRecordingLogger(LogLevel.debug);
    const operation = createOperation(logger, new RowsBackend(10));

    await operation.close();
    await operation.close();

    expect(logger.matching(SUMMARY).map((r) => r.message)).to.deep.equal([
      'Result set of operation with id: op-1 closed (rows: 0, fetches: 0)',
    ]);
  });

  for (const [code, outcome] of [
    [OperationStateErrorCode.Canceled, 'cancelled'],
    [OperationStateErrorCode.Closed, 'closed'],
  ] as const) {
    it(`logs the summary once when the backend reports the operation ${outcome}`, async () => {
      const logger = new LeveledRecordingLogger(LogLevel.debug);
      const backend = new RowsBackend(10);
      const operation = createOperation(logger, backend);

      await operation.fetchChunk({ maxRows: 2, disableBuffering: true });
      backend.waitError = new OperationStateError(code);
      let error: unknown;
      try {
        await operation.hasMoreRows();
      } catch (e) {
        error = e;
      }
      expect(error).to.be.instanceOf(OperationStateError);
      await operation.close();
      await operation.cancel();

      expect(logger.matching(SUMMARY).map((r) => r.message)).to.deep.equal([
        `Result set of operation with id: op-1 ${outcome} (rows: 2, fetches: 1)`,
      ]);
    });
  }

  it('logs no summary for an operation without a result set', async () => {
    const logger = new LeveledRecordingLogger(LogLevel.debug);
    const operation = createOperation(logger, new RowsBackend(0, false));

    await operation.close();

    expect(logger.matching(SUMMARY)).to.have.length(0);
  });

  it('never sends trace records to a logger without getLevel', async () => {
    const logger = new RecordingLogger();
    const operation = createOperation(logger, new RowsBackend(5));

    await fetchOneRowAtATime(operation);
    await operation.close();

    expect(logger.records.some((r) => r.level === LogLevel.trace)).to.be.false;
    expect(logger.matching(PER_FETCH)).to.have.length(0);
    expect(logger.matching(SUMMARY)).to.have.length(1);
  });

  it('logs the summary after the Thrift backend closes the operation', async () => {
    const context = new ClientContextStub();
    const logger = new LeveledRecordingLogger(LogLevel.debug);
    context.logger = logger;
    const operation = createOperationForTest({
      handle: {
        operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
        operationType: TOperationType.EXECUTE_STATEMENT,
        hasResultSet: true,
      },
      context,
    });

    await operation.fetchChunk({ maxRows: 1, disableBuffering: true });
    await operation.close();

    const messages = logger.records.map((r) => r.message);
    const closing = messages.findIndex((m) => m.startsWith('Closing operation with id'));
    const summary = messages.findIndex((m) => SUMMARY.test(m));
    expect(closing).to.be.gte(0);
    expect(summary).to.be.greaterThan(closing);
    expect(messages[summary]).to.match(/ closed \(rows: 3, fetches: 1\)$/);
    expect(logger.matching(PER_FETCH)).to.have.length(0);
  });
});
