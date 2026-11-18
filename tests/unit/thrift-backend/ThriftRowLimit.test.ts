import assert from 'assert';
import http from 'http';
import { AddressInfo } from 'net';
import { expect } from 'chai';
import { tableFromArrays, tableToIPC } from 'apache-arrow';
import Int64 from 'node-int64';
import sinon from 'sinon';
import {
  TFetchResultsResp,
  TSparkRowSetType,
  TSessionHandle,
  TStatusCode,
  TTypeId,
} from '../../../thrift/TCLIService_types';
import { RowMode } from '../../../lib/contracts/IDBSQLSession';
import StatusError from '../../../lib/errors/StatusError';
import ClientContextStub from '../.stubs/ClientContextStub';
import { createSessionForTest } from '../.stubs/createSessionForTest';

const sessionHandle: TSessionHandle = {
  sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
};
const status = { statusCode: TStatusCode.SUCCESS_STATUS };

function createArrowPage(
  start: number,
  hasMoreRows: boolean,
): { schema: Buffer; ipc: Buffer; page: TFetchResultsResp } {
  const table = tableFromArrays({ id: new Int32Array(Array.from({ length: 6 }, (_, index) => start + index)) });
  const ipc = Buffer.from(tableToIPC(table, 'stream'));
  const schemaLength = 8 + ipc.readInt32LE(4);
  return {
    schema: ipc.subarray(0, schemaLength),
    ipc,
    page: {
      status,
      hasMoreRows,
      results: {
        startRowOffset: new Int64(start),
        rows: [],
        arrowBatches: [{ batch: ipc.subarray(schemaLength), rowCount: new Int64(6) }],
      },
    },
  };
}

function createContext(resultFormat: TSparkRowSetType): ClientContextStub {
  const context = new ClientContextStub();
  context.driver.executeStatementResp.operationHandle!.hasResultSet = true;
  context.driver.getResultSetMetadataResp.resultFormat = resultFormat;
  context.driver.getResultSetMetadataResp.schema = {
    columns: [{ columnName: 'id', position: 1, typeDesc: { types: [{ primitiveEntry: { type: TTypeId.INT_TYPE } }] } }],
  };
  return context;
}

describe('Thrift rowLimit source budgets', () => {
  afterEach(() => sinon.restore());

  for (const method of ['fetchAll', 'hasMoreRows'] as const) {
    it(`reports an already-prefetched error before zero-cap ${method}`, async () => {
      const context = createContext(TSparkRowSetType.ARROW_BASED_SET);
      context.driver.executeStatementResp.directResults = {
        resultSet: { status: { statusCode: TStatusCode.ERROR_STATUS, errorMessage: 'direct fetch failed' } },
      };
      const fetch = sinon.spy(context.driver, 'fetchResults');
      const close = sinon.spy(context.driver, 'closeOperation');
      const session = createSessionForTest({ handle: sessionHandle, context });
      try {
        const operation = await session.executeStatement('SELECT id FROM range(6)', { rowLimit: 0 });
        try {
          await assert.rejects(
            operation[method](),
            (error: unknown) => error instanceof StatusError && error.message === 'direct fetch failed',
          );
          expect(fetch.called).to.be.false;
        } finally {
          await operation.close();
        }
        expect(close.calledOnce).to.be.true;
      } finally {
        await session.close();
      }
    });
  }

  for (const disableBuffering of [false, true]) {
    for (const prefetched of [false, true]) {
      for (const rowLimit of [0, 4, 6, 9, 12, 20, undefined]) {
        it(`caps oversized Arrow pages at ${rowLimit} (disableBuffering=${disableBuffering}, prefetched=${prefetched})`, async () => {
          const context = createContext(TSparkRowSetType.ARROW_BASED_SET);
          const first = createArrowPage(0, true);
          const second = createArrowPage(6, false);
          context.driver.getResultSetMetadataResp.arrowSchema = first.schema;
          const fetch = sinon.stub(context.driver, 'fetchResults').rejects(new Error('unexpected fetch past cap'));
          if (prefetched) {
            context.driver.executeStatementResp.directResults = {
              resultSetMetadata: context.driver.getResultSetMetadataResp,
              resultSet: first.page,
            };
            fetch.onFirstCall().resolves(second.page);
          } else {
            fetch.onFirstCall().resolves(first.page);
            fetch.onSecondCall().resolves(second.page);
          }
          const session = createSessionForTest({ handle: sessionHandle, context });
          const operation = await session.executeStatement('SELECT id FROM range(12)', { rowLimit });
          const rows: object[] = [];
          while (await operation.hasMoreRows()) {
            rows.push(...(await operation.fetchChunk({ maxRows: 2, disableBuffering })));
          }
          const count = Math.min(rowLimit ?? 12, 12);
          expect(rows).to.deep.equal(Array.from({ length: count }, (_, id) => ({ id })));
          expect(await operation.hasMoreRows()).to.be.false;
          expect(await operation.fetchAll()).to.deep.equal([]);
          expect(await operation.fetchChunk()).to.deep.equal([]);
          const pages = Math.ceil(count / 6);
          expect(fetch.callCount).to.equal(Math.max(0, pages - (prefetched ? 1 : 0)));
        });
      }
    }
  }

  for (const rowLimit of [4, 6, 9]) {
    it(`does not read a failing Arrow page beyond cap ${rowLimit}`, async () => {
      const context = createContext(TSparkRowSetType.ARROW_BASED_SET);
      const first = createArrowPage(0, true);
      context.driver.getResultSetMetadataResp.arrowSchema = first.schema;
      const fetch = sinon.stub(context.driver, 'fetchResults').rejects(new Error('required page failed'));
      fetch.onFirstCall().resolves(first.page);
      const session = createSessionForTest({ handle: sessionHandle, context });
      const operation = await session.executeStatement('SELECT id FROM range(12)', { rowLimit });
      if (rowLimit > 6) {
        await assert.rejects(operation.fetchAll(), /required page failed/);
      } else {
        expect(await operation.fetchAll()).to.deep.equal(Array.from({ length: rowLimit }, (_, id) => ({ id })));
        expect(await operation.hasMoreRows()).to.be.false;
        expect(fetch.callCount).to.equal(1);
      }
    });
  }

  it('coerces a BigInt fetch-page limit when a statement cap is set', async () => {
    const context = createContext(TSparkRowSetType.ARROW_BASED_SET);
    const first = createArrowPage(0, false);
    context.driver.getResultSetMetadataResp.arrowSchema = first.schema;
    sinon.stub(context.driver, 'fetchResults').resolves(first.page);
    const session = createSessionForTest({ handle: sessionHandle, context });
    const operation = await session.executeStatement('SELECT id FROM range(6)', { rowLimit: 5 });
    expect(await operation.fetchChunk({ maxRows: BigInt(2) as unknown as number })).to.deep.equal([
      { id: 0 },
      { id: 1 },
    ]);
    expect(await operation.fetchAll()).to.deep.equal([{ id: 2 }, { id: 3 }, { id: 4 }]);
  });

  for (const rowMode of ['object', 'array'] as RowMode[]) {
    for (const disableRowMaterialization of [false, true]) {
      it(`caps Arrow rowMode=${rowMode} with disableRowMaterialization=${disableRowMaterialization}`, async () => {
        const context = createContext(TSparkRowSetType.ARROW_BASED_SET);
        context.configOverrides.disableRowMaterialization = disableRowMaterialization;
        const first = createArrowPage(0, true);
        context.driver.getResultSetMetadataResp.arrowSchema = first.schema;
        const fetch = sinon.stub(context.driver, 'fetchResults').rejects(new Error('unexpected prefetch'));
        fetch.onFirstCall().resolves(first.page);
        const session = createSessionForTest({ handle: sessionHandle, context });
        const operation = await session.executeStatement('SELECT id FROM range(6)', { rowLimit: 4, rowMode });
        expect(await operation.fetchAll()).to.deep.equal(
          Array.from({ length: 4 }, (_, id) => {
            if (disableRowMaterialization) {
              return null;
            }
            return rowMode === 'array' ? [id] : { id };
          }),
        );
        expect(fetch.callCount).to.equal(1);
      });
    }
  }

  describe('CloudFetch and staging', () => {
    let server: http.Server;
    let baseUrl: string;
    let requests: string[];

    before(async () => {
      server = http.createServer((request, response) => {
        requests.push(`${request.method} ${request.url}`);
        if (request.method === 'DELETE') {
          response.end();
        } else {
          const start = Number(request.url!.slice(1)) * 6;
          response.end(createArrowPage(start, false).ipc);
        }
      });
      await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
      baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    });

    beforeEach(() => {
      requests = [];
    });

    after(async () => {
      await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
    });

    for (const concurrency of [1, 16]) {
      for (const disableBuffering of [false, true]) {
        for (const rowLimit of [0, 4, 6, 9, 12, 20, undefined]) {
          it(`bounds CloudFetch downloads at ${rowLimit} (concurrency=${concurrency}, disableBuffering=${disableBuffering})`, async () => {
            const context = createContext(TSparkRowSetType.URL_BASED_SET);
            context.configOverrides.cloudFetchConcurrentDownloads = concurrency;
            const fetch = sinon.stub(context.driver, 'fetchResults').rejects(new Error('unexpected fetch past cap'));
            fetch.onFirstCall().resolves({
              status,
              hasMoreRows: rowLimit !== undefined && rowLimit <= 12,
              results: {
                startRowOffset: new Int64(0),
                rows: [],
                resultLinks: Array.from({ length: 3 }, (_, index) => ({
                  fileLink: `${baseUrl}/${index}`,
                  expiryTime: new Int64(Date.now() + 60000),
                  startRowOffset: new Int64(index * 6),
                  rowCount: new Int64(6),
                  bytesNum: new Int64(0),
                })),
              },
            });
            const session = createSessionForTest({ handle: sessionHandle, context });
            const operation = await session.executeStatement('SELECT id FROM range(18)', { rowLimit });
            const rows: object[] = [];
            while (await operation.hasMoreRows()) {
              rows.push(...(await operation.fetchChunk({ maxRows: 2, disableBuffering })));
            }
            const count = Math.min(rowLimit ?? 18, 18);
            expect(rows).to.deep.equal(Array.from({ length: count }, (_, id) => ({ id })));
            expect(await operation.fetchChunk()).to.deep.equal([]);
            expect(await operation.hasMoreRows()).to.be.false;
            expect(fetch.callCount).to.equal(count === 0 ? 0 : 1);
            expect(requests).to.have.members(
              Array.from({ length: Math.ceil(count / 6) }, (_, index) => `GET /${index}`),
            );
          });
        }
      }
    }

    for (const rowMode of ['object', 'array'] as RowMode[]) {
      for (const stagingAllowed of [false, true]) {
        it(`preserves internal staging instructions with rowLimit=0 (rowMode=${rowMode}, stagingAllowed=${stagingAllowed})`, async () => {
          const context = createContext(TSparkRowSetType.COLUMN_BASED_SET);
          context.driver.getResultSetMetadataResp.isStagingOperation = true;
          context.driver.getResultSetMetadataResp.schema = {
            columns: ['presignedUrl', 'operation'].map((columnName, index) => ({
              columnName,
              position: index + 1,
              typeDesc: { types: [{ primitiveEntry: { type: TTypeId.STRING_TYPE } }] },
            })),
          };
          const fetch = sinon.stub(context.driver, 'fetchResults').resolves({ status, hasMoreRows: false });
          fetch.onFirstCall().resolves({
            status,
            hasMoreRows: false,
            results: {
              startRowOffset: new Int64(0),
              rows: [],
              columns: [
                { stringVal: { values: [`${baseUrl}/staging`], nulls: Buffer.alloc(0) } },
                { stringVal: { values: ['REMOVE'], nulls: Buffer.alloc(0) } },
              ],
            },
          });
          const session = createSessionForTest({ handle: sessionHandle, context });
          const operation = await session.executeStatement('REMOVE volume_file', {
            rowLimit: 0,
            stagingAllowedLocalPath: stagingAllowed ? '/tmp' : undefined,
            rowMode,
          });
          expect(await operation.fetchAll()).to.deep.equal([]);
          expect(requests).to.deep.equal(stagingAllowed ? ['DELETE /staging'] : []);
        });
      }
    }

    for (const prefetched of [false, true]) {
      for (const rowMode of ['object', 'array'] as RowMode[]) {
        it(`reads an Arrow staging instruction when the server honors the cap (prefetched=${prefetched}, rowMode=${rowMode})`, async () => {
          const context = createContext(TSparkRowSetType.ARROW_BASED_SET);
          const metadata = context.driver.getResultSetMetadataResp;
          metadata.isStagingOperation = true;
          metadata.schema = {
            columns: ['presignedUrl', 'operation'].map((columnName, index) => ({
              columnName,
              position: index + 1,
              typeDesc: { types: [{ primitiveEntry: { type: TTypeId.STRING_TYPE } }] },
            })),
          };
          const fetch = sinon.stub(context.driver, 'fetchResults').resolves({ status, hasMoreRows: false });
          const execute = sinon.stub(context.driver, 'executeStatement').callsFake(async (request) => {
            const table = tableFromArrays({ presignedUrl: [`${baseUrl}/staging`], operation: ['REMOVE'] }).slice(
              0,
              request.resultRowLimit?.toNumber() ?? 1,
            );
            const ipc = Buffer.from(tableToIPC(table, 'stream'));
            const schemaLength = 8 + ipc.readInt32LE(4);
            metadata.arrowSchema = ipc.subarray(0, schemaLength);
            const page: TFetchResultsResp = {
              status,
              hasMoreRows: false,
              results: {
                startRowOffset: new Int64(0),
                rows: [],
                arrowBatches: [{ batch: ipc.subarray(schemaLength), rowCount: new Int64(table.numRows) }],
              },
            };
            if (prefetched) {
              context.driver.executeStatementResp.directResults = { resultSet: page, resultSetMetadata: metadata };
            } else {
              fetch.onFirstCall().resolves(page);
            }
            return context.driver.executeStatementResp;
          });
          const session = createSessionForTest({ handle: sessionHandle, context });
          try {
            const operation = await session.executeStatement('REMOVE volume_file', {
              rowLimit: 0,
              stagingAllowedLocalPath: '/tmp',
              rowMode,
            });
            expect(execute.firstCall.args[0].resultRowLimit?.toNumber()).to.equal(1);
            expect(requests).to.deep.equal(['DELETE /staging']);
            expect(await operation.fetchAll()).to.deep.equal([]);
          } finally {
            await session.close();
          }
        });
      }
    }

    for (const rowLimit of [0, 3]) {
      it(`keeps an ordinary Arrow result capped at ${rowLimit} with stagingAllowedLocalPath set`, async () => {
        const context = createContext(TSparkRowSetType.ARROW_BASED_SET);
        const fetch = sinon.stub(context.driver, 'fetchResults').resolves({ status, hasMoreRows: false });
        const execute = sinon.stub(context.driver, 'executeStatement').callsFake(async (request) => {
          const table = tableFromArrays({ id: new Int32Array([0, 1, 2, 3, 4, 5]) }).slice(
            0,
            request.resultRowLimit?.toNumber() ?? 6,
          );
          const ipc = Buffer.from(tableToIPC(table, 'stream'));
          const schemaLength = 8 + ipc.readInt32LE(4);
          context.driver.getResultSetMetadataResp.arrowSchema = ipc.subarray(0, schemaLength);
          fetch.onFirstCall().resolves({
            status,
            hasMoreRows: false,
            results: {
              startRowOffset: new Int64(0),
              rows: [],
              arrowBatches: [{ batch: ipc.subarray(schemaLength), rowCount: new Int64(table.numRows) }],
            },
          });
          return context.driver.executeStatementResp;
        });
        const session = createSessionForTest({ handle: sessionHandle, context });
        const operation = await session.executeStatement('SELECT id FROM range(6)', {
          rowLimit,
          stagingAllowedLocalPath: '/tmp',
        });
        expect(execute.firstCall.args[0].resultRowLimit?.toNumber()).to.equal(Math.max(rowLimit, 1));
        expect(await operation.fetchAll()).to.deep.equal(Array.from({ length: rowLimit }, (_, id) => ({ id })));
        expect(await operation.hasMoreRows()).to.be.false;
        expect(requests).to.deep.equal([]);
      });
    }

    it('keeps ordinary results capped when stagingAllowedLocalPath is set', async () => {
      const context = createContext(TSparkRowSetType.COLUMN_BASED_SET);
      const fetch = sinon.spy(context.driver, 'fetchResults');
      const session = createSessionForTest({ handle: sessionHandle, context });
      const operation = await session.executeStatement('SELECT 1', { rowLimit: 0, stagingAllowedLocalPath: '/tmp' });
      expect(await operation.fetchAll()).to.deep.equal([]);
      expect(fetch.called).to.be.false;
      expect(requests).to.deep.equal([]);
    });
  });
});
