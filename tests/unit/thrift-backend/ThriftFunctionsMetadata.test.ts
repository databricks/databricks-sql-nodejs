import { expect } from 'chai';
import { tableFromArrays, tableToIPC } from 'apache-arrow';
import Int64 from 'node-int64';
import sinon, { SinonStub } from 'sinon';
import { TOperationType, TSparkRowSetType, TSessionHandle, TTypeId } from '../../../thrift/TCLIService_types';
import ClientContextStub from '../.stubs/ClientContextStub';
import { createSessionForTest } from '../.stubs/createSessionForTest';

const sessionHandle: TSessionHandle = {
  sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
};

function createContext(resultFormat: TSparkRowSetType, disableRowMaterialization = false): ClientContextStub {
  const context = new ClientContextStub({ disableRowMaterialization });
  context.driver.getFunctionsResp.operationHandle!.hasResultSet = true;
  context.driver.getFunctionsResp.operationHandle!.operationType = TOperationType.GET_FUNCTIONS;
  context.driver.getResultSetMetadataResp.resultFormat = resultFormat;
  context.driver.getResultSetMetadataResp.schema = {
    columns: ['FUNCTION_CAT', 'FUNCTION_NAME'].map((columnName, index) => ({
      columnName,
      typeDesc: { types: [{ primitiveEntry: { type: TTypeId.STRING_TYPE } }] },
      position: index + 1,
      comment: '',
    })),
  };

  if (resultFormat === TSparkRowSetType.ARROW_BASED_SET) {
    const table = tableFromArrays({
      FUNCTION_CAT: ['', 'server_catalog'],
      FUNCTION_NAME: ['avg', 'sum'],
    });
    const ipc = Buffer.from(tableToIPC(table, 'stream'));
    // Thrift sends the schema IPC message separately from the record batches.
    const schemaLength = 8 + ipc.readInt32LE(4);
    context.driver.getResultSetMetadataResp.arrowSchema = ipc.subarray(0, schemaLength);
    context.driver.fetchResultsResp.results = {
      startRowOffset: new Int64(0),
      rows: [],
      arrowBatches: [{ batch: ipc.subarray(schemaLength), rowCount: new Int64(2) }],
    };
  } else {
    context.driver.fetchResultsResp.results = {
      startRowOffset: new Int64(0),
      rows: [],
      columns: [
        { stringVal: { values: ['', 'server_catalog'], nulls: Buffer.alloc(0) } },
        { stringVal: { values: ['avg', 'sum'], nulls: Buffer.alloc(0) } },
      ],
    };
  }

  sinon
    .stub(context.driver, 'fetchResults')
    .resolves({ status: context.driver.fetchResultsResp.status, hasMoreRows: false })
    .onFirstCall()
    .callsFake(async () => context.driver.fetchResultsResp);

  return context;
}

describe('Thrift getFunctions FUNCTION_CAT', () => {
  afterEach(() => sinon.restore());

  [TSparkRowSetType.COLUMN_BASED_SET, TSparkRowSetType.ARROW_BASED_SET].forEach((resultFormat) => {
    describe(TSparkRowSetType[resultFormat], () => {
      for (const directResults of [false, true]) {
        for (const catalogName of [
          undefined,
          '',
          'comparator_tests',
          'COMPARATOR-TESTS',
          '%',
          'nonexistent',
          'compar\\_tests',
        ]) {
          it(`preserves catalog ${JSON.stringify(catalogName)} with directResults=${directResults}`, async () => {
            const context = createContext(resultFormat);
            const fetchResults = context.driver.fetchResults as SinonStub;
            if (directResults) {
              context.driver.getFunctionsResp.directResults = {
                resultSetMetadata: context.driver.getResultSetMetadataResp,
                resultSet: context.driver.fetchResultsResp,
              };
              fetchResults
                .onFirstCall()
                .resolves({ status: context.driver.fetchResultsResp.status, hasMoreRows: false });
            }
            const session = createSessionForTest({ handle: sessionHandle, context });
            const operation = await session.getFunctions({ catalogName, functionName: '%' });

            expect(await operation.fetchChunk({ maxRows: 1 })).to.deep.equal([
              { FUNCTION_CAT: catalogName ?? null, FUNCTION_NAME: 'avg' },
            ]);
            expect(await operation.fetchChunk({ maxRows: 1 })).to.deep.equal([
              { FUNCTION_CAT: catalogName ?? null, FUNCTION_NAME: 'sum' },
            ]);
            expect(await operation.fetchChunk({ maxRows: 1 })).to.deep.equal([]);
            expect(await operation.hasMoreRows()).to.be.false;
            expect(await operation.getSchema()).to.deep.equal(context.driver.getResultSetMetadataResp.schema);
            const expectedFetches = resultFormat === TSparkRowSetType.COLUMN_BASED_SET ? 2 : 1;
            expect(fetchResults.callCount).to.equal(expectedFetches - (directResults ? 1 : 0));
          });
        }
      }

      it('preserves the catalog captured before the Thrift request completes', async () => {
        const context = createContext(resultFormat);
        const request = { catalogName: 'original_catalog', functionName: '%' };
        sinon.stub(context.driver, 'getFunctions').callsFake(async () => {
          request.catalogName = 'changed_catalog';
          return context.driver.getFunctionsResp;
        });
        const session = createSessionForTest({ handle: sessionHandle, context });
        const operation = await session.getFunctions(request);

        expect(await operation.fetchAll()).to.deep.equal([
          { FUNCTION_CAT: 'original_catalog', FUNCTION_NAME: 'avg' },
          { FUNCTION_CAT: 'original_catalog', FUNCTION_NAME: 'sum' },
        ]);
      });

      it('does not rewrite a FUNCTION_CAT column in ordinary query results', async () => {
        const context = createContext(resultFormat);
        context.driver.executeStatementResp.operationHandle!.hasResultSet = true;
        const session = createSessionForTest({ handle: sessionHandle, context });
        const operation = await session.executeStatement('SELECT FUNCTION_CAT, FUNCTION_NAME FROM functions');

        expect(await operation.fetchAll()).to.deep.equal([
          { FUNCTION_CAT: '', FUNCTION_NAME: 'avg' },
          { FUNCTION_CAT: 'server_catalog', FUNCTION_NAME: 'sum' },
        ]);
      });

      it('returns no rows for an empty function result', async () => {
        const context = createContext(resultFormat);
        context.driver.fetchResultsResp.results = {
          startRowOffset: new Int64(0),
          rows: [],
          columns: [
            { stringVal: { values: [], nulls: Buffer.alloc(0) } },
            { stringVal: { values: [], nulls: Buffer.alloc(0) } },
          ],
          arrowBatches: [],
        };
        const session = createSessionForTest({ handle: sessionHandle, context });
        const operation = await session.getFunctions({ catalogName: 'catalog', functionName: '%' });

        expect(await operation.fetchAll()).to.deep.equal([]);
      });
    });
  });

  it('preserves null row placeholders when Arrow materialization is disabled', async () => {
    const context = createContext(TSparkRowSetType.ARROW_BASED_SET, true);
    const session = createSessionForTest({ handle: sessionHandle, context });
    const operation = await session.getFunctions({ catalogName: 'catalog', functionName: '%' });

    expect(await operation.fetchAll()).to.deep.equal([null, null]);
  });
});
