import assert from 'node:assert/strict';
import { expect } from 'chai';
import Int64 from 'node-int64';
import sinon from 'sinon';
import {
  TGetOperationStatusResp,
  TOperationState,
  TOperationType,
  TSparkRowSetType,
  TStatusCode,
  TTypeId,
} from '../../../thrift/TCLIService_types';
import IOperation from '../../../lib/contracts/IOperation';
import DBSQLOperation from '../../../lib/DBSQLOperation';
import { PrimaryKeysRequest } from '../../../lib/contracts/IDBSQLSession';
import OperationStateError, { OperationStateErrorCode } from '../../../lib/errors/OperationStateError';
import StatusError from '../../../lib/errors/StatusError';
import ClientContextStub from '../.stubs/ClientContextStub';
import { createSessionForTest } from '../.stubs/createSessionForTest';

const request = { schemaName: 'missing_schema', tableName: 'table' };
const expectedColumns = [
  ['TABLE_CAT', TTypeId.STRING_TYPE],
  ['TABLE_SCHEM', TTypeId.STRING_TYPE],
  ['TABLE_NAME', TTypeId.STRING_TYPE],
  ['COLUMN_NAME', TTypeId.STRING_TYPE],
  ['KEY_SEQ', TTypeId.INT_TYPE],
  ['PK_NAME', TTypeId.STRING_TYPE],
];

function createContext(): ClientContextStub {
  const context = new ClientContextStub();
  context.driver.getPrimaryKeysResp.operationHandle!.hasResultSet = true;
  context.driver.getPrimaryKeysResp.operationHandle!.operationType = TOperationType.GET_FUNCTIONS;
  return context;
}

function createSession(context: ClientContextStub) {
  return createSessionForTest({
    handle: { sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) } },
    context,
  });
}

function missingSchemaStatus(): TGetOperationStatusResp {
  return {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationState: TOperationState.ERROR_STATE,
    sqlState: '42704',
    errorMessage: '[SCHEMA_NOT_FOUND] The schema cannot be found',
    hasResultSet: false,
  };
}

async function expectEmptyPrimaryKeys(operation: IOperation): Promise<void> {
  const schema = await operation.getSchema();
  expect(
    schema?.columns.map((column) => [column.columnName, column.typeDesc.types[0].primitiveEntry?.type]),
  ).to.deep.equal(expectedColumns);
  expect(schema?.columns.map((column) => column.position)).to.deep.equal([1, 2, 3, 4, 5, 6]);
  await operation.finished();
  expect(await operation.fetchAll()).to.deep.equal([]);
  expect(await operation.hasMoreRows()).to.be.false;
  const metadata = await (operation as DBSQLOperation).getMetadata();
  expect(metadata.status.statusCode).to.equal(TStatusCode.SUCCESS_STATUS);
  expect(metadata.resultFormat).to.equal(TSparkRowSetType.COLUMN_BASED_SET);
  const status = await operation.status();
  expect(status.operationState).to.equal(TOperationState.FINISHED_STATE);
  expect(status.status.statusCode).to.equal(TStatusCode.SUCCESS_STATUS);
  expect(status.hasResultSet).to.be.true;
  expect(status.sqlState).to.be.undefined;
  expect(status.errorMessage).to.be.undefined;
}

describe('Thrift getPrimaryKeys missing objects', () => {
  afterEach(() => sinon.restore());

  const notFoundErrors = [
    { sqlState: '42704', errorMessage: 'Object is missing' },
    { sqlState: 'HY000', errorMessage: '[NO_SUCH_CATALOG_EXCEPTION] Missing catalog' },
    { sqlState: 'HY000', errorMessage: '[SCHEMA_NOT_FOUND] Missing schema' },
    { sqlState: '42P01', errorMessage: '[TABLE_OR_VIEW_NOT_FOUND] Missing table' },
    { sqlState: '22023', errorMessage: '[INVALID_PARAMETER_VALUE] Invalid catalog' },
    { sqlState: '22023', errorMessage: '[INVALID_PARAMETER_VALUE.LENGTH] Identifier is too long' },
    { sqlState: '42501', errorMessage: '[INSUFFICIENT_PERMISSIONS] Wrapped cause: SCHEMA_NOT_FOUND' },
  ];

  for (const error of notFoundErrors) {
    for (const delivery of ['initial', 'direct', 'poll']) {
      it(`returns a typed empty result for ${error.errorMessage} via ${delivery}`, async () => {
        const context = createContext();
        const driver = sinon.spy(context.driver);
        const failure = { ...missingSchemaStatus(), ...error };
        if (delivery === 'initial') {
          context.driver.getPrimaryKeysResp = {
            status: { statusCode: TStatusCode.ERROR_STATUS, ...error },
          };
        } else if (delivery === 'direct') {
          context.driver.getPrimaryKeysResp.directResults = {
            operationStatus: failure,
            resultSetMetadata: context.driver.getResultSetMetadataResp,
            resultSet: context.driver.fetchResultsResp,
          };
        } else {
          context.driver.getOperationStatusResp = failure;
        }
        const operation = await createSession(context).getPrimaryKeys(request);

        await expectEmptyPrimaryKeys(operation);
        expect(driver.getResultSetMetadata.called).to.be.false;
        expect(driver.fetchResults.called).to.be.false;
        expect(driver.getOperationStatus.callCount).to.equal(delivery === 'poll' ? 1 : 0);
        await operation.close();
        await operation.close();
        expect(driver.closeOperation.callCount).to.equal(delivery === 'initial' ? 0 : 1);
      });
    }
  }

  for (const delivery of ['initial', 'direct', 'poll'] as const) {
    it(`isolates schema mutations from ${delivery} fallback results across sessions`, async () => {
      const contexts = { initial: createContext(), direct: createContext(), poll: createContext() };
      contexts.initial.driver.getPrimaryKeysResp = {
        status: { statusCode: TStatusCode.ERROR_STATUS, sqlState: '42704' },
      };
      contexts.direct.driver.getPrimaryKeysResp.directResults = { operationStatus: missingSchemaStatus() };
      contexts.poll.driver.getOperationStatusResp = missingSchemaStatus();
      const operation = await createSession(contexts[delivery]).getPrimaryKeys(request);
      const schema = await operation.getSchema();
      assert.ok(schema);

      schema.columns[0].columnName = 'renamed';
      schema.columns[0].typeDesc.types[0].primitiveEntry!.type = TTypeId.BOOLEAN_TYPE;
      schema.columns.pop();

      await Promise.all(
        Object.values(contexts).map(async (context) => {
          const nextOperation = await createSession(context).getPrimaryKeys(request);
          await expectEmptyPrimaryKeys(nextOperation);
        }),
      );
    });
  }

  it('closes a real handle returned with an initial error', async () => {
    const context = createContext();
    context.driver.getPrimaryKeysResp.status = {
      statusCode: TStatusCode.ERROR_STATUS,
      sqlState: '42704',
    };
    const closeOperation = sinon.spy(context.driver, 'closeOperation');
    const operation = await createSession(context).getPrimaryKeys(request);

    await expectEmptyPrimaryKeys(operation);
    await operation.close();
    expect(
      closeOperation.calledOnceWithExactly({
        operationHandle: context.driver.getPrimaryKeysResp.operationHandle!,
      }),
    ).to.be.true;
  });

  it('uses a prefetched close response after recovering direct results', async () => {
    const context = createContext();
    context.driver.getPrimaryKeysResp.directResults = {
      operationStatus: missingSchemaStatus(),
      closeOperation: context.driver.closeOperationResp,
    };
    const closeOperation = sinon.spy(context.driver, 'closeOperation');
    const operation = await createSession(context).getPrimaryKeys(request);

    await expectEmptyPrimaryKeys(operation);
    expect((await operation.close()).isSuccess).to.be.true;
    expect(closeOperation.called).to.be.false;
  });

  it('polls asynchronously and reports recovered success to callbacks', async () => {
    const context = createContext();
    const poll = sinon.stub(context.driver, 'getOperationStatus').resolves(missingSchemaStatus());
    poll.onFirstCall().resolves({
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
      operationState: TOperationState.RUNNING_STATE,
    });
    const operation = await createSession(context).getPrimaryKeys(request);
    expect(poll.called).to.be.false;
    const callback = sinon.spy();

    await operation.finished({ callback });
    expect(callback.args.map(([status]) => status.operationState)).to.deep.equal([
      TOperationState.RUNNING_STATE,
      TOperationState.FINISHED_STATE,
    ]);
    await expectEmptyPrimaryKeys(operation);
  });

  it('recovers a not-found RPC status without an operation state', async () => {
    const context = createContext();
    context.driver.getOperationStatusResp = {
      status: { statusCode: TStatusCode.ERROR_STATUS, errorMessage: '[SCHEMA_NOT_FOUND] Missing schema' },
    };
    const operation = await createSession(context).getPrimaryKeys(request);

    await expectEmptyPrimaryKeys(operation);
  });

  it('waits for metadata even when the initial handle has no result set', async () => {
    const context = createContext();
    context.driver.getPrimaryKeysResp.operationHandle!.hasResultSet = false;
    context.driver.getOperationStatusResp = missingSchemaStatus();
    const operation = await createSession(context).getPrimaryKeys(request);

    await expectEmptyPrimaryKeys(operation);
  });

  it('preserves an unrelated wrapped RPC error without an operation state', async () => {
    const context = createContext();
    const status = {
      statusCode: TStatusCode.ERROR_STATUS,
      sqlState: '22023',
      errorMessage: '[INTERNAL_ERROR] Wrapped cause: Invalid parameter value for tableName',
    };
    context.driver.getOperationStatusResp = { status };
    const operation = await createSession(context).getPrimaryKeys(request);

    await assert.rejects(operation.fetchAll(), (error) => {
      expect(error).to.be.instanceOf(StatusError);
      expect(error).to.have.property('sqlState', status.sqlState);
      expect(error).to.have.property('message', status.errorMessage);
      return true;
    });
  });

  for (const delivery of ['initial', 'direct', 'poll']) {
    it(`can cancel after converting a ${delivery} failure to an empty result`, async () => {
      const context = createContext();
      const driver = sinon.spy(context.driver);
      if (delivery === 'initial') {
        context.driver.getPrimaryKeysResp = {
          status: { statusCode: TStatusCode.ERROR_STATUS, sqlState: '42704' },
        };
      } else if (delivery === 'direct') {
        context.driver.getPrimaryKeysResp.directResults = { operationStatus: missingSchemaStatus() };
      } else {
        context.driver.getOperationStatusResp = missingSchemaStatus();
      }
      const operation = await createSession(context).getPrimaryKeys(request);
      await expectEmptyPrimaryKeys(operation);

      expect((await operation.cancel()).isSuccess).to.be.true;
      expect((await operation.cancel()).isSuccess).to.be.true;
      expect(driver.cancelOperation.callCount).to.equal(delivery === 'initial' ? 0 : 1);
      expect(await operation.hasMoreRows()).to.be.false;
      await assert.rejects(operation.fetchAll(), (error) => {
        expect(error).to.be.instanceOf(OperationStateError);
        expect(error).to.have.property('errorCode', OperationStateErrorCode.Canceled);
        return true;
      });
    });
  }

  it('does not recover invalid-handle RPC errors', async () => {
    const context = createContext();
    context.driver.getOperationStatusResp = {
      ...missingSchemaStatus(),
      status: { statusCode: TStatusCode.INVALID_HANDLE_STATUS, errorMessage: 'Invalid handle' },
    };
    const operation = await createSession(context).getPrimaryKeys(request);

    await assert.rejects(operation.finished(), (error) => {
      expect(error).to.be.instanceOf(StatusError);
      expect(error).to.have.property('message', 'Invalid handle');
      return true;
    });
  });

  it('recognizes a not-found error in displayMessage', async () => {
    const context = createContext();
    context.driver.getOperationStatusResp = {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
      operationState: TOperationState.ERROR_STATE,
      displayMessage: '[SCHEMA_NOT_FOUND] Missing schema',
    };
    const operation = await createSession(context).getPrimaryKeys(request);

    await expectEmptyPrimaryKeys(operation);
  });

  for (const sqlState of ['42501', '28000', 'HYT00', '22023']) {
    it(`preserves unrelated failures with SQLSTATE ${sqlState}`, async () => {
      const context = createContext();
      const errorStatus = { statusCode: TStatusCode.ERROR_STATUS, sqlState, errorMessage: 'Unrelated failure' };
      context.driver.getPrimaryKeysResp.status = errorStatus;
      const session = createSession(context);
      await assert.rejects(session.getPrimaryKeys(request), (error) => {
        expect(error).to.be.instanceOf(StatusError);
        expect(error).to.have.property('sqlState', sqlState);
        return true;
      });
      context.driver.getPrimaryKeysResp.status = { statusCode: TStatusCode.SUCCESS_STATUS };
      const failure = { ...missingSchemaStatus(), sqlState, errorMessage: 'Unrelated failure' };
      context.driver.getOperationStatusResp = failure;
      const operation = await session.getPrimaryKeys(request);
      await assert.rejects(operation.fetchAll(), (error) => {
        expect(error).to.be.instanceOf(OperationStateError);
        expect(error).to.have.property('response', failure);
        return true;
      });
    });
  }

  it('preserves a terminal error observed before result access', async () => {
    const context = createContext();
    const failure = { ...missingSchemaStatus(), sqlState: '42501', errorMessage: 'Permission denied' };
    context.driver.getOperationStatusResp = failure;
    const operation = await createSession(context).getPrimaryKeys(request);

    expect((await operation.status()).operationState).to.equal(TOperationState.ERROR_STATE);
    for (const method of ['getSchema', 'fetchChunk', 'hasMoreRows'] as const) {
      // eslint-disable-next-line no-await-in-loop
      await assert.rejects(operation[method](), OperationStateError);
    }
  });

  for (const [operationState, errorCode] of [
    [TOperationState.CANCELED_STATE, OperationStateErrorCode.Canceled],
    [TOperationState.CLOSED_STATE, OperationStateErrorCode.Closed],
    [TOperationState.TIMEDOUT_STATE, OperationStateErrorCode.Timeout],
  ] as const) {
    it(`preserves ${TOperationState[operationState]} even with a not-found message`, async () => {
      const context = createContext();
      context.driver.getOperationStatusResp = { ...missingSchemaStatus(), operationState };
      const operation = await createSession(context).getPrimaryKeys(request);

      await assert.rejects(operation.finished(), (error) => {
        expect(error).to.be.instanceOf(OperationStateError);
        expect(error).to.have.property('errorCode', errorCode);
        return true;
      });
    });
  }

  for (const method of ['executeStatement', 'getTables']) {
    it(`continues throwing missing-object errors for ${method}`, async () => {
      const context = createContext();
      context.driver.getOperationStatusResp = missingSchemaStatus();
      const session = createSession(context);
      const operation = await (method === 'executeStatement'
        ? session.executeStatement('SELECT * FROM missing_schema.table')
        : session.getTables(request));

      await assert.rejects(operation.finished(), OperationStateError);
    });
  }

  for (const catalogName of [undefined, null, '', 'catalog']) {
    it(`preserves real primary keys with catalog=${JSON.stringify(catalogName)}`, async () => {
      const context = createContext();
      context.driver.getResultSetMetadataResp.schema = {
        columns: expectedColumns.map(([name, type], index) => ({
          columnName: name === 'KEY_SEQ' ? 'KEQ_SEQ' : String(name),
          typeDesc: { types: [{ primitiveEntry: { type: type as TTypeId } }] },
          position: index + 1,
        })),
      };
      const session = createSession(context);
      const operation = await session.getPrimaryKeys({ ...request, catalogName } as PrimaryKeysRequest);

      // The successful server response is authoritative, including its wire metadata.
      const metadata = context.driver.getResultSetMetadataResp;
      const fetch = sinon.stub(context.driver, 'fetchResults').resolves({ status: metadata.status });
      fetch.onFirstCall().resolves({
        status: metadata.status,
        results: {
          startRowOffset: new Int64(0),
          rows: [],
          columns: [
            { stringVal: { values: ['catalog'], nulls: Buffer.alloc(0) } },
            { stringVal: { values: ['schema'], nulls: Buffer.alloc(0) } },
            { stringVal: { values: ['table'], nulls: Buffer.alloc(0) } },
            { stringVal: { values: ['id'], nulls: Buffer.alloc(0) } },
            { i32Val: { values: [1], nulls: Buffer.alloc(0) } },
            { stringVal: { values: ['pk_table'], nulls: Buffer.alloc(0) } },
          ],
        },
      });

      expect(await operation.getSchema()).to.equal(metadata.schema);
      expect(await operation.fetchAll()).to.deep.equal([
        {
          TABLE_CAT: 'catalog',
          TABLE_SCHEM: 'schema',
          TABLE_NAME: 'table',
          COLUMN_NAME: 'id',
          KEQ_SEQ: 1,
          PK_NAME: 'pk_table',
        },
      ]);
      expect(await (operation as DBSQLOperation).getMetadata()).to.equal(metadata);
      expect(context.driver.getPrimaryKeysReq?.catalogName).to.equal(catalogName);
    });
  }

  it('preserves a successful empty server response', async () => {
    const context = createContext();
    context.driver.fetchResultsResp.results = { startRowOffset: new Int64(0), rows: [], columns: [] };
    const operation = await createSession(context).getPrimaryKeys(request);

    expect(await operation.fetchAll()).to.deep.equal([]);
    expect(await operation.getSchema()).to.equal(context.driver.getResultSetMetadataResp.schema);
  });
});
