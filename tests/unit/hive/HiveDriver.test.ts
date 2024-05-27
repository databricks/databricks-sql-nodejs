import { expect } from 'chai';
import sinon from 'sinon';
import Int64 from 'node-int64';
import HiveDriver from '../../../lib/hive/HiveDriver';
import {
  TCancelDelegationTokenReq,
  TCancelOperationReq,
  TCloseOperationReq,
  TCloseSessionReq,
  TExecuteStatementReq,
  TFetchOrientation,
  TFetchResultsReq,
  TGetCatalogsReq,
  TGetColumnsReq,
  TGetCrossReferenceReq,
  TGetDelegationTokenReq,
  TGetFunctionsReq,
  TGetInfoReq,
  TGetInfoType,
  TGetOperationStatusReq,
  TGetPrimaryKeysReq,
  TGetResultSetMetadataReq,
  TGetSchemasReq,
  TGetTablesReq,
  TGetTableTypesReq,
  TGetTypeInfoReq,
  TOperationHandle,
  TOperationType,
  TRenewDelegationTokenReq,
  TSessionHandle,
} from '../../../thrift/TCLIService_types';

import ClientContextStub from '../.stubs/ClientContextStub';

describe('HiveDriver', () => {
  const sessionHandle: TSessionHandle = { sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) } };

  const operationHandle: TOperationHandle = {
    operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
    operationType: TOperationType.UNKNOWN,
    hasResultSet: false,
  };

  it('should execute closeSession', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TCloseSessionReq = { sessionHandle };
    const response = await driver.closeSession(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.CloseSession.called).to.be.true;
    expect(new TCloseSessionReq(request)).to.deep.equal(context.thriftClient.closeSessionReq);
    expect(response).to.deep.equal(context.thriftClient.closeSessionResp);
  });

  it('should execute executeStatement', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TExecuteStatementReq = { sessionHandle, statement: 'SELECT 1' };
    const response = await driver.executeStatement(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.ExecuteStatement.called).to.be.true;
    expect(new TExecuteStatementReq(request)).to.deep.equal(context.thriftClient.executeStatementReq);
    expect(response).to.deep.equal(context.thriftClient.executeStatementResp);
  });

  it('should execute getResultSetMetadata', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TGetResultSetMetadataReq = { operationHandle };
    const response = await driver.getResultSetMetadata(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.GetResultSetMetadata.called).to.be.true;
    expect(new TGetResultSetMetadataReq(request)).to.deep.equal(context.thriftClient.getResultSetMetadataReq);
    expect(response).to.deep.equal(context.thriftClient.getResultSetMetadataResp);
  });

  it('should execute fetchResults', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TFetchResultsReq = {
      operationHandle,
      orientation: TFetchOrientation.FETCH_FIRST,
      maxRows: new Int64(1),
    };
    const response = await driver.fetchResults(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.FetchResults.called).to.be.true;
    expect(new TFetchResultsReq(request)).to.deep.equal(context.thriftClient.fetchResultsReq);
    expect(response).to.deep.equal(context.thriftClient.fetchResultsResp);
  });

  it('should execute getInfo', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TGetInfoReq = { sessionHandle, infoType: TGetInfoType.CLI_SERVER_NAME };
    const response = await driver.getInfo(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.GetInfo.called).to.be.true;
    expect(new TGetInfoReq(request)).to.deep.equal(context.thriftClient.getInfoReq);
    expect(response).to.deep.equal(context.thriftClient.getInfoResp);
  });

  it('should execute getTypeInfo', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TGetTypeInfoReq = { sessionHandle };
    const response = await driver.getTypeInfo(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.GetTypeInfo.called).to.be.true;
    expect(new TGetTypeInfoReq(request)).to.deep.equal(context.thriftClient.getTypeInfoReq);
    expect(response).to.deep.equal(context.thriftClient.getTypeInfoResp);
  });

  it('should execute getCatalogs', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TGetCatalogsReq = { sessionHandle };
    const response = await driver.getCatalogs(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.GetCatalogs.called).to.be.true;
    expect(new TGetCatalogsReq(request)).to.deep.equal(context.thriftClient.getCatalogsReq);
    expect(response).to.deep.equal(context.thriftClient.getCatalogsResp);
  });

  it('should execute getSchemas', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TGetSchemasReq = { sessionHandle, catalogName: 'catalog' };
    const response = await driver.getSchemas(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.GetSchemas.called).to.be.true;
    expect(new TGetSchemasReq(request)).to.deep.equal(context.thriftClient.getSchemasReq);
    expect(response).to.deep.equal(context.thriftClient.getSchemasResp);
  });

  it('should execute getTables', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TGetTablesReq = { sessionHandle, catalogName: 'catalog', schemaName: 'schema' };
    const response = await driver.getTables(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.GetTables.called).to.be.true;
    expect(new TGetTablesReq(request)).to.deep.equal(context.thriftClient.getTablesReq);
    expect(response).to.deep.equal(context.thriftClient.getTablesResp);
  });

  it('should execute getTableTypes', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TGetTableTypesReq = { sessionHandle };
    const response = await driver.getTableTypes(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.GetTableTypes.called).to.be.true;
    expect(new TGetTableTypesReq(request)).to.deep.equal(context.thriftClient.getTableTypesReq);
    expect(response).to.deep.equal(context.thriftClient.getTableTypesResp);
  });

  it('should execute getColumns', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TGetColumnsReq = { sessionHandle, catalogName: 'catalog', schemaName: 'schema', tableName: 'table' };
    const response = await driver.getColumns(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.GetColumns.called).to.be.true;
    expect(new TGetColumnsReq(request)).to.deep.equal(context.thriftClient.getColumnsReq);
    expect(response).to.deep.equal(context.thriftClient.getColumnsResp);
  });

  it('should execute getFunctions', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TGetFunctionsReq = {
      sessionHandle,
      catalogName: 'catalog',
      schemaName: 'schema',
      functionName: 'func',
    };
    const response = await driver.getFunctions(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.GetFunctions.called).to.be.true;
    expect(new TGetFunctionsReq(request)).to.deep.equal(context.thriftClient.getFunctionsReq);
    expect(response).to.deep.equal(context.thriftClient.getFunctionsResp);
  });

  it('should execute getPrimaryKeys', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TGetPrimaryKeysReq = { sessionHandle, catalogName: 'catalog', schemaName: 'schema' };
    const response = await driver.getPrimaryKeys(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.GetPrimaryKeys.called).to.be.true;
    expect(new TGetPrimaryKeysReq(request)).to.deep.equal(context.thriftClient.getPrimaryKeysReq);
    expect(response).to.deep.equal(context.thriftClient.getPrimaryKeysResp);
  });

  it('should execute getCrossReference', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TGetCrossReferenceReq = {
      sessionHandle,
      parentCatalogName: 'parent_catalog',
      foreignCatalogName: 'foreign_catalog',
    };
    const response = await driver.getCrossReference(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.GetCrossReference.called).to.be.true;
    expect(new TGetCrossReferenceReq(request)).to.deep.equal(context.thriftClient.getCrossReferenceReq);
    expect(response).to.deep.equal(context.thriftClient.getCrossReferenceResp);
  });

  it('should execute getOperationStatus', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TGetOperationStatusReq = { operationHandle };
    const response = await driver.getOperationStatus(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.GetOperationStatus.called).to.be.true;
    expect(new TGetOperationStatusReq(request)).to.deep.equal(context.thriftClient.getOperationStatusReq);
    expect(response).to.deep.equal(context.thriftClient.getOperationStatusResp);
  });

  it('should execute cancelOperation', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TCancelOperationReq = { operationHandle };
    const response = await driver.cancelOperation(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.CancelOperation.called).to.be.true;
    expect(new TCancelOperationReq(request)).to.deep.equal(context.thriftClient.cancelOperationReq);
    expect(response).to.deep.equal(context.thriftClient.cancelOperationResp);
  });

  it('should execute closeOperation', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TCloseOperationReq = { operationHandle };
    const response = await driver.closeOperation(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.CloseOperation.called).to.be.true;
    expect(new TCloseOperationReq(request)).to.deep.equal(context.thriftClient.closeOperationReq);
    expect(response).to.deep.equal(context.thriftClient.closeOperationResp);
  });

  it('should execute getDelegationToken', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TGetDelegationTokenReq = { sessionHandle, owner: 'owner', renewer: 'renewer' };
    const response = await driver.getDelegationToken(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.GetDelegationToken.called).to.be.true;
    expect(new TGetDelegationTokenReq(request)).to.deep.equal(context.thriftClient.getDelegationTokenReq);
    expect(response).to.deep.equal(context.thriftClient.getDelegationTokenResp);
  });

  it('should execute cancelDelegationToken', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TCancelDelegationTokenReq = { sessionHandle, delegationToken: 'token' };
    const response = await driver.cancelDelegationToken(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.CancelDelegationToken.called).to.be.true;
    expect(new TCancelDelegationTokenReq(request)).to.deep.equal(context.thriftClient.cancelDelegationTokenReq);
    expect(response).to.deep.equal(context.thriftClient.cancelDelegationTokenResp);
  });

  it('should execute renewDelegationToken', async () => {
    const context = sinon.spy(new ClientContextStub());
    const thriftClient = sinon.spy(context.thriftClient);
    const driver = new HiveDriver({ context });

    const request: TRenewDelegationTokenReq = { sessionHandle, delegationToken: 'token' };
    const response = await driver.renewDelegationToken(request);

    expect(context.getClient.called).to.be.true;
    expect(thriftClient.RenewDelegationToken.called).to.be.true;
    expect(new TRenewDelegationTokenReq(request)).to.deep.equal(context.thriftClient.renewDelegationTokenReq);
    expect(response).to.deep.equal(context.thriftClient.renewDelegationTokenResp);
  });
});
