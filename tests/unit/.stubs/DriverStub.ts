import Int64 from 'node-int64';
import IDriver from '../../../lib/contracts/IDriver';
import {
  TCancelDelegationTokenReq,
  TCancelDelegationTokenResp,
  TCancelOperationReq,
  TCancelOperationResp,
  TCloseOperationReq,
  TCloseOperationResp,
  TCloseSessionReq,
  TCloseSessionResp,
  TExecuteStatementReq,
  TExecuteStatementResp,
  TFetchResultsReq,
  TFetchResultsResp,
  TGetCatalogsReq,
  TGetCatalogsResp,
  TGetColumnsReq,
  TGetColumnsResp,
  TGetCrossReferenceReq,
  TGetCrossReferenceResp,
  TGetDelegationTokenReq,
  TGetDelegationTokenResp,
  TGetFunctionsReq,
  TGetFunctionsResp,
  TGetInfoReq,
  TGetInfoResp,
  TGetOperationStatusReq,
  TGetOperationStatusResp,
  TGetPrimaryKeysReq,
  TGetPrimaryKeysResp,
  TGetResultSetMetadataReq,
  TGetResultSetMetadataResp,
  TGetSchemasReq,
  TGetSchemasResp,
  TGetTablesReq,
  TGetTablesResp,
  TGetTableTypesReq,
  TGetTableTypesResp,
  TGetTypeInfoReq,
  TGetTypeInfoResp,
  TOpenSessionReq,
  TOpenSessionResp,
  TOperationState,
  TOperationType,
  TProtocolVersion,
  TRenewDelegationTokenReq,
  TRenewDelegationTokenResp,
  TSparkRowSetType,
  TStatusCode,
  TTypeId,
} from '../../../thrift/TCLIService_types';

export default class DriverStub implements IDriver {
  public openSessionReq?: TOpenSessionReq;

  public openSessionResp: TOpenSessionResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    serverProtocolVersion: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8,
  };

  public async openSession(req: TOpenSessionReq) {
    this.openSessionReq = req;
    return this.openSessionResp;
  }

  public closeSessionReq?: TCloseSessionReq;

  public closeSessionResp: TCloseSessionResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
  };

  public async closeSession(req: TCloseSessionReq) {
    this.closeSessionReq = req;
    return this.closeSessionResp;
  }

  public getInfoReq?: TGetInfoReq;

  public getInfoResp: TGetInfoResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    infoValue: { stringValue: 'test' },
  };

  public async getInfo(req: TGetInfoReq) {
    this.getInfoReq = req;
    return this.getInfoResp;
  }

  public executeStatementReq?: TExecuteStatementReq;

  public executeStatementResp: TExecuteStatementResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationHandle: {
      operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      operationType: TOperationType.EXECUTE_STATEMENT,
      hasResultSet: false,
    },
  };

  public async executeStatement(req: TExecuteStatementReq) {
    this.executeStatementReq = req;
    return this.executeStatementResp;
  }

  public getTypeInfoReq?: TGetTypeInfoReq;

  public getTypeInfoResp: TGetTypeInfoResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationHandle: {
      operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      operationType: TOperationType.EXECUTE_STATEMENT,
      hasResultSet: false,
    },
  };

  public async getTypeInfo(req: TGetTypeInfoReq) {
    this.getTypeInfoReq = req;
    return this.getTypeInfoResp;
  }

  public getCatalogsReq?: TGetCatalogsReq;

  public getCatalogsResp: TGetCatalogsResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationHandle: {
      operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      operationType: TOperationType.EXECUTE_STATEMENT,
      hasResultSet: false,
    },
  };

  public async getCatalogs(req: TGetCatalogsReq) {
    this.getCatalogsReq = req;
    return this.getCatalogsResp;
  }

  public getSchemasReq?: TGetSchemasReq;

  public getSchemasResp: TGetSchemasResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationHandle: {
      operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      operationType: TOperationType.EXECUTE_STATEMENT,
      hasResultSet: false,
    },
  };

  public async getSchemas(req: TGetSchemasReq) {
    this.getSchemasReq = req;
    return this.getSchemasResp;
  }

  public getTablesReq?: TGetTablesReq;

  public getTablesResp: TGetTablesResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationHandle: {
      operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      operationType: TOperationType.EXECUTE_STATEMENT,
      hasResultSet: false,
    },
  };

  public async getTables(req: TGetTablesReq) {
    this.getTablesReq = req;
    return this.getTablesResp;
  }

  public getTableTypesReq?: TGetTableTypesReq;

  public getTableTypesResp: TGetTableTypesResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationHandle: {
      operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      operationType: TOperationType.EXECUTE_STATEMENT,
      hasResultSet: false,
    },
  };

  public async getTableTypes(req: TGetTableTypesReq) {
    this.getTableTypesReq = req;
    return this.getTableTypesResp;
  }

  public getColumnsReq?: TGetColumnsReq;

  public getColumnsResp: TGetColumnsResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationHandle: {
      operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      operationType: TOperationType.EXECUTE_STATEMENT,
      hasResultSet: false,
    },
  };

  public async getColumns(req: TGetColumnsReq) {
    this.getColumnsReq = req;
    return this.getColumnsResp;
  }

  public getFunctionsReq?: TGetFunctionsReq;

  public getFunctionsResp: TGetFunctionsResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationHandle: {
      operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      operationType: TOperationType.EXECUTE_STATEMENT,
      hasResultSet: false,
    },
  };

  public async getFunctions(req: TGetFunctionsReq) {
    this.getFunctionsReq = req;
    return this.getFunctionsResp;
  }

  public getPrimaryKeysReq?: TGetPrimaryKeysReq;

  public getPrimaryKeysResp: TGetPrimaryKeysResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationHandle: {
      operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      operationType: TOperationType.EXECUTE_STATEMENT,
      hasResultSet: false,
    },
  };

  public async getPrimaryKeys(req: TGetPrimaryKeysReq) {
    this.getPrimaryKeysReq = req;
    return this.getPrimaryKeysResp;
  }

  public getCrossReferenceReq?: TGetCrossReferenceReq;

  public getCrossReferenceResp: TGetCrossReferenceResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationHandle: {
      operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      operationType: TOperationType.EXECUTE_STATEMENT,
      hasResultSet: false,
    },
  };

  public async getCrossReference(req: TGetCrossReferenceReq) {
    this.getCrossReferenceReq = req;
    return this.getCrossReferenceResp;
  }

  public getOperationStatusReq?: TGetOperationStatusReq;

  public getOperationStatusResp: TGetOperationStatusResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationState: TOperationState.FINISHED_STATE,
  };

  public async getOperationStatus(req: TGetOperationStatusReq) {
    this.getOperationStatusReq = req;
    return this.getOperationStatusResp;
  }

  public cancelOperationReq?: TCancelOperationReq;

  public cancelOperationResp: TCancelOperationResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
  };

  public async cancelOperation(req: TCancelOperationReq) {
    this.cancelOperationReq = req;
    return this.cancelOperationResp;
  }

  public closeOperationReq?: TCloseOperationReq;

  public closeOperationResp: TCloseOperationResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
  };

  public async closeOperation(req: TCloseOperationReq) {
    this.closeOperationReq = req;
    return this.closeOperationResp;
  }

  public getResultSetMetadataReq?: TGetResultSetMetadataReq;

  public getResultSetMetadataResp: TGetResultSetMetadataResp = {
    status: { statusCode: 0 },
    resultFormat: TSparkRowSetType.COLUMN_BASED_SET,
    schema: {
      columns: [
        {
          columnName: 'test',
          typeDesc: {
            types: [
              {
                primitiveEntry: {
                  type: TTypeId.STRING_TYPE,
                },
              },
            ],
          },
          position: 1,
          comment: '',
        },
      ],
    },
  };

  public async getResultSetMetadata(req: TGetResultSetMetadataReq) {
    this.getResultSetMetadataReq = req;
    return this.getResultSetMetadataResp;
  }

  public fetchResultsReq?: TFetchResultsReq;

  public fetchResultsResp: TFetchResultsResp = {
    status: { statusCode: 0 },
    hasMoreRows: false,
    results: {
      startRowOffset: new Int64(0),
      rows: [],
      columns: [
        {
          stringVal: { values: ['a', 'b', 'c'], nulls: Buffer.from([]) },
        },
      ],
      binaryColumns: Buffer.from([]),
      columnCount: 2,
    },
  };

  public async fetchResults(req: TFetchResultsReq) {
    this.fetchResultsReq = req;
    return this.fetchResultsResp;
  }

  public getDelegationTokenReq?: TGetDelegationTokenReq;

  public getDelegationTokenResp: TGetDelegationTokenResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    delegationToken: 'token',
  };

  public async getDelegationToken(req: TGetDelegationTokenReq) {
    this.getDelegationTokenReq = req;
    return this.getDelegationTokenResp;
  }

  public cancelDelegationTokenReq?: TCancelDelegationTokenReq;

  public cancelDelegationTokenResp: TCancelDelegationTokenResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
  };

  public async cancelDelegationToken(req: TCancelDelegationTokenReq) {
    this.cancelDelegationTokenReq = req;
    return this.cancelDelegationTokenResp;
  }

  public renewDelegationTokenReq?: TRenewDelegationTokenReq;

  public renewDelegationTokenResp: TRenewDelegationTokenResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
  };

  public async renewDelegationToken(req: TRenewDelegationTokenReq) {
    this.renewDelegationTokenReq = req;
    return this.renewDelegationTokenResp;
  }
}
