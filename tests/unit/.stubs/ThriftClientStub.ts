import Int64 from 'node-int64';
import IThriftClient from '../../../lib/contracts/IThriftClient';
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

export type ThriftClientCommandCallback<R> = (error: void, resp: R) => void;

export default class ThriftClientStub implements IThriftClient {
  public openSessionReq?: TOpenSessionReq;

  public openSessionResp: TOpenSessionResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    sessionHandle: {
      sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
    },
    serverProtocolVersion: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8,
  };

  public OpenSession(req: TOpenSessionReq, callback?: ThriftClientCommandCallback<TOpenSessionResp>) {
    this.openSessionReq = req;
    callback?.(undefined, this.openSessionResp);
  }

  public closeSessionReq?: TCloseSessionReq;

  public closeSessionResp: TCloseSessionResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
  };

  public CloseSession(req: TCloseSessionReq, callback?: ThriftClientCommandCallback<TCloseSessionResp>) {
    this.closeSessionReq = req;
    callback?.(undefined, this.closeSessionResp);
  }

  public getInfoReq?: TGetInfoReq;

  public getInfoResp: TGetInfoResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    infoValue: { stringValue: 'test' },
  };

  public GetInfo(req: TGetInfoReq, callback?: ThriftClientCommandCallback<TGetInfoResp>) {
    this.getInfoReq = req;
    callback?.(undefined, this.getInfoResp);
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

  public ExecuteStatement(req: TExecuteStatementReq, callback?: ThriftClientCommandCallback<TExecuteStatementResp>) {
    this.executeStatementReq = req;
    callback?.(undefined, this.executeStatementResp);
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

  public GetTypeInfo(req: TGetTypeInfoReq, callback?: ThriftClientCommandCallback<TGetTypeInfoResp>) {
    this.getTypeInfoReq = req;
    callback?.(undefined, this.getTypeInfoResp);
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

  public GetCatalogs(req: TGetCatalogsReq, callback?: ThriftClientCommandCallback<TGetCatalogsResp>) {
    this.getCatalogsReq = req;
    callback?.(undefined, this.getCatalogsResp);
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

  public GetSchemas(req: TGetSchemasReq, callback?: ThriftClientCommandCallback<TGetSchemasResp>) {
    this.getSchemasReq = req;
    callback?.(undefined, this.getSchemasResp);
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

  public GetTables(req: TGetTablesReq, callback?: ThriftClientCommandCallback<TGetTablesResp>) {
    this.getTablesReq = req;
    callback?.(undefined, this.getTablesResp);
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

  public GetTableTypes(req: TGetTableTypesReq, callback?: ThriftClientCommandCallback<TGetTableTypesResp>) {
    this.getTableTypesReq = req;
    callback?.(undefined, this.getTableTypesResp);
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

  public GetColumns(req: TGetColumnsReq, callback?: ThriftClientCommandCallback<TGetColumnsResp>) {
    this.getColumnsReq = req;
    callback?.(undefined, this.getColumnsResp);
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

  public GetFunctions(req: TGetFunctionsReq, callback?: ThriftClientCommandCallback<TGetFunctionsResp>) {
    this.getFunctionsReq = req;
    callback?.(undefined, this.getFunctionsResp);
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

  public GetPrimaryKeys(req: TGetPrimaryKeysReq, callback?: ThriftClientCommandCallback<TGetPrimaryKeysResp>) {
    this.getPrimaryKeysReq = req;
    callback?.(undefined, this.getPrimaryKeysResp);
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

  public GetCrossReference(req: TGetCrossReferenceReq, callback?: ThriftClientCommandCallback<TGetCrossReferenceResp>) {
    this.getCrossReferenceReq = req;
    callback?.(undefined, this.getCrossReferenceResp);
  }

  public getOperationStatusReq?: TGetOperationStatusReq;

  public getOperationStatusResp: TGetOperationStatusResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationState: TOperationState.FINISHED_STATE,
  };

  public GetOperationStatus(
    req: TGetOperationStatusReq,
    callback?: ThriftClientCommandCallback<TGetOperationStatusResp>,
  ) {
    this.getOperationStatusReq = req;
    callback?.(undefined, this.getOperationStatusResp);
  }

  public cancelOperationReq?: TCancelOperationReq;

  public cancelOperationResp: TCancelOperationResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
  };

  public CancelOperation(req: TCancelOperationReq, callback?: ThriftClientCommandCallback<TCancelOperationResp>) {
    this.cancelOperationReq = req;
    callback?.(undefined, this.cancelOperationResp);
  }

  public closeOperationReq?: TCloseOperationReq;

  public closeOperationResp: TCloseOperationResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
  };

  public CloseOperation(req: TCloseOperationReq, callback?: ThriftClientCommandCallback<TCloseOperationResp>) {
    this.closeOperationReq = req;
    callback?.(undefined, this.closeOperationResp);
  }

  public getResultSetMetadataReq?: TGetResultSetMetadataReq;

  public getResultSetMetadataResp: TGetResultSetMetadataResp = {
    status: { statusCode: 0 },
    resultFormat: TSparkRowSetType.COLUMN_BASED_SET,
    schema: {
      columns: [
        {
          columnName: 'column1',
          typeDesc: {
            types: [
              {
                primitiveEntry: {
                  type: TTypeId.STRING_TYPE,
                },
              },
            ],
          },
          position: 0,
          comment: '',
        },
      ],
    },
  };

  public GetResultSetMetadata(
    req: TGetResultSetMetadataReq,
    callback?: ThriftClientCommandCallback<TGetResultSetMetadataResp>,
  ) {
    this.getResultSetMetadataReq = req;
    callback?.(undefined, this.getResultSetMetadataResp);
  }

  public fetchResultsReq?: TFetchResultsReq;

  public fetchResultsResp: TFetchResultsResp = {
    status: { statusCode: 0 },
    hasMoreRows: false,
    results: {
      startRowOffset: new Int64(0),
      rows: [
        {
          colVals: [{ boolVal: { value: true } }, { stringVal: { value: 'value' } }],
        },
      ],
      columns: [
        { boolVal: { values: [true], nulls: Buffer.from([]) } },
        { stringVal: { values: ['value'], nulls: Buffer.from([]) } },
      ],
      binaryColumns: Buffer.from([]),
      columnCount: 2,
    },
  };

  public FetchResults(req: TFetchResultsReq, callback?: ThriftClientCommandCallback<TFetchResultsResp>) {
    this.fetchResultsReq = req;
    callback?.(undefined, this.fetchResultsResp);
  }

  public getDelegationTokenReq?: TGetDelegationTokenReq;

  public getDelegationTokenResp: TGetDelegationTokenResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    delegationToken: 'token',
  };

  public GetDelegationToken(
    req: TGetDelegationTokenReq,
    callback?: ThriftClientCommandCallback<TGetDelegationTokenResp>,
  ) {
    this.getDelegationTokenReq = req;
    callback?.(undefined, this.getDelegationTokenResp);
  }

  public cancelDelegationTokenReq?: TCancelDelegationTokenReq;

  public cancelDelegationTokenResp: TCancelDelegationTokenResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
  };

  public CancelDelegationToken(
    req: TCancelDelegationTokenReq,
    callback?: ThriftClientCommandCallback<TCancelDelegationTokenResp>,
  ) {
    this.cancelDelegationTokenReq = req;
    callback?.(undefined, this.cancelDelegationTokenResp);
  }

  public renewDelegationTokenReq?: TRenewDelegationTokenReq;

  public renewDelegationTokenResp: TRenewDelegationTokenResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
  };

  public RenewDelegationToken(
    req: TRenewDelegationTokenReq,
    callback?: ThriftClientCommandCallback<TRenewDelegationTokenResp>,
  ) {
    this.renewDelegationTokenReq = req;
    callback?.(undefined, this.renewDelegationTokenResp);
  }
}
