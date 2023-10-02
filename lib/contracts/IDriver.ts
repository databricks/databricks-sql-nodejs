import {
  TOpenSessionReq,
  TOpenSessionResp,
  TCloseSessionReq,
  TCloseSessionResp,
  TExecuteStatementReq,
  TExecuteStatementResp,
  TGetResultSetMetadataReq,
  TGetResultSetMetadataResp,
  TFetchResultsReq,
  TFetchResultsResp,
  TGetInfoReq,
  TGetInfoResp,
  TGetTypeInfoReq,
  TGetTypeInfoResp,
  TGetCatalogsReq,
  TGetCatalogsResp,
  TGetSchemasReq,
  TGetSchemasResp,
  TGetTablesReq,
  TGetTablesResp,
  TGetTableTypesReq,
  TGetTableTypesResp,
  TGetColumnsReq,
  TGetColumnsResp,
  TGetFunctionsReq,
  TGetFunctionsResp,
  TGetPrimaryKeysReq,
  TGetPrimaryKeysResp,
  TGetCrossReferenceReq,
  TGetCrossReferenceResp,
  TGetOperationStatusReq,
  TGetOperationStatusResp,
  TCancelOperationReq,
  TCancelOperationResp,
  TCloseOperationReq,
  TCloseOperationResp,
  TGetDelegationTokenReq,
  TGetDelegationTokenResp,
  TCancelDelegationTokenReq,
  TCancelDelegationTokenResp,
  TRenewDelegationTokenReq,
  TRenewDelegationTokenResp,
} from '../../thrift/TCLIService_types';

export default interface IDriver {
  openSession(request: TOpenSessionReq): Promise<TOpenSessionResp>;

  closeSession(request: TCloseSessionReq): Promise<TCloseSessionResp>;

  executeStatement(request: TExecuteStatementReq): Promise<TExecuteStatementResp>;

  getResultSetMetadata(request: TGetResultSetMetadataReq): Promise<TGetResultSetMetadataResp>;

  fetchResults(request: TFetchResultsReq): Promise<TFetchResultsResp>;

  getInfo(request: TGetInfoReq): Promise<TGetInfoResp>;

  getTypeInfo(request: TGetTypeInfoReq): Promise<TGetTypeInfoResp>;

  getCatalogs(request: TGetCatalogsReq): Promise<TGetCatalogsResp>;

  getSchemas(request: TGetSchemasReq): Promise<TGetSchemasResp>;

  getTables(request: TGetTablesReq): Promise<TGetTablesResp>;

  getTableTypes(request: TGetTableTypesReq): Promise<TGetTableTypesResp>;

  getColumns(request: TGetColumnsReq): Promise<TGetColumnsResp>;

  getFunctions(request: TGetFunctionsReq): Promise<TGetFunctionsResp>;

  getPrimaryKeys(request: TGetPrimaryKeysReq): Promise<TGetPrimaryKeysResp>;

  getCrossReference(request: TGetCrossReferenceReq): Promise<TGetCrossReferenceResp>;

  getOperationStatus(request: TGetOperationStatusReq): Promise<TGetOperationStatusResp>;

  cancelOperation(request: TCancelOperationReq): Promise<TCancelOperationResp>;

  closeOperation(request: TCloseOperationReq): Promise<TCloseOperationResp>;

  getDelegationToken(request: TGetDelegationTokenReq): Promise<TGetDelegationTokenResp>;

  cancelDelegationToken(request: TCancelDelegationTokenReq): Promise<TCancelDelegationTokenResp>;

  renewDelegationToken(request: TRenewDelegationTokenReq): Promise<TRenewDelegationTokenResp>;
}
