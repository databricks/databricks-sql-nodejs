import { ThriftClient, TCLIServiceTypes } from './Types/';
import { OpenSessionRequest, OpenSessionResponse } from './Commands/OpenSessionCommand';
import { CloseSessionRequest, CloseSessionResponse } from './Commands/CloseSessionCommand';
import { ExecuteStatementResponse, ExecuteStatementRequest } from './Commands/ExecuteStatementCommand';
import { GetResultSetMetadataRequest, GetResultSetMetadataResponse } from './Commands/GetResultSetMetadataCommand';
import { FetchResultsRequest, FetchResultsResponse } from './Commands/FetchResultsCommand';
import { GetInfoRequest, GetInfoResponse } from './Commands/GetInfoCommand';
import { GetTypeInfoRequest, GetTypeInfoResponse } from './Commands/GetTypeInfoCommand';
import { GetCatalogsRequest, GetCatalogsResponse } from './Commands/GetCatalogsCommand';
import { GetSchemasRequest, GetSchemasResponse } from './Commands/GetSchemasCommand';
import { GetTablesRequest, GetTablesResponse } from './Commands/GetTablesCommand';
import { GetTableTypesRequest, GetTableTypesResponse } from './Commands/GetTableTypesCommand';
import { GetColumnsRequest, GetColumnsResponse } from './Commands/GetColumnsCommand';
import { GetFunctionsRequest, GetFunctionsResponse } from './Commands/GetFunctionsCommand';
import { GetPrimaryKeysRequest, GetPrimaryKeysResponse } from './Commands/GetPrimaryKeysCommand';
import { GetCrossReferenceRequest, GetCrossReferenceResponse } from './Commands/GetCrossReferenceCommand';
import { GetOperationStatusRequest, GetOperationStatusResponse } from './Commands/GetOperationStatusCommand';
import { CancelOperationRequest, CancelOperationResponse } from './Commands/CancelOperationCommand';
import { CloseOperationRequest, CloseOperationResponse } from './Commands/CloseOperationCommand';
import { GetDelegationTokenRequest, GetDelegationTokenResponse } from './Commands/GetDelegationTokenCommand';
import { CancelDelegationTokenRequest, CancelDelegationTokenResponse } from './Commands/CancelDelegationTokenCommand';
import { RenewDelegationTokenRequest, RenewDelegationTokenResponse } from './Commands/RenewDelegationTokenCommand';
import { GetQueryIdRequest, GetQueryIdResponse } from './Commands/GetQueryIdCommand';
import { SetClientInfoRequest, SetClientInfoResponse } from './Commands/SetClientInfoCommand';
export default class HiveDriver {
    private TCLIService_types;
    private client;
    constructor(TCLIService_types: TCLIServiceTypes, client: ThriftClient);
    openSession(request: OpenSessionRequest): Promise<OpenSessionResponse>;
    closeSession(request: CloseSessionRequest): Promise<CloseSessionResponse>;
    executeStatement(request: ExecuteStatementRequest): Promise<ExecuteStatementResponse>;
    getResultSetMetadata(request: GetResultSetMetadataRequest): Promise<GetResultSetMetadataResponse>;
    fetchResults(request: FetchResultsRequest): Promise<FetchResultsResponse>;
    getInfo(request: GetInfoRequest): Promise<GetInfoResponse>;
    getTypeInfo(request: GetTypeInfoRequest): Promise<GetTypeInfoResponse>;
    getCatalogs(request: GetCatalogsRequest): Promise<GetCatalogsResponse>;
    getSchemas(request: GetSchemasRequest): Promise<GetSchemasResponse>;
    getTables(request: GetTablesRequest): Promise<GetTablesResponse>;
    getTableTypes(request: GetTableTypesRequest): Promise<GetTableTypesResponse>;
    getColumns(request: GetColumnsRequest): Promise<GetColumnsResponse>;
    getFunctions(request: GetFunctionsRequest): Promise<GetFunctionsResponse>;
    getPrimaryKeys(request: GetPrimaryKeysRequest): Promise<GetPrimaryKeysResponse>;
    getCrossReference(request: GetCrossReferenceRequest): Promise<GetCrossReferenceResponse>;
    getOperationStatus(request: GetOperationStatusRequest): Promise<GetOperationStatusResponse>;
    cancelOperation(request: CancelOperationRequest): Promise<CancelOperationResponse>;
    closeOperation(request: CloseOperationRequest): Promise<CloseOperationResponse>;
    getDelegationToken(request: GetDelegationTokenRequest): Promise<GetDelegationTokenResponse>;
    cancelDelegationToken(request: CancelDelegationTokenRequest): Promise<CancelDelegationTokenResponse>;
    renewDelegationToken(request: RenewDelegationTokenRequest): Promise<RenewDelegationTokenResponse>;
    getQueryId(request: GetQueryIdRequest): Promise<GetQueryIdResponse>;
    setClientInfo(request: SetClientInfoRequest): Promise<SetClientInfoResponse>;
}
