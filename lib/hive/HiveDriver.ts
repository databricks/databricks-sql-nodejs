import TCLIService from '../../thrift/TCLIService';
import {
  TOpenSessionReq,
  TCloseSessionReq,
  TExecuteStatementReq,
  TGetResultSetMetadataReq,
  TFetchResultsReq,
  TGetInfoReq,
  TGetTypeInfoReq,
  TGetCatalogsReq,
  TGetSchemasReq,
  TGetTablesReq,
  TGetTableTypesReq,
  TGetColumnsReq,
  TGetFunctionsReq,
  TGetPrimaryKeysReq,
  TGetCrossReferenceReq,
  TGetOperationStatusReq,
  TCancelOperationReq,
  TCloseOperationReq,
  TGetDelegationTokenReq,
  TCancelDelegationTokenReq,
  TRenewDelegationTokenReq,
} from '../../thrift/TCLIService_types';
import OpenSessionCommand from './Commands/OpenSessionCommand';
import CloseSessionCommand from './Commands/CloseSessionCommand';
import ExecuteStatementCommand from './Commands/ExecuteStatementCommand';
import GetResultSetMetadataCommand from './Commands/GetResultSetMetadataCommand';
import FetchResultsCommand from './Commands/FetchResultsCommand';
import GetInfoCommand from './Commands/GetInfoCommand';
import GetTypeInfoCommand from './Commands/GetTypeInfoCommand';
import GetCatalogsCommand from './Commands/GetCatalogsCommand';
import GetSchemasCommand from './Commands/GetSchemasCommand';
import GetTablesCommand from './Commands/GetTablesCommand';
import GetTableTypesCommand from './Commands/GetTableTypesCommand';
import GetColumnsCommand from './Commands/GetColumnsCommand';
import GetFunctionsCommand from './Commands/GetFunctionsCommand';
import GetPrimaryKeysCommand from './Commands/GetPrimaryKeysCommand';
import GetCrossReferenceCommand from './Commands/GetCrossReferenceCommand';
import GetOperationStatusCommand from './Commands/GetOperationStatusCommand';
import CancelOperationCommand from './Commands/CancelOperationCommand';
import CloseOperationCommand from './Commands/CloseOperationCommand';
import GetDelegationTokenCommand from './Commands/GetDelegationTokenCommand';
import CancelDelegationTokenCommand from './Commands/CancelDelegationTokenCommand';
import RenewDelegationTokenCommand from './Commands/RenewDelegationTokenCommand';

export default class HiveDriver {
  private client: TCLIService.Client;

  constructor(client: TCLIService.Client) {
    this.client = client;
  }

  openSession(request: TOpenSessionReq) {
    const action = new OpenSessionCommand(this.client);

    return action.execute(request);
  }

  closeSession(request: TCloseSessionReq) {
    const command = new CloseSessionCommand(this.client);

    return command.execute(request);
  }

  executeStatement(request: TExecuteStatementReq) {
    const command = new ExecuteStatementCommand(this.client);

    return command.execute(request);
  }

  getResultSetMetadata(request: TGetResultSetMetadataReq) {
    const command = new GetResultSetMetadataCommand(this.client);

    return command.execute(request);
  }

  fetchResults(request: TFetchResultsReq) {
    const command = new FetchResultsCommand(this.client);

    return command.execute(request);
  }

  getInfo(request: TGetInfoReq) {
    const command = new GetInfoCommand(this.client);

    return command.execute(request);
  }

  getTypeInfo(request: TGetTypeInfoReq) {
    const command = new GetTypeInfoCommand(this.client);

    return command.execute(request);
  }

  getCatalogs(request: TGetCatalogsReq) {
    const command = new GetCatalogsCommand(this.client);

    return command.execute(request);
  }

  getSchemas(request: TGetSchemasReq) {
    const command = new GetSchemasCommand(this.client);

    return command.execute(request);
  }

  getTables(request: TGetTablesReq) {
    const command = new GetTablesCommand(this.client);

    return command.execute(request);
  }

  getTableTypes(request: TGetTableTypesReq) {
    const command = new GetTableTypesCommand(this.client);

    return command.execute(request);
  }

  getColumns(request: TGetColumnsReq) {
    const command = new GetColumnsCommand(this.client);

    return command.execute(request);
  }

  getFunctions(request: TGetFunctionsReq) {
    const command = new GetFunctionsCommand(this.client);

    return command.execute(request);
  }

  getPrimaryKeys(request: TGetPrimaryKeysReq) {
    const command = new GetPrimaryKeysCommand(this.client);

    return command.execute(request);
  }

  getCrossReference(request: TGetCrossReferenceReq) {
    const command = new GetCrossReferenceCommand(this.client);

    return command.execute(request);
  }

  getOperationStatus(request: TGetOperationStatusReq) {
    const command = new GetOperationStatusCommand(this.client);

    return command.execute(request);
  }

  cancelOperation(request: TCancelOperationReq) {
    const command = new CancelOperationCommand(this.client);

    return command.execute(request);
  }

  closeOperation(request: TCloseOperationReq) {
    const command = new CloseOperationCommand(this.client);

    return command.execute(request);
  }

  getDelegationToken(request: TGetDelegationTokenReq) {
    const command = new GetDelegationTokenCommand(this.client);

    return command.execute(request);
  }

  cancelDelegationToken(request: TCancelDelegationTokenReq) {
    const command = new CancelDelegationTokenCommand(this.client);

    return command.execute(request);
  }

  renewDelegationToken(request: TRenewDelegationTokenReq) {
    const command = new RenewDelegationTokenCommand(this.client);

    return command.execute(request);
  }
}
