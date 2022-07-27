import { TableSchema, Status, OperationHandle } from '../Types';
import BaseCommand from './BaseCommand';
import TCLIService_types from '../../../thrift/TCLIService_types';

export type GetResultSetMetadataRequest = {
  operationHandle: OperationHandle;
};

export type GetResultSetMetadataResponse = {
  status: Status;
  schema: TableSchema;
};

export default class GetResultSetMetadataCommand extends BaseCommand {
  execute(getResultSetMetadataRequest: GetResultSetMetadataRequest): Promise<GetResultSetMetadataResponse> {
    const request = new TCLIService_types.TGetResultSetMetadataReq(getResultSetMetadataRequest);

    return this.executeCommand<GetResultSetMetadataResponse>(request, this.client.GetResultSetMetadata);
  }
}
