import BaseCommand from './BaseCommand';
import { Status, SessionHandle, OperationHandle } from '../Types';
import TCLIService_types from '../../../thrift/TCLIService_types';

export type GetSchemasRequest = {
  sessionHandle: SessionHandle;
  catalogName?: string;
  schemaName?: string;
};

export type GetSchemasResponse = {
  status: Status;
  operationHandle: OperationHandle;
};

export default class GetSchemasCommand extends BaseCommand {
  execute(data: GetSchemasRequest): Promise<GetSchemasResponse> {
    const request = new TCLIService_types.TGetSchemasReq(data);

    return this.executeCommand<GetSchemasResponse>(request, this.client.GetSchemas);
  }
}
