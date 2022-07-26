import BaseCommand from './BaseCommand';
import { Status, SessionHandle, OperationHandle } from '../Types';
import TCLIService_types from '../../../thrift/TCLIService_types';

export type GetCrossReferenceRequest = {
  sessionHandle: SessionHandle;
  parentCatalogName?: string;
  parentSchemaName: string;
  parentTableName: string;
  foreignCatalogName?: string;
  foreignSchemaName: string;
  foreignTableName: string;
};

export type GetCrossReferenceResponse = {
  status: Status;
  operationHandle: OperationHandle;
};

export default class GetCrossReferenceCommand extends BaseCommand {
  execute(data: GetCrossReferenceRequest): Promise<GetCrossReferenceResponse> {
    const request = new TCLIService_types.TGetCrossReferenceReq(data);

    return this.executeCommand<GetCrossReferenceResponse>(request, this.client.GetCrossReference);
  }
}
