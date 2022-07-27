import BaseCommand from './BaseCommand';
import { Status, SessionHandle, OperationHandle } from '../Types';
import TCLIService_types from '../../../thrift/TCLIService_types';

export type GetTypeInfoRequest = {
  sessionHandle: SessionHandle;
};

export type GetTypeInfoResponse = {
  status: Status;
  operationHandle: OperationHandle;
};

export default class GetTypeInfoCommand extends BaseCommand {
  execute(data: GetTypeInfoRequest): Promise<GetTypeInfoResponse> {
    const request = new TCLIService_types.TGetTypeInfoReq(data);

    return this.executeCommand<GetTypeInfoResponse>(request, this.client.GetTypeInfo);
  }
}
