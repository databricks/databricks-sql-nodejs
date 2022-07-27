import BaseCommand from './BaseCommand';
import { Status, OperationHandle } from '../Types';
import TCLIService_types from '../../../thrift/TCLIService_types';

export type CancelOperationRequest = {
  operationHandle: OperationHandle;
};

export type CancelOperationResponse = {
  status: Status;
};

export default class CancelOperationCommand extends BaseCommand {
  execute(data: CancelOperationRequest): Promise<CancelOperationResponse> {
    const request = new TCLIService_types.TCancelOperationReq(data);

    return this.executeCommand<CancelOperationResponse>(request, this.client.CancelOperation);
  }
}
