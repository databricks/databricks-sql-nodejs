import BaseCommand from './BaseCommand';
import { TGetOperationStatusReq, TGetOperationStatusResp } from '../../../thrift/TCLIService_types';

export default class GetOperationStatusCommand extends BaseCommand {
  execute(data: TGetOperationStatusReq): Promise<TGetOperationStatusResp> {
    const request = new TGetOperationStatusReq(data);

    return this.executeCommand<TGetOperationStatusResp>(request, this.client.GetOperationStatus);
  }
}
