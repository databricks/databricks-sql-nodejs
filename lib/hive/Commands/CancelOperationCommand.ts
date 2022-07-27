import BaseCommand from './BaseCommand';
import { TCancelOperationReq, TCancelOperationResp } from '../../../thrift/TCLIService_types';

export default class CancelOperationCommand extends BaseCommand {
  execute(data: TCancelOperationReq): Promise<TCancelOperationResp> {
    const request = new TCancelOperationReq(data);

    return this.executeCommand<TCancelOperationResp>(request, this.client.CancelOperation);
  }
}
