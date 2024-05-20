import BaseCommand from './BaseCommand';
import { TCancelOperationReq, TCancelOperationResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'CancelOperation'>;

export default class CancelOperationCommand extends BaseCommand<Client> {
  execute(data: TCancelOperationReq): Promise<TCancelOperationResp> {
    const request = new TCancelOperationReq(data);

    return this.executeCommand<TCancelOperationResp>(request, this.client.CancelOperation);
  }
}
