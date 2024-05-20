import BaseCommand from './BaseCommand';
import { TCancelDelegationTokenReq, TCancelDelegationTokenResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'CancelDelegationToken'>;

export default class CancelDelegationTokenCommand extends BaseCommand<Client> {
  execute(data: TCancelDelegationTokenReq): Promise<TCancelDelegationTokenResp> {
    const request = new TCancelDelegationTokenReq(data);

    return this.executeCommand<TCancelDelegationTokenResp>(request, this.client.CancelDelegationToken);
  }
}
