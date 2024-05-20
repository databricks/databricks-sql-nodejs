import BaseCommand from './BaseCommand';
import { TGetDelegationTokenReq, TGetDelegationTokenResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'GetDelegationToken'>;

export default class GetDelegationTokenCommand extends BaseCommand<Client> {
  execute(data: TGetDelegationTokenReq): Promise<TGetDelegationTokenResp> {
    const request = new TGetDelegationTokenReq(data);

    return this.executeCommand<TGetDelegationTokenResp>(request, this.client.GetDelegationToken);
  }
}
