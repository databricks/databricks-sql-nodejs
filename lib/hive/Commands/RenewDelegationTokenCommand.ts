import BaseCommand from './BaseCommand';
import { TRenewDelegationTokenReq, TRenewDelegationTokenResp } from '../../../thrift/TCLIService_types';

export default class RenewDelegationTokenCommand extends BaseCommand {
  execute(data: TRenewDelegationTokenReq): Promise<TRenewDelegationTokenResp> {
    const request = new TRenewDelegationTokenReq(data);

    return this.executeCommand<TRenewDelegationTokenResp>(request, this.client.RenewDelegationToken);
  }
}
