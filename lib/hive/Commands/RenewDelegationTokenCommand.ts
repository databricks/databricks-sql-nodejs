import BaseCommand from './BaseCommand';
import { TRenewDelegationTokenReq, TRenewDelegationTokenResp } from '../../../thrift/TCLIService_types';
import IThriftClient from '../../contracts/IThriftClient';

type Client = Pick<IThriftClient, 'RenewDelegationToken'>;

export default class RenewDelegationTokenCommand extends BaseCommand<Client> {
  execute(data: TRenewDelegationTokenReq): Promise<TRenewDelegationTokenResp> {
    const request = new TRenewDelegationTokenReq(data);

    return this.executeCommand<TRenewDelegationTokenResp>(request, this.client.RenewDelegationToken);
  }
}
