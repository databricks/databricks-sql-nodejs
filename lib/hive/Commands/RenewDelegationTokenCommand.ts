import BaseCommand from './BaseCommand';
import { Status, SessionHandle } from '../Types';
import TCLIService_types from '../../../thrift/TCLIService_types';

export type RenewDelegationTokenRequest = {
  sessionHandle: SessionHandle;
  delegationToken: string;
};

export type RenewDelegationTokenResponse = {
  status: Status;
};

export default class RenewDelegationTokenCommand extends BaseCommand {
  execute(data: RenewDelegationTokenRequest): Promise<RenewDelegationTokenResponse> {
    const request = new TCLIService_types.TRenewDelegationTokenReq(data);

    return this.executeCommand<RenewDelegationTokenResponse>(request, this.client.RenewDelegationToken);
  }
}
