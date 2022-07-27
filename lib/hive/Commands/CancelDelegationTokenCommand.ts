import BaseCommand from './BaseCommand';
import { Status, SessionHandle } from '../Types';
import TCLIService_types from '../../../thrift/TCLIService_types';

export type CancelDelegationTokenRequest = {
  sessionHandle: SessionHandle;
  delegationToken: string;
};

export type CancelDelegationTokenResponse = {
  status: Status;
};

export default class CancelDelegationTokenCommand extends BaseCommand {
  execute(data: CancelDelegationTokenRequest): Promise<CancelDelegationTokenResponse> {
    const request = new TCLIService_types.TCancelDelegationTokenReq(data);

    return this.executeCommand<CancelDelegationTokenResponse>(request, this.client.CancelDelegationToken);
  }
}
