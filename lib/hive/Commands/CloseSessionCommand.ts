import { SessionHandle, Status } from '../Types';
import BaseCommand from './BaseCommand';
import TCLIService_types from '../../../thrift/TCLIService_types';

export type CloseSessionRequest = {
  sessionHandle: SessionHandle;
};

export type CloseSessionResponse = {
  status: Status;
};

export default class CloseSessionCommand extends BaseCommand {
  execute(openSessionRequest: CloseSessionRequest): Promise<CloseSessionResponse> {
    const request = new TCLIService_types.TCloseSessionReq(openSessionRequest);

    return this.executeCommand<CloseSessionResponse>(request, this.client.CloseSession);
  }
}
