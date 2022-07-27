import BaseCommand from './BaseCommand';
import { TCloseSessionReq, TCloseSessionResp } from '../../../thrift/TCLIService_types';

export default class CloseSessionCommand extends BaseCommand {
  execute(openSessionRequest: TCloseSessionReq): Promise<TCloseSessionResp> {
    const request = new TCloseSessionReq(openSessionRequest);

    return this.executeCommand<TCloseSessionResp>(request, this.client.CloseSession);
  }
}
