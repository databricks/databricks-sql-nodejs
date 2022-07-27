import BaseCommand from './BaseCommand';
import { TOpenSessionReq, TOpenSessionResp } from '../../../thrift/TCLIService_types';

export default class OpenSessionCommand extends BaseCommand {
  execute(openSessionRequest: TOpenSessionReq): Promise<TOpenSessionResp> {
    const request = new TOpenSessionReq(openSessionRequest);

    return this.executeCommand<TOpenSessionResp>(request, this.client.OpenSession);
  }
}
