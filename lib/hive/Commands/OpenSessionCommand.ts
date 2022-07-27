import BaseCommand from './BaseCommand';
import { TOpenSessionReq, TOpenSessionResp } from '../../../thrift/TCLIService_types';

/**
 * For auth mechanism GSSAPI the host and service should be provided when session is opened.
 *
 * TOpenSessionReq.configuration: {
 *   krb_host?: string;
 *   krb_service?: string;
 *   [key: string]: any;
 * }
 */
export default class OpenSessionCommand extends BaseCommand {
  execute(openSessionRequest: TOpenSessionReq): Promise<TOpenSessionResp> {
    const request = new TOpenSessionReq(openSessionRequest);

    return this.executeCommand<TOpenSessionResp>(request, this.client.OpenSession);
  }
}
