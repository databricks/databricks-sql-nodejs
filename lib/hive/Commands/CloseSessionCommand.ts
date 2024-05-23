import BaseCommand from './BaseCommand';
import { TCloseSessionReq, TCloseSessionResp } from '../../../thrift/TCLIService_types';
import IThriftClient from '../../contracts/IThriftClient';

type Client = Pick<IThriftClient, 'CloseSession'>;

export default class CloseSessionCommand extends BaseCommand<Client> {
  execute(openSessionRequest: TCloseSessionReq): Promise<TCloseSessionResp> {
    const request = new TCloseSessionReq(openSessionRequest);

    return this.executeCommand<TCloseSessionResp>(request, this.client.CloseSession);
  }
}
