import BaseCommand from './BaseCommand';
import { TGetInfoReq, TGetInfoResp } from '../../../thrift/TCLIService_types';
import IThriftClient from '../../contracts/IThriftClient';

type Client = Pick<IThriftClient, 'GetInfo'>;

export default class GetInfoCommand extends BaseCommand<Client> {
  execute(data: TGetInfoReq): Promise<TGetInfoResp> {
    const request = new TGetInfoReq(data);

    return this.executeCommand<TGetInfoResp>(request, this.client.GetInfo);
  }
}
