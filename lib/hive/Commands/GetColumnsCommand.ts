import BaseCommand from './BaseCommand';
import { TGetColumnsReq, TGetColumnsResp } from '../../../thrift/TCLIService_types';
import IThriftClient from '../../contracts/IThriftClient';

type Client = Pick<IThriftClient, 'GetColumns'>;

export default class GetColumnsCommand extends BaseCommand<Client> {
  execute(data: TGetColumnsReq): Promise<TGetColumnsResp> {
    const request = new TGetColumnsReq(data);

    return this.executeCommand<TGetColumnsResp>(request, this.client.GetColumns);
  }
}
