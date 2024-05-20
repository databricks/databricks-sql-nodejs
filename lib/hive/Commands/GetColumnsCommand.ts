import BaseCommand from './BaseCommand';
import { TGetColumnsReq, TGetColumnsResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'GetColumns'>;

export default class GetColumnsCommand extends BaseCommand<Client> {
  execute(data: TGetColumnsReq): Promise<TGetColumnsResp> {
    const request = new TGetColumnsReq(data);

    return this.executeCommand<TGetColumnsResp>(request, this.client.GetColumns);
  }
}
