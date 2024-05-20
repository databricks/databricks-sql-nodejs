import BaseCommand from './BaseCommand';
import { TGetTablesReq, TGetTablesResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'GetTables'>;

export default class GetTablesCommand extends BaseCommand<Client> {
  execute(data: TGetTablesReq): Promise<TGetTablesResp> {
    const request = new TGetTablesReq(data);

    return this.executeCommand<TGetTablesResp>(request, this.client.GetTables);
  }
}
