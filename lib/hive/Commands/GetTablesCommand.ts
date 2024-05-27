import BaseCommand from './BaseCommand';
import { TGetTablesReq, TGetTablesResp } from '../../../thrift/TCLIService_types';
import IThriftClient from '../../contracts/IThriftClient';

type Client = Pick<IThriftClient, 'GetTables'>;

export default class GetTablesCommand extends BaseCommand<Client> {
  execute(data: TGetTablesReq): Promise<TGetTablesResp> {
    const request = new TGetTablesReq(data);

    return this.executeCommand<TGetTablesResp>(request, this.client.GetTables);
  }
}
