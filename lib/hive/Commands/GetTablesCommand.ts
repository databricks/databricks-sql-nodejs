import BaseCommand from './BaseCommand';
import { TGetTablesReq, TGetTablesResp } from '../../../thrift/TCLIService_types';

export default class GetTablesCommand extends BaseCommand {
  execute(data: TGetTablesReq): Promise<TGetTablesResp> {
    const request = new TGetTablesReq(data);

    return this.executeCommand<TGetTablesResp>(request, this.client.GetTables);
  }
}
