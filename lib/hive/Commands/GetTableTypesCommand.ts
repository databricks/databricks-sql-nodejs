import BaseCommand from './BaseCommand';
import { TGetTableTypesReq, TGetTableTypesResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'GetTableTypes'>;

export default class GetTableTypesCommand extends BaseCommand<Client> {
  execute(data: TGetTableTypesReq): Promise<TGetTableTypesResp> {
    const request = new TGetTableTypesReq(data);

    return this.executeCommand<TGetTableTypesResp>(request, this.client.GetTableTypes);
  }
}
