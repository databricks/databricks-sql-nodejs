import BaseCommand from './BaseCommand';
import { TGetTableTypesReq, TGetTableTypesResp } from '../../../thrift/TCLIService_types';

export default class GetTableTypesCommand extends BaseCommand {
  execute(data: TGetTableTypesReq): Promise<TGetTableTypesResp> {
    const request = new TGetTableTypesReq(data);

    return this.executeCommand<TGetTableTypesResp>(request, this.client.GetTableTypes);
  }
}
