import BaseCommand from './BaseCommand';
import { TGetSchemasReq, TGetSchemasResp } from '../../../thrift/TCLIService_types';

export default class GetSchemasCommand extends BaseCommand {
  execute(data: TGetSchemasReq): Promise<TGetSchemasResp> {
    const request = new TGetSchemasReq(data);

    return this.executeCommand<TGetSchemasResp>(request, this.client.GetSchemas);
  }
}
