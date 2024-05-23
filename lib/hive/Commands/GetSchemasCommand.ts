import BaseCommand from './BaseCommand';
import { TGetSchemasReq, TGetSchemasResp } from '../../../thrift/TCLIService_types';
import IThriftClient from '../../contracts/IThriftClient';

type Client = Pick<IThriftClient, 'GetSchemas'>;

export default class GetSchemasCommand extends BaseCommand<Client> {
  execute(data: TGetSchemasReq): Promise<TGetSchemasResp> {
    const request = new TGetSchemasReq(data);

    return this.executeCommand<TGetSchemasResp>(request, this.client.GetSchemas);
  }
}
