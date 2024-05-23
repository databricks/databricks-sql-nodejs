import BaseCommand from './BaseCommand';
import { TGetTableTypesReq, TGetTableTypesResp } from '../../../thrift/TCLIService_types';
import IThriftClient from '../../contracts/IThriftClient';

type Client = Pick<IThriftClient, 'GetTableTypes'>;

export default class GetTableTypesCommand extends BaseCommand<Client> {
  execute(data: TGetTableTypesReq): Promise<TGetTableTypesResp> {
    const request = new TGetTableTypesReq(data);

    return this.executeCommand<TGetTableTypesResp>(request, this.client.GetTableTypes);
  }
}
