import BaseCommand from './BaseCommand';
import { TGetCatalogsReq, TGetCatalogsResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'GetCatalogs'>;

export default class GetCatalogsCommand extends BaseCommand<Client> {
  execute(data: TGetCatalogsReq): Promise<TGetCatalogsResp> {
    const request = new TGetCatalogsReq(data);

    return this.executeCommand<TGetCatalogsResp>(request, this.client.GetCatalogs);
  }
}
