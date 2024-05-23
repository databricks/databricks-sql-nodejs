import BaseCommand from './BaseCommand';
import { TGetCatalogsReq, TGetCatalogsResp } from '../../../thrift/TCLIService_types';
import IThriftClient from '../../contracts/IThriftClient';

type Client = Pick<IThriftClient, 'GetCatalogs'>;

export default class GetCatalogsCommand extends BaseCommand<Client> {
  execute(data: TGetCatalogsReq): Promise<TGetCatalogsResp> {
    const request = new TGetCatalogsReq(data);

    return this.executeCommand<TGetCatalogsResp>(request, this.client.GetCatalogs);
  }
}
