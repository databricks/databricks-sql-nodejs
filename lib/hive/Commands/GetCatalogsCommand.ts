import BaseCommand from './BaseCommand';
import { TGetCatalogsReq, TGetCatalogsResp } from '../../../thrift/TCLIService_types';

export default class GetCatalogsCommand extends BaseCommand {
  execute(data: TGetCatalogsReq): Promise<TGetCatalogsResp> {
    const request = new TGetCatalogsReq(data);

    return this.executeCommand<TGetCatalogsResp>(request, this.client.GetCatalogs);
  }
}
