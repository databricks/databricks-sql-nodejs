import BaseCommand from './BaseCommand';
import { TGetPrimaryKeysReq, TGetPrimaryKeysResp } from '../../../thrift/TCLIService_types';

export default class GetPrimaryKeysCommand extends BaseCommand {
  execute(data: TGetPrimaryKeysReq): Promise<TGetPrimaryKeysResp> {
    const request = new TGetPrimaryKeysReq(data);

    return this.executeCommand<TGetPrimaryKeysResp>(request, this.client.GetPrimaryKeys);
  }
}
