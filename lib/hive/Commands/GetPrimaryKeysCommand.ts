import BaseCommand from './BaseCommand';
import { TGetPrimaryKeysReq, TGetPrimaryKeysResp } from '../../../thrift/TCLIService_types';
import IThriftClient from '../../contracts/IThriftClient';

type Client = Pick<IThriftClient, 'GetPrimaryKeys'>;

export default class GetPrimaryKeysCommand extends BaseCommand<Client> {
  execute(data: TGetPrimaryKeysReq): Promise<TGetPrimaryKeysResp> {
    const request = new TGetPrimaryKeysReq(data);

    return this.executeCommand<TGetPrimaryKeysResp>(request, this.client.GetPrimaryKeys);
  }
}
