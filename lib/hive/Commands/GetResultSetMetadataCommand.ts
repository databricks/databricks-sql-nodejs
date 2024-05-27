import BaseCommand from './BaseCommand';
import { TGetResultSetMetadataReq, TGetResultSetMetadataResp } from '../../../thrift/TCLIService_types';
import IThriftClient from '../../contracts/IThriftClient';

type Client = Pick<IThriftClient, 'GetResultSetMetadata'>;

export default class GetResultSetMetadataCommand extends BaseCommand<Client> {
  execute(getResultSetMetadataRequest: TGetResultSetMetadataReq): Promise<TGetResultSetMetadataResp> {
    const request = new TGetResultSetMetadataReq(getResultSetMetadataRequest);

    return this.executeCommand<TGetResultSetMetadataResp>(request, this.client.GetResultSetMetadata);
  }
}
