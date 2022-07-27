import BaseCommand from './BaseCommand';
import { TGetResultSetMetadataReq, TGetResultSetMetadataResp } from '../../../thrift/TCLIService_types';

export default class GetResultSetMetadataCommand extends BaseCommand {
  execute(getResultSetMetadataRequest: TGetResultSetMetadataReq): Promise<TGetResultSetMetadataResp> {
    const request = new TGetResultSetMetadataReq(getResultSetMetadataRequest);

    return this.executeCommand<TGetResultSetMetadataResp>(request, this.client.GetResultSetMetadata);
  }
}
