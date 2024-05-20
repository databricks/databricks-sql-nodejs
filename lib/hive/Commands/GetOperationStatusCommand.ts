import BaseCommand from './BaseCommand';
import { TGetOperationStatusReq, TGetOperationStatusResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'GetOperationStatus'>;

export default class GetOperationStatusCommand extends BaseCommand<Client> {
  execute(data: TGetOperationStatusReq): Promise<TGetOperationStatusResp> {
    const request = new TGetOperationStatusReq(data);

    return this.executeCommand<TGetOperationStatusResp>(request, this.client.GetOperationStatus);
  }
}
