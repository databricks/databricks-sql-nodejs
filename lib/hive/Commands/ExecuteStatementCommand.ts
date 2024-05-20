import BaseCommand from './BaseCommand';
import { TExecuteStatementReq, TExecuteStatementResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'ExecuteStatement'>;

export default class ExecuteStatementCommand extends BaseCommand<Client> {
  execute(executeStatementRequest: TExecuteStatementReq): Promise<TExecuteStatementResp> {
    const request = new TExecuteStatementReq(executeStatementRequest);

    return this.executeCommand<TExecuteStatementResp>(request, this.client.ExecuteStatement);
  }
}
