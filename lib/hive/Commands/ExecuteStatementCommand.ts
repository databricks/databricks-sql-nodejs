import BaseCommand from './BaseCommand';
import { TExecuteStatementReq, TExecuteStatementResp } from '../../../thrift/TCLIService_types';

export default class ExecuteStatementCommand extends BaseCommand {
  execute(executeStatementRequest: TExecuteStatementReq): Promise<TExecuteStatementResp> {
    const request = new TExecuteStatementReq(executeStatementRequest);

    return this.executeCommand<TExecuteStatementResp>(request, this.client.ExecuteStatement);
  }
}
