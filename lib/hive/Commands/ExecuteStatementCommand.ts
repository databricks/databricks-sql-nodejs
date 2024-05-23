import BaseCommand from './BaseCommand';
import { TExecuteStatementReq, TExecuteStatementResp } from '../../../thrift/TCLIService_types';
import IThriftClient from '../../contracts/IThriftClient';

type Client = Pick<IThriftClient, 'ExecuteStatement'>;

export default class ExecuteStatementCommand extends BaseCommand<Client> {
  execute(executeStatementRequest: TExecuteStatementReq): Promise<TExecuteStatementResp> {
    const request = new TExecuteStatementReq(executeStatementRequest);

    return this.executeCommand<TExecuteStatementResp>(request, this.client.ExecuteStatement);
  }
}
