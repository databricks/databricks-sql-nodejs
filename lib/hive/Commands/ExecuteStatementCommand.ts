import { SessionHandle, Status, OperationHandle, Int64 } from '../Types';
import BaseCommand from './BaseCommand';

export type ExecuteStatementRequest = {
  sessionHandle: SessionHandle;
  statement: string;
  confOverlay?: Record<string, string>;
  runAsync?: boolean;
  queryTimeout?: Int64;
};

export type ExecuteStatementResponse = {
  status: Status;
  operationHandle: OperationHandle;
};

export default class ExecuteStatementCommand extends BaseCommand {
  execute(executeStatementRequest: ExecuteStatementRequest): Promise<ExecuteStatementResponse> {
    const request = new this.TCLIService_types.TExecuteStatementReq(executeStatementRequest);

    return this.executeCommand<ExecuteStatementResponse>(request, this.client.ExecuteStatement);
  }
}
