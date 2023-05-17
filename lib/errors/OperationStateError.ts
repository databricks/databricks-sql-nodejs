import DBSQLError from './DBSQLError';
import { TGetOperationStatusResp } from '../../thrift/TCLIService_types';

export default class OperationStateError extends DBSQLError {
  public response: TGetOperationStatusResp;

  constructor(message: string, response: TGetOperationStatusResp) {
    super(message);

    this.response = response;
  }
}
