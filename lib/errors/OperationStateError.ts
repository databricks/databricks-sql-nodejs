import HiveDriverError from './HiveDriverError';
import { TGetOperationStatusResp } from '../../thrift/TCLIService_types';

export default class OperationStateError extends HiveDriverError {
  public response: TGetOperationStatusResp;

  constructor(message: string, response: TGetOperationStatusResp) {
    super(message);

    this.response = response;
  }
}
