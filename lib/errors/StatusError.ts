import { TStatus } from '../../thrift/TCLIService_types';
import DBSQLError from './DBSQLError';

export default class StatusError extends DBSQLError {
  public readonly errorCode: number;

  public stack?: string;

  constructor(status: TStatus) {
    super(status.errorMessage || '');

    this.errorCode = status.errorCode || -1;

    if (Array.isArray(status.infoMessages)) {
      this.stack = status.infoMessages.join('\n');
    }
  }
}
