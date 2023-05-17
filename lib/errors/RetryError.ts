import DBSQLError, { DBSQLErrorOptions } from './DBSQLError';

export enum RetryErrorCode {
  OutOfTime = 'OUT_OF_TIME',
  OutOfAttempts = 'OUT_OF_ATTEMPTS',
}

const errorMessages: Record<RetryErrorCode, string> = {
  [RetryErrorCode.OutOfTime]: 'Retry timeout exceeded',
  [RetryErrorCode.OutOfAttempts]: 'Max retry count exceeded',
};

export interface RetryErrorOptions extends DBSQLErrorOptions {}

export default class RetryError extends DBSQLError {
  public readonly errorCode: RetryErrorCode;

  constructor(errorCode: RetryErrorCode, options: RetryErrorOptions) {
    let message = errorMessages[errorCode];
    if (options.cause) {
      message = `${options.cause.message} ${message}`;
    }

    super(message, options);

    this.errorCode = errorCode;
  }
}
