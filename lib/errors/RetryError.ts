export enum RetryErrorCode {
  AttemptsExceeded = 'ATTEMPTS_EXCEEDED',
  TimeoutExceeded = 'TIMEOUT_EXCEEDED',
}

const errorMessages: Record<RetryErrorCode, string> = {
  [RetryErrorCode.AttemptsExceeded]: 'Max retry count exceeded',
  [RetryErrorCode.TimeoutExceeded]: 'Retry timeout exceeded',
};

export default class RetryError extends Error {
  public readonly errorCode: RetryErrorCode;

  public readonly payload: unknown;

  constructor(errorCode: RetryErrorCode, payload?: unknown) {
    super(errorMessages[errorCode]);
    this.errorCode = errorCode;
    this.payload = payload;
  }
}
