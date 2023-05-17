export interface DBSQLErrorOptions {
  cause?: Error;
}

export default class DBSQLError extends Error {
  public readonly cause?: Error;

  public get isRetryable(): boolean {
    return false;
  }

  public get retryAfter(): number | undefined {
    return undefined;
  }

  constructor(message: string, options: DBSQLErrorOptions = {}) {
    super(message);
    this.cause = options.cause;
  }
}

