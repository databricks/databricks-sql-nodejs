import { IncomingMessage, IncomingHttpHeaders } from 'http';
import DBSQLError, { DBSQLErrorOptions } from './DBSQLError';

function isString(value: unknown): value is string {
  return Object.prototype.toString.call(value) === '[object String]';
}

function getHeaderValue(headers: IncomingHttpHeaders, name: string): string {
  const value = headers[name];
  return isString(value) ? value : '';
}

export interface TransportErrorOptions extends DBSQLErrorOptions {
  response: IncomingMessage;
}

const thriftErrorMessageHeader = 'x-thriftserver-error-message';
const databricksErrorOrRedirectHeader = 'x-databricks-error-or-redirect-message';
const databricksReasonHeader = 'x-databricks-reason-phrase';

function extractErrorFromHeaders(headers: IncomingHttpHeaders, defaultMessage: string): string {
  let result: string = defaultMessage;

  const thriftErrorMessage = getHeaderValue(headers, thriftErrorMessageHeader);
  const errorOrRedirectMessage = getHeaderValue(headers, databricksErrorOrRedirectHeader);
  const reasonMessage = getHeaderValue(headers, databricksReasonHeader);

  if (thriftErrorMessage !== '' && errorOrRedirectMessage !== '') {
    // We don't expect both to be set, but log both here just in case
    result = `Thrift server error: ${thriftErrorMessage}, Databricks error: ${errorOrRedirectMessage}`;
  } else if (thriftErrorMessage !== '') {
    result = thriftErrorMessage || errorOrRedirectMessage || defaultMessage;
  }

  if (reasonMessage !== '') {
    result = `${result}: ${reasonMessage}`;
  }

  return result;
}

export default class TransportError extends DBSQLError {
  public readonly response: IncomingMessage;

  public get statusCode(): number | undefined {
    return this.response.statusCode;
  }

  public get isRetryable(): boolean {
    return this.response.statusCode === 429 || this.response.statusCode === 503;
  }

  constructor(message: string, options: TransportErrorOptions) {
    const errorMessage = extractErrorFromHeaders(options.response.headers, message);

    super(errorMessage, options);

    this.response = options.response;
  }
}
