import { Thrift } from 'thrift';
import { IncomingMessage } from 'http';
import TCLIService from '../../../thrift/TCLIService';
import DBSQLError from '../../errors/DBSQLError';
import DriverError from '../../errors/DriverError';
import TransportError from '../../errors/TransportError';
import RetryError, { RetryErrorCode } from '../../errors/RetryError';
import globalConfig from '../../globalConfig';

interface CommandExecutionInfo {
  startTime: number; // in milliseconds
  attempt: number;
}

function getRetryDelay(attempt: number): number {
  const scale = Math.max(1, 1.5 ** (attempt - 1)); // ensure scale >= 1
  return Math.min(globalConfig.retryDelayMin * scale, globalConfig.retryDelayMax);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(() => resolve(), milliseconds);
  });
}

function convertThriftError(error: Error): Error {
  if (error instanceof Thrift.TApplicationException) {
    // Detect THTTPException which is not exported from `thrift`
    if ('response' in error && error.response instanceof IncomingMessage) {
      return new TransportError(error.message, {
        cause: error,
        response: error.response,
      });
    }

    return new DriverError(error.message, { cause: error });
  }

  if (error instanceof Thrift.TProtocolException) {
    return new DriverError(error.message, { cause: error });
  }

  // Return other errors as is
  return error;
}

export default abstract class BaseCommand {
  protected client: TCLIService.Client;

  constructor(client: TCLIService.Client) {
    this.client = client;
  }

  protected executeCommand<Response>(request: object, command: Function | void): Promise<Response> {
    return this.invokeWithErrorHandling<Response>(request, command, { startTime: Date.now(), attempt: 0 });
  }

  private async invokeWithErrorHandling<Response>(
    request: object,
    command: Function | void,
    info: CommandExecutionInfo,
  ): Promise<Response> {
    try {
      return await this.invokeCommand<Response>(request, command);
    } catch (error) {
      if (error instanceof DBSQLError) {
        // When handling retryable errors we should keep in mind that
        // error may be caused by server being overwhelmed or even down.
        // Therefore, we need to add some delay between attempts so
        // server can recover and more likely handle next request
        if (error.isRetryable) {
          info.attempt += 1;

          // Delay interval depends on current attempt - the more attempts we do
          // the longer the interval will be
          // TODO: Respect `Retry-After` header (PECO-729)
          const retryDelay = getRetryDelay(info.attempt);

          const attemptsExceeded = info.attempt >= globalConfig.retryMaxAttempts;
          if (attemptsExceeded) {
            throw new RetryError(RetryErrorCode.OutOfAttempts, { cause: error });
          }

          const timeoutExceeded = Date.now() - info.startTime + retryDelay >= globalConfig.retriesTimeout;
          if (timeoutExceeded) {
            throw new RetryError(RetryErrorCode.OutOfTime, { cause: error });
          }

          await delay(retryDelay);
          return this.invokeWithErrorHandling(request, command, info);
        }
      }

      // Re-throw error we didn't handle
      throw error;
    }
  }

  private invokeCommand<Response>(request: object, command: Function | void): Promise<Response> {
    if (typeof command !== 'function') {
      return Promise.reject(new DriverError('The operation does not exist, try to choose another Thrift file.'));
    }

    return new Promise((resolve, reject) => {
      try {
        command.call(this.client, request, (err: Error, response: Response) => {
          if (err) {
            reject(convertThriftError(err));
          } else {
            resolve(response);
          }
        });
      } catch (error) {
        if (error instanceof Error) {
          reject(convertThriftError(error));
        } else {
          reject(error);
        }
      }
    });
  }
}
