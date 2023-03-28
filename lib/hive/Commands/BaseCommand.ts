import { Thrift } from 'thrift';
import TCLIService from '../../../thrift/TCLIService';
import HiveDriverError from '../../errors/HiveDriverError';

interface CommandExecutionInfo {
  startTime: number; // in milliseconds
  attempt: number;
}

const retryMaxAttempts = 30;
const retriesTimeout = 900 * 1000; // in milliseconds
const retryDelayMin = 1 * 1000; // in milliseconds
const retryDelayMax = 60 * 1000; // in milliseconds

function getRetryDelay(attempt: number): number {
  const scale = 1.5 ** (attempt - 1); // attempt >= 0, scale >= 1
  return Math.min(retryDelayMin * scale, retryDelayMax);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(() => resolve(), milliseconds);
  });
}

export default abstract class BaseCommand {
  protected client: TCLIService.Client;

  constructor(client: TCLIService.Client) {
    this.client = client;
  }

  executeCommand<Response>(request: object, command: Function | void): Promise<Response> {
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
      if (error instanceof Thrift.TApplicationException) {
        if ('statusCode' in error) {
          switch (error.statusCode) {
            // On this status codes it's safe to retry the request. However,
            // both error codes mean that server is overwhelmed or even down.
            // Therefore, we need to add some delay between attempts so
            // server can recover and more likely handle next request
            case 429: // Too Many Requests
            case 503: // Service Unavailable
              info.attempt += 1;

              // Delay interval depends on current attempt - the more attempts we do
              // the longer the interval will be
              // TODO: Respect `Retry-After` header
              const retryDelay = getRetryDelay(info.attempt);

              const attemptsExceeded = info.attempt >= retryMaxAttempts;
              const timeoutExceeded = Date.now() - info.startTime + retryDelay >= retriesTimeout;

              if (attemptsExceeded || timeoutExceeded) {
                return Promise.reject(error);
              }

              await delay(retryDelay);
              return this.invokeWithErrorHandling(request, command, info);

            case 404: // Not Found
              return Promise.reject(
                new HiveDriverError('Hive driver: 404 when connecting to resource. Check the host and path provided.'),
              );

            // These two status codes usually mean that wrong credentials were passed
            case 401: // Unauthorized
            case 403: // Forbidden
              return Promise.reject(
                new HiveDriverError(
                  `Hive driver: ${error.statusCode} when connecting to resource. Check the token used to authenticate.`,
                ),
              );

            // no default
          }
        }
      }

      // Re-throw error we didn't handle
      throw error;
    }
  }

  private invokeCommand<Response>(request: object, command: Function | void): Promise<Response> {
    if (typeof command !== 'function') {
      return Promise.reject(
        new HiveDriverError('Hive driver: the operation does not exist, try to choose another Thrift file.'),
      );
    }

    return new Promise((resolve, reject) => {
      try {
        command.call(this.client, request, (err: Error, response: Response) => {
          if (err) {
            reject(err);
          } else {
            resolve(response);
          }
        });
      } catch (error) {
        reject(error);
      }
    });
  }
}
