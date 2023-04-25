import { Thrift } from 'thrift';
import TCLIService from '../../../thrift/TCLIService';
import HiveDriverError from '../../errors/HiveDriverError';
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
      if (error instanceof Thrift.TApplicationException) {
        if ('statusCode' in error) {
          switch (error.statusCode) {
            // On these status codes it's safe to retry the request. However,
            // both error codes mean that server is overwhelmed or even down.
            // Therefore, we need to add some delay between attempts so
            // server can recover and more likely handle next request
            case 429: // Too Many Requests
            case 503: // Service Unavailable
              info.attempt += 1;

              // Delay interval depends on current attempt - the more attempts we do
              // the longer the interval will be
              // TODO: Respect `Retry-After` header (PECO-729)
              const retryDelay = getRetryDelay(info.attempt);

              const attemptsExceeded = info.attempt >= globalConfig.retryMaxAttempts;
              if (attemptsExceeded) {
                throw new HiveDriverError(
                  `Hive driver: ${error.statusCode} when connecting to resource. Max retry count exceeded.`,
                );
              }

              const timeoutExceeded = Date.now() - info.startTime + retryDelay >= globalConfig.retriesTimeout;
              if (timeoutExceeded) {
                throw new HiveDriverError(
                  `Hive driver: ${error.statusCode} when connecting to resource. Retry timeout exceeded.`,
                );
              }

              await delay(retryDelay);
              return this.invokeWithErrorHandling(request, command, info);

            // TODO: Here we should handle other error types (see PECO-730)

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
