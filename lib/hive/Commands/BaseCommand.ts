import { Thrift } from 'thrift';
import TCLIService from '../../../thrift/TCLIService';
import HiveDriverError from '../../errors/HiveDriverError';

interface CommandRequestInfo {
  numRetries: number;
  startTime: number;
}

export default abstract class BaseCommand {
  protected client: TCLIService.Client;

  constructor(client: TCLIService.Client) {
    this.client = client;
  }

  executeCommand<Response>(request: object, command: Function | void): Promise<Response> {
    return this.invokeWithErrorHandling<Response>(request, command, { numRetries: 0, startTime: Date.now() });
  }

  private async invokeWithErrorHandling<Response>(
    request: object,
    command: Function | void,
    info: CommandRequestInfo,
  ): Promise<Response> {
    try {
      return await this.invokeCommand<Response>(request, command);
    } catch (error) {
      if (error instanceof Thrift.TApplicationException) {
        if ('statusCode' in error) {
          switch (error.statusCode) {
            case 429:
            case 503:
              if (Date.now() - info.startTime > 15000) {
                return Promise.reject(error);
              }

              info.numRetries += 1;
              return this.invokeWithErrorHandling(request, command, info);
            case 404:
              return Promise.reject(
                new HiveDriverError('Hive driver: 404 when connecting to resource. Check the host provided.'),
              );
            case 403:
              return Promise.reject(
                new HiveDriverError(
                  'Hive driver: 403 when connecting to resource. Check the token used to authenticate.',
                ),
              );
            case 401:
              return Promise.reject(
                new HiveDriverError('Hive driver: 401 when connecting to resource. Check the path provided.'),
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
