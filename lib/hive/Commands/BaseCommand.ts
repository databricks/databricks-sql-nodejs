import { Response } from 'node-fetch';
import TCLIService from '../../../thrift/TCLIService';
import HiveDriverError from '../../errors/HiveDriverError';
import RetryError, { RetryErrorCode } from '../../errors/RetryError';
import IClientContext from '../../contracts/IClientContext';

export default abstract class BaseCommand {
  protected client: TCLIService.Client;

  protected context: IClientContext;

  constructor(client: TCLIService.Client, context: IClientContext) {
    this.client = client;
    this.context = context;
  }

  protected async executeCommand<Response>(request: object, command: Function | void): Promise<Response> {
    try {
      return await this.invokeCommand<Response>(request, command);
    } catch (error) {
      if (error instanceof RetryError) {
        let statusCode: number | undefined;
        if (
          error.payload &&
          typeof error.payload === 'object' &&
          'response' in error.payload &&
          error.payload.response instanceof Response
        ) {
          statusCode = error.payload.response.status;
        }

        switch (error.errorCode) {
          case RetryErrorCode.AttemptsExceeded:
            throw new HiveDriverError(
              `Hive driver: ${statusCode ?? 'Error'} when connecting to resource. Max retry count exceeded.`,
            );
          case RetryErrorCode.TimeoutExceeded:
            throw new HiveDriverError(
              `Hive driver: ${statusCode ?? 'Error'} when connecting to resource. Retry timeout exceeded.`,
            );
          // no default
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
