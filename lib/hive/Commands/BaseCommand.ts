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
    return this.handleErrors<Response>(request, command, { numRetries: 0, startTime: Date.now() });
  }

  private handleErrors<Response>(
    request: object,
    command: Function | void,
    info: CommandRequestInfo,
  ): Promise<Response> {
    return new Promise((resolve, reject) => {
      if (typeof command !== 'function') {
        reject(new HiveDriverError('Hive driver: the operation does not exist, try to choose another Thrift file.'));
        return;
      }

      try {
        command.call(this.client, request, (err: Error, response: Response) => {
          if (response) {
            resolve(response);
            return;
          }
          if (err instanceof Thrift.TApplicationException) {
            if ('statusCode' in err) {
              // eslint-disable-next-line @typescript-eslint/dot-notation
              switch (err['statusCode']) {
                case 429:
                case 503:
                  if (Date.now() - info.startTime > 15000) {
                    reject(err);
                    return;
                  }

                  info.numRetries += 1;
                  return this.handleErrors(request, command, info);

                case 404:
                  reject(new HiveDriverError('Hive driver: 404 when connecting to resource. Check the host provided.'));
                  return;
                case 403:
                  reject(
                    new HiveDriverError(
                      'Hive driver: 403 when connecting to resource. Check the token used to authenticate.',
                    ),
                  );
                  return;
                case 401:
                  reject(new HiveDriverError('Hive driver: 401 when connecting to resource. Check the path provided.'));
                  return;
                default:
                  reject(err);
              }
            }
          } else {
            reject(err);
          }
        });
      } catch {
        reject(new HiveDriverError('Hive driver: Error when invoking command.'));
      }
    });
  }
}
