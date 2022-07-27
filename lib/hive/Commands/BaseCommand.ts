import TCLIService from '../../../thrift/TCLIService';
import HiveDriverError from '../../errors/HiveDriverError';

export default abstract class BaseCommand {
  protected client: TCLIService.Client;

  constructor(client: TCLIService.Client) {
    this.client = client;
  }

  executeCommand<Response>(request: object, command: Function | void): Promise<Response> {
    return new Promise((resolve, reject) => {
      if (typeof command !== 'function') {
        return reject(
          new HiveDriverError('Hive driver: the operation does not exist, try to choose another Thrift file.'),
        );
      }

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
