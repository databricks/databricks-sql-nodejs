import TCLIService from '../../../thrift/TCLIService';
import HiveDriverError from '../../errors/HiveDriverError';
import errorHandler from '../../utils/errorHandler';

export default abstract class BaseCommand {
  protected client: TCLIService.Client;

  constructor(client: TCLIService.Client) {
    this.client = client;
  }

  executeCommand<Response>(request: object, command: Function | void): Promise<Response> {
    // return errorHandler<Response>(this.client, request, command);
    return new Promise((resolve, reject) => {
      if (typeof command !== 'function') {
        reject(new HiveDriverError('Hive driver: the operation does not exist, try to choose another Thrift file.'));
        return;
      }

      try {
        console.log("Here1");
        command.call(this.client, request, (err: Error, response: Response) => {
          console.log("Here2");
          if (err) {
            reject(err);
          } else {
            resolve(response);
          }
        });
      } catch (error) {
        console.log("Here3");
        reject(error);
      }
    });
  }
}
