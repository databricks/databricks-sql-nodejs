import { type } from 'os';
import TCLIService from '../../../thrift/TCLIService';
import HiveDriverError from '../../errors/HiveDriverError';
import errorHandler from '../../utils/errorHandler';
import { TApplicationException, TProtocolException } from '../..';

export default abstract class BaseCommand {
  protected client: TCLIService.Client;

  constructor(client: TCLIService.Client) {
    this.client = client;
  }

  executeCommand<Response>(request: object, command: Function | void): Promise<Response> {
    return errorHandler<Response>(this.client, request, command, {numRetries: 0, startTime: Date.now()});
  }
}
