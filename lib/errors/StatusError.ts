import { TStatus } from '../../thrift/TCLIService_types';

export default class StatusError implements Error {
  public name: string;
  public message: string;
  public code: number;
  public stack?: string;

  constructor(status: TStatus) {
    this.name = 'Status Error';
    this.message = status.errorMessage || '';
    this.code = status.errorCode || -1;

    if (Array.isArray(status.infoMessages)) {
      this.stack = status.infoMessages.join('\n');
    }
  }
}
