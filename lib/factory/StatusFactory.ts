import { TStatusCode, TStatus } from '../../thrift/TCLIService_types';
import Status from '../dto/Status';
import StatusError from '../errors/StatusError';

export default class StatusFactory {
  /**
   * @param status thrift status object from API responses
   * @throws {StatusError}
   */
  create(status: TStatus): Status {
    if (this.isError(status)) {
      throw new StatusError(status);
    }

    return new Status({
      success: this.isSuccess(status),
      executing: this.isExecuting(status),
      infoMessages: status.infoMessages || [],
    });
  }

  private isSuccess(status: TStatus): boolean {
    return (
      status.statusCode === TStatusCode.SUCCESS_STATUS || status.statusCode === TStatusCode.SUCCESS_WITH_INFO_STATUS
    );
  }

  private isError(status: TStatus): boolean {
    return status.statusCode === TStatusCode.ERROR_STATUS || status.statusCode === TStatusCode.INVALID_HANDLE_STATUS;
  }

  private isExecuting(status: TStatus): boolean {
    return status.statusCode === TStatusCode.STILL_EXECUTING_STATUS;
  }
}
