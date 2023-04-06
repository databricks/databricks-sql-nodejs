import { TStatus, TStatusCode } from '../../thrift/TCLIService_types';
import StatusError from '../errors/StatusError';

export default class Status {
  private readonly status: TStatus;

  constructor(status: TStatus) {
    this.status = status;
  }

  public get isSuccess(): boolean {
    const { statusCode } = this.status;
    return statusCode === TStatusCode.SUCCESS_STATUS || statusCode === TStatusCode.SUCCESS_WITH_INFO_STATUS;
  }

  public get isExecuting(): boolean {
    const { statusCode } = this.status;
    return statusCode === TStatusCode.STILL_EXECUTING_STATUS;
  }

  public get isError(): boolean {
    const { statusCode } = this.status;
    return statusCode === TStatusCode.ERROR_STATUS || statusCode === TStatusCode.INVALID_HANDLE_STATUS;
  }

  public get info(): Array<string> {
    return this.status.infoMessages || [];
  }

  public static assert(status: TStatus) {
    const statusWrapper = new Status(status);
    if (statusWrapper.isError) {
      throw new StatusError(status);
    }
  }

  public static success(info: Array<string> = []): Status {
    return new Status({
      statusCode: info.length > 0 ? TStatusCode.SUCCESS_WITH_INFO_STATUS : TStatusCode.SUCCESS_STATUS,
      infoMessages: info,
    });
  }
}
