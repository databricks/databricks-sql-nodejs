import http from 'http';
import { HeadersInit } from 'node-fetch';
import IConnectionProvider, {
  HttpRetryPolicyOptions,
  HttpTransactionDetails,
} from '../../../lib/connection/contracts/IConnectionProvider';
import IRetryPolicy from '../../../lib/connection/contracts/IRetryPolicy';
import NullRetryPolicy from '../../../lib/connection/connections/NullRetryPolicy';

export default class ConnectionProviderStub implements IConnectionProvider {
  public headers: HeadersInit = {};

  public async getThriftConnection(): Promise<any> {
    return {};
  }

  public async getAgent(): Promise<http.Agent | undefined> {
    return undefined;
  }

  public setHeaders(headers: HeadersInit) {
    this.headers = headers;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async getRetryPolicy(options?: HttpRetryPolicyOptions): Promise<IRetryPolicy<HttpTransactionDetails>> {
    return new NullRetryPolicy();
  }
}
