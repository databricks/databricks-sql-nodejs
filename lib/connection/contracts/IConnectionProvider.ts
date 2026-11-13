import http from 'http';
import { HeadersInit, Request, Response } from 'node-fetch';
import IRetryPolicy from './IRetryPolicy';

export interface HttpTransactionDetails {
  request: Request;
  response: Response;
}

export interface HttpRetryPolicyOptions {
  /**
   * Also retry 4xx responses, except these statuses. For idempotent GETs of presigned
   * URLs (CloudFetch), where a 4xx from storage does not mean the request was wrong.
   */
  retryClientErrorsExcept?: ReadonlySet<number>;
}

export default interface IConnectionProvider {
  getThriftConnection(): Promise<any>;

  /**
   * Returns the agent for a request to `url`, whose protocol it must match.
   * Without `url`, returns the agent for the SQL transport.
   */
  getAgent(url?: string | URL): Promise<http.Agent | undefined>;

  setHeaders(headers: HeadersInit): void;

  getRetryPolicy(options?: HttpRetryPolicyOptions): Promise<IRetryPolicy<HttpTransactionDetails>>;
}
