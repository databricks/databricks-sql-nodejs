import http from 'http';
import { HeadersInit, Request, Response } from 'node-fetch';
import IRetryPolicy from './IRetryPolicy';

export interface HttpTransactionDetails {
  request: Request;
  response: Response;
}

export default interface IConnectionProvider {
  getThriftConnection(): Promise<any>;

  getAgent(): Promise<http.Agent>;

  setHeaders(headers: HeadersInit): void;

  getRetryPolicy(): Promise<IRetryPolicy<HttpTransactionDetails>>;
}
