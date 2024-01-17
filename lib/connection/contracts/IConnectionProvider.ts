import http from 'http';
import { HeadersInit, Response } from 'node-fetch';
import IRetryPolicy from './IRetryPolicy';

export default interface IConnectionProvider {
  getThriftConnection(): Promise<any>;

  getAgent(): Promise<http.Agent>;

  setHeaders(headers: HeadersInit): void;

  getRetryPolicy(): Promise<IRetryPolicy<Response>>;
}
