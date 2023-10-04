import http from 'http';
import { HeadersInit } from 'node-fetch';

export default interface IConnectionProvider {
  getThriftConnection(): Promise<any>;

  getAgent(): Promise<http.Agent>;

  setHeaders(headers: HeadersInit): void;
}
