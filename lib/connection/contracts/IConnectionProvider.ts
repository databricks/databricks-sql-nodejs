import { HeadersInit } from 'node-fetch';

export default interface IConnectionProvider {
  getThriftConnection(): Promise<any>;

  setHeaders(headers: HeadersInit): void;
}
