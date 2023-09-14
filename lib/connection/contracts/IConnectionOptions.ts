import { HeadersInit } from 'node-fetch';

export default interface IConnectionOptions {
  host: string;
  port: number;
  path?: string;
  https?: boolean;
  headers?: HeadersInit;
  socketTimeout?: number;
  proxy?: string;

  ca?: Buffer | string;
  cert?: Buffer | string;
  key?: Buffer | string;
}
