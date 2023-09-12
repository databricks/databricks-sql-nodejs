import { AxiosRequestConfig } from 'axios';

export default interface IConnectionOptions {
  host: string;
  port: number;
  path?: string;
  https?: boolean;
  headers?: AxiosRequestConfig['headers'];
  socketTimeout?: number;

  ca?: Buffer | string;
  cert?: Buffer | string;
  key?: Buffer | string;
}
