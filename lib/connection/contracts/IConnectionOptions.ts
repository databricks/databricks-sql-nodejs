import { HttpHeaders } from 'thrift';

export type Options = {
  socketTimeout?: number;
  username?: string;
  password?: string;
  ssl?: boolean;
  https?: boolean;
  debug?: boolean;
  max_attempts?: number;
  retry_max_delay?: number;
  connect_timeout?: number;
  timeout?: number;
  headers?: HttpHeaders;
  path?: string;
  ca?: Buffer | string;
  cert?: Buffer | string;
  key?: Buffer | string;
  [key: string]: any;
};

export default interface IConnectionOptions {
  host: string;
  port: number;
  options?: Options;
}
