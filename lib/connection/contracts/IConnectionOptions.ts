import { HeadersInit } from 'node-fetch';

export interface ProxyOptions {
  protocol: 'http' | 'https' | 'socks' | 'socks4' | 'socks4a' | 'socks5' | 'socks5h';
  host: string;
  port: number;
  auth?: {
    username?: string;
    password?: string;
  };
}

export default interface IConnectionOptions {
  host: string;
  port: number;
  path?: string;
  https?: boolean;
  headers?: HeadersInit;
  socketTimeout?: number;
  proxy?: ProxyOptions;

  ca?: Buffer | string | Array<Buffer | string>;
  cert?: Buffer | string;
  key?: Buffer | string;

  // Whether the TLS server certificate is validated against the trusted CA set.
  // When omitted the connection provider treats it as `true` (validation enabled).
  rejectUnauthorized?: boolean;
}
