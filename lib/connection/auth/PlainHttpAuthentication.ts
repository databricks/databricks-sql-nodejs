import { HeadersInit } from 'node-fetch';
import IAuthentication from '../contracts/IAuthentication';
import IClientContext from '../../contracts/IClientContext';

interface PlainHttpAuthenticationOptions {
  username?: string;
  password?: string;
  headers?: HeadersInit;
  context: IClientContext;
}

export default class PlainHttpAuthentication implements IAuthentication {
  protected readonly context: IClientContext;

  protected readonly username: string;

  protected readonly password: string;

  protected readonly headers: HeadersInit;

  constructor(options: PlainHttpAuthenticationOptions) {
    this.context = options.context;
    this.username = options?.username || 'anonymous';
    this.password = options?.password ?? 'anonymous';
    this.headers = options?.headers || {};
  }

  public async authenticate(): Promise<HeadersInit> {
    return {
      ...this.headers,
      Authorization: `Bearer ${this.password}`,
    };
  }
}
