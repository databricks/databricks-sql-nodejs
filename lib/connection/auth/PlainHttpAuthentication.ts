import { HttpHeaders } from 'thrift';
import IAuthentication from '../contracts/IAuthentication';

interface PlainHttpAuthenticationOptions {
  username?: string;
  password?: string;
  headers?: HttpHeaders;
}

export default class PlainHttpAuthentication implements IAuthentication {
  private readonly username: string;

  private readonly password: string;

  private readonly headers: HttpHeaders;

  constructor(options: PlainHttpAuthenticationOptions) {
    this.username = options?.username || 'anonymous';
    this.password = options?.password ?? 'anonymous';
    this.headers = options?.headers || {};
  }

  public async authenticate(): Promise<HttpHeaders> {
    return {
      ...this.headers,
      Authorization: `Bearer ${this.password}`,
    };
  }
}
