import IAuthentication from '../contracts/IAuthentication';
import HttpTransport from '../transports/HttpTransport';
import { AuthOptions } from '../types/AuthOptions';

type HttpAuthOptions = AuthOptions & {
  headers?: object;
};

export default class PlainHttpAuthentication implements IAuthentication {
  private readonly username: string;

  private readonly password: string;

  private readonly headers: object;

  constructor(options: HttpAuthOptions) {
    this.username = options?.username || 'anonymous';
    this.password = options?.password !== undefined ? options?.password : 'anonymous';
    this.headers = options?.headers || {};
  }

  public async authenticate(transport: HttpTransport): Promise<HttpTransport> {
    transport.updateHeaders({
      ...this.headers,
      Authorization: `Bearer ${this.password}`,
    });

    return transport;
  }
}
