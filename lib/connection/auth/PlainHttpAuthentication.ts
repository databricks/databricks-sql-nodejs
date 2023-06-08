import IAuthentication from '../contracts/IAuthentication';
import ITransport from '../contracts/ITransport';
import { AuthOptions } from '../types/AuthOptions';

interface HttpAuthOptions extends AuthOptions {
  headers?: object;
}

export default class PlainHttpAuthentication implements IAuthentication {
  private readonly username: string;

  private readonly password: string;

  private readonly headers: object;

  constructor(options: HttpAuthOptions) {
    this.username = options?.username || 'anonymous';
    this.password = options?.password !== undefined ? options?.password : 'anonymous';
    this.headers = options?.headers || {};
  }

  async authenticate(transport: ITransport): Promise<ITransport> {
    transport.setOptions('headers', {
      ...this.headers,
      Authorization: this.getToken(this.username, this.password),
    });

    return transport;
  }

  private getToken(username: string, password: string): string {
    return `Bearer ${password}`;
  }
}
