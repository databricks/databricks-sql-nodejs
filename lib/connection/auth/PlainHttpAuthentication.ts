import IAuthentication from '../contracts/IAuthentication';
import ITransport from '../contracts/ITransport';
import { AuthOptions } from '../types/AuthOptions';

type HttpAuthOptions = AuthOptions & {
  headers?: object;
  useAADToken?: boolean;
};

export default class PlainHttpAuthentication implements IAuthentication {
  private username: string;

  private password: string;

  private useAADToken: boolean;

  private headers: object;

  constructor(options: HttpAuthOptions) {
    this.username = options?.username || 'anonymous';
    this.password = options?.password !== undefined ? options?.password : 'anonymous';
    this.headers = options?.headers || {};
    this.useAADToken = options?.useAADToken !== undefined ? options?.useAADToken : false;
  }

  authenticate(transport: ITransport): Promise<ITransport> {
    transport.setOptions('headers', {
      ...this.headers,
      Authorization: this.getToken(this.username, this.password),
    });

    return Promise.resolve(transport);
  }

  private getToken(username: string, password: string): string {
    if (this.useAADToken) {
      return `Bearer ${password}`;
    }
    return `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;
  }
}
