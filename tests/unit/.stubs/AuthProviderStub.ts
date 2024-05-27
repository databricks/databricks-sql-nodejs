import { HeadersInit } from 'node-fetch';
import IAuthentication from '../../../lib/connection/contracts/IAuthentication';

export default class AuthProviderStub implements IAuthentication {
  public headers: HeadersInit;

  constructor(headers: HeadersInit = {}) {
    this.headers = headers;
  }

  public async authenticate() {
    return this.headers;
  }
}
