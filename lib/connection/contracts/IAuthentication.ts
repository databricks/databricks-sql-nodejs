import HttpTransport from '../transports/HttpTransport';

export default interface IAuthentication {
  authenticate(transport: HttpTransport): Promise<HttpTransport>;
}
