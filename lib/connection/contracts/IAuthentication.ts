import { HttpHeaders } from 'thrift';

export default interface IAuthentication {
  authenticate(): Promise<HttpHeaders>;
}
