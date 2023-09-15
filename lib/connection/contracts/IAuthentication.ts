import { HeadersInit } from 'node-fetch';

export default interface IAuthentication {
  authenticate(): Promise<HeadersInit>;
}
