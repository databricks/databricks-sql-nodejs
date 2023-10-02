import http from 'http';
import { HeadersInit } from 'node-fetch';

export default interface IAuthentication {
  authenticate(agent?: http.Agent): Promise<HeadersInit>;
}
