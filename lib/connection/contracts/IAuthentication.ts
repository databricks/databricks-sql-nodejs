import { HeadersInit } from 'node-fetch';
import http from 'http';

export default interface IAuthentication {
  authenticate(agent?: http.Agent): Promise<HeadersInit>;
}
