import { ConnectOptions, HttpHeaders } from 'thrift';

export default class HttpTransport {
  private options: ConnectOptions;

  constructor(options: ConnectOptions = {}) {
    this.options = { ...options };
  }

  public getOptions(): ConnectOptions {
    return this.options;
  }

  public setOptions(options: ConnectOptions) {
    this.options = { ...options };
  }

  public updateOptions(options: Partial<ConnectOptions>) {
    this.options = {
      ...this.options,
      ...options,
    };
  }

  public getOption<K extends keyof ConnectOptions>(option: K): ConnectOptions[K] {
    return this.options[option];
  }

  public setOption<K extends keyof ConnectOptions>(option: K, value: ConnectOptions[K]) {
    this.options = {
      ...this.options,
      [option]: value,
    };
  }

  public getHeaders(): HttpHeaders {
    return this.options.headers ?? {};
  }

  public setHeaders(headers: HttpHeaders) {
    this.options = {
      ...this.options,
      headers: { ...headers },
    };
  }

  public updateHeaders(headers: Partial<ConnectOptions['headers']>) {
    this.options = {
      ...this.options,
      headers: {
        ...this.options.headers,
        ...headers,
      },
    };
  }
}
