import ITransport from '../contracts/ITransport';

export default class XhrTransport implements ITransport {
  private xhrOptions: object;

  constructor(httpOptions: object = {}) {
    this.xhrOptions = httpOptions;
  }

  getTransport(): any {
    return this.xhrOptions;
  }

  setOptions(option: string, value: any) {
    this.xhrOptions = {
      ...this.xhrOptions,
      [option]: value,
    };
  }

  getOptions(): object {
    return this.xhrOptions;
  }

  connect() {}

  addListener() {}

  removeListener() {}

  write() {}

  end() {}

  emit() {}
}
