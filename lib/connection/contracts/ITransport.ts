export type ThriftConnection = any;

export default interface ITransport {
  getTransport(): any;

  setOptions(option: string, value: any): void;

  getOptions(): object;
}
