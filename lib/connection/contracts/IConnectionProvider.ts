import IConnectionOptions from './IConnectionOptions';
import IThriftConnection from './IThriftConnection';

export default interface IConnectionProvider {
  connect(options: IConnectionOptions): Promise<IThriftConnection>;
}
