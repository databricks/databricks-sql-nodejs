import IDBSQLLogger from './IDBSQLLogger';
import IDriver from './IDriver';
import IConnectionProvider from '../connection/contracts/IConnectionProvider';
import IThriftClient from './IThriftClient';
import { TSparkRowSetType } from '../../thrift/TCLIService_types';

export interface ClientConfig {
  directResultsDefaultMaxRows: number;
  fetchChunkDefaultMaxRows: number;

  arrowEnabled?: boolean;
  useArrowNativeTypes?: boolean;
  socketTimeout: number;

  retryMaxAttempts: number;
  retriesTimeout: number; // in milliseconds
  retryDelayMin: number; // in milliseconds
  retryDelayMax: number; // in milliseconds

  useCloudFetch: boolean;
  cloudFetchConcurrentDownloads: number;

  useLZ4Compression: boolean;

  useAsSparkThriftServerClient: boolean;
  /**
   * Optionally set the expected result set type in case the server does not provide it.
   * This is useful for Spark Thrift Server, which does not provide the result set type.
   */
  resultFormat?: TSparkRowSetType;
}

export default interface IClientContext {
  getConfig(): ClientConfig;

  getLogger(): IDBSQLLogger;

  getConnectionProvider(): Promise<IConnectionProvider>;

  getClient(): Promise<IThriftClient>;

  getDriver(): Promise<IDriver>;
}
