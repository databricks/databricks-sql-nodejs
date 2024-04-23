import IDBSQLLogger from './IDBSQLLogger';
import IDriver from './IDriver';
import IConnectionProvider from '../connection/contracts/IConnectionProvider';
import TCLIService from '../../thrift/TCLIService';

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
}

export default interface IClientContext {
  getConfig(): ClientConfig;

  getLogger(): IDBSQLLogger;

  getConnectionProvider(): Promise<IConnectionProvider>;

  getClient(): Promise<TCLIService.Client>;

  getDriver(): Promise<IDriver>;
}
