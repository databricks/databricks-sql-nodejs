import { v4 } from 'uuid';
import { TTableSchema } from '../../thrift/TCLIService_types';
import IOperationBackend from '../contracts/IOperationBackend';
import { OperationState, OperationStatus } from '../contracts/OperationStatus';
import { ResultFormat, ResultMetadata } from '../contracts/ResultMetadata';
import Status from '../dto/Status';

export default class EmptyOperationBackend implements IOperationBackend {
  public readonly id = v4();

  constructor(private readonly schema: TTableSchema) {}

  public hasResultSet(): boolean {
    return true;
  }

  public async fetchChunk(): Promise<Array<object>> {
    return [];
  }

  public async hasMore(): Promise<boolean> {
    return false;
  }

  public async waitUntilReady(): Promise<void> {
    return undefined;
  }

  public async status(): Promise<OperationStatus> {
    return { state: OperationState.Succeeded, hasResultSet: true };
  }

  public async getResultMetadata(): Promise<ResultMetadata> {
    return { schema: this.schema, resultFormat: ResultFormat.ColumnBased, isStagingOperation: false };
  }

  public async cancel(): Promise<Status> {
    return Status.success();
  }

  public async close(): Promise<Status> {
    return Status.success();
  }
}
