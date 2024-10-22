import IOperation, {
  IOperationChunksIterator,
  IOperationRowsIterator,
  IteratorOptions,
  NodeStreamOptions,
} from '../../../lib/contracts/IOperation';
import Status from '../../../lib/dto/Status';
import { OperationChunksIterator, OperationRowsIterator } from '../../../lib/utils/OperationIterator';
import { Readable } from 'node:stream';

export default class OperationStub implements IOperation {
  public readonly id: string = '';

  private chunks: Array<Array<any>>;
  public closed: boolean;

  constructor(chunks: Array<Array<any>>) {
    this.chunks = Array.isArray(chunks) ? [...chunks] : [];
    this.closed = false;
  }

  public async fetchChunk() {
    return this.chunks.shift() ?? [];
  }

  public async fetchAll() {
    const result = this.chunks.flat();
    this.chunks = [];
    return result;
  }

  public async status() {
    return Promise.reject(new Error('Not implemented'));
  }

  public async cancel() {
    return Promise.reject(new Error('Not implemented'));
  }

  public async close() {
    this.closed = true;
    return Status.success();
  }

  public async finished() {
    return Promise.resolve();
  }

  public async hasMoreRows() {
    return !this.closed && this.chunks.length > 0;
  }

  public async getSchema() {
    return Promise.reject(new Error('Not implemented'));
  }

  public iterateChunks(options?: IteratorOptions): IOperationChunksIterator {
    return new OperationChunksIterator(this, options);
  }

  public iterateRows(options?: IteratorOptions): IOperationRowsIterator {
    return new OperationRowsIterator(this, options);
  }

  public toNodeStream(options?: NodeStreamOptions): Readable {
    throw new Error('Not implemented');
  }
}
