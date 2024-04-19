import IOperation, { IOperationChunksIterator, IOperationRowsIterator, IteratorOptions } from '../contracts/IOperation';

abstract class OperationIterator<R> implements AsyncIterableIterator<R> {
  public readonly operation: IOperation;

  protected readonly options?: IteratorOptions;

  constructor(operation: IOperation, options?: IteratorOptions) {
    this.operation = operation;
    this.options = options;
  }

  protected abstract getNext(): Promise<IteratorResult<R>>;

  public [Symbol.asyncIterator]() {
    return this;
  }

  public async next() {
    const result = await this.getNext();

    if (result.done && this.options?.autoClose) {
      await this.operation.close();
    }

    return result;
  }

  // This method is intended for a cleanup when the caller does not intend to make any more
  // reads from iterator (e.g. when using `break` in a `for ... of` loop)
  public async return(value?: any) {
    if (this.options?.autoClose) {
      await this.operation.close();
    }

    return { done: true, value };
  }
}

export class OperationChunksIterator extends OperationIterator<Array<object>> implements IOperationChunksIterator {
  protected async getNext(): Promise<IteratorResult<Array<object>>> {
    const hasMoreRows = await this.operation.hasMoreRows();
    if (hasMoreRows) {
      const value = await this.operation.fetchChunk(this.options);
      return { done: false, value };
    }

    return { done: true, value: undefined };
  }
}

export class OperationRowsIterator extends OperationIterator<object> implements IOperationRowsIterator {
  private chunk: Array<object> = [];

  private index: number = 0;

  constructor(operation: IOperation, options?: IteratorOptions) {
    super(operation, {
      ...options,
      // Tell slicer to return raw chunks. We're going to process rows one by one anyway,
      // so no need to additionally buffer and slice chunks returned by server
      disableBuffering: true,
    });
  }

  protected async getNext(): Promise<IteratorResult<object>> {
    if (this.index < this.chunk.length) {
      const value = this.chunk[this.index];
      this.index += 1;
      return { done: false, value };
    }

    const hasMoreRows = await this.operation.hasMoreRows();
    if (hasMoreRows) {
      this.chunk = await this.operation.fetchChunk(this.options);
      this.index = 0;
      // Note: this call is not really a recursion. Since this method is
      // async - the call will be actually scheduled for processing on
      // the next event loop cycle
      return this.getNext();
    }

    return { done: true, value: undefined };
  }
}
