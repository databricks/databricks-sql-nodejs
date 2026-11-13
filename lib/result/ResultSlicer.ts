import IClientContext from '../contracts/IClientContext';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';

export interface ResultSlicerFetchNextOptions extends ResultsProviderFetchNextOptions {
  // Setting this to `true` will disable slicer, and it will return unprocessed chunks
  // from underlying results provider
  disableBuffering?: boolean;
}

export function concatChunks<T>(chunks: Array<Array<T>>): Array<T> {
  if (chunks.length === 1) {
    return chunks[0];
  }

  // Avoid concat: apache-arrow's Symbol.isConcatSpreadable forces V8's slow path.
  let total = 0;
  for (const chunk of chunks) {
    total += chunk.length;
  }

  // Larger initial allocations use V8's slow dictionary elements; grow sequentially instead.
  const result = new Array<T>(Math.min(total, 2 ** 25));
  let offset = 0;
  for (const chunk of chunks) {
    for (let i = 0; i < chunk.length; i += 1) {
      result[offset + i] = chunk[i];
    }
    offset += chunk.length;
  }
  return result;
}

export default class ResultSlicer<T> implements IResultsProvider<Array<T>> {
  private readonly context: IClientContext;

  private readonly source: IResultsProvider<Array<T>>;

  private remainingResults: Array<T | undefined> = [];

  private remainingOffset: number = 0;

  constructor(context: IClientContext, source: IResultsProvider<Array<T>>) {
    this.context = context;
    this.source = source;
  }

  private get remainingCount(): number {
    return this.remainingResults.length - this.remainingOffset;
  }

  private compactRemaining(): void {
    if (this.remainingCount === 0) {
      this.remainingResults = [];
      this.remainingOffset = 0;
    } else if (this.remainingOffset > this.remainingResults.length / 2) {
      // Shrink the backing array without shifting the tail on every fetch.
      this.remainingResults = this.remainingResults.slice(this.remainingOffset);
      this.remainingOffset = 0;
    }
  }

  private takeRemaining(maxCount: number): Array<T> {
    const count = Number.isNaN(maxCount) ? this.remainingCount : Math.max(0, Math.trunc(maxCount));
    const end = Math.min(this.remainingResults.length, this.remainingOffset + count);
    // Unread slots contain T; only consumed slots are cleared.
    const result = this.remainingResults.slice(this.remainingOffset, end) as Array<T>;
    this.remainingResults.fill(undefined, this.remainingOffset, end);
    this.remainingOffset = end;
    this.compactRemaining();
    return result;
  }

  public async hasMore(): Promise<boolean> {
    if (this.remainingCount > 0) {
      return true;
    }
    return this.source.hasMore();
  }

  public async fetchNext(options: ResultSlicerFetchNextOptions): Promise<Array<T>> {
    // If we're asked to not use buffer - first try to return whatever we have in buffer.
    // If buffer is empty - just proxy the call to underlying results provider
    if (options.disableBuffering) {
      if (this.remainingCount > 0) {
        return this.takeRemaining(this.remainingCount);
      }

      return this.source.fetchNext(options);
    }

    const limit = Number(options.limit);
    const sourceOptions = { ...options, limit };
    const result: Array<Array<T>> = [];
    let resultsCount = 0;

    // First, use remaining items from the previous fetch
    if (this.remainingCount > 0) {
      const remaining = this.takeRemaining(limit);
      if (this.remainingCount > 0) {
        return remaining;
      }
      result.push(remaining);
      resultsCount += remaining.length;
    }

    // Fetch items from source results provider until we reach a requested count
    while (resultsCount < limit) {
      // eslint-disable-next-line no-await-in-loop
      const hasMore = await this.source.hasMore();
      if (!hasMore) {
        break;
      }

      // eslint-disable-next-line no-await-in-loop
      const chunk = await this.source.fetchNext(sourceOptions);
      const neededCount = limit - resultsCount;
      if (chunk.length > neededCount) {
        const taken = chunk.slice(0, neededCount);
        result.push(taken);
        // Own the tail so consumed references can be cleared without mutating the source.
        this.remainingResults = chunk.slice(taken.length);
        this.remainingOffset = 0;
        resultsCount += neededCount;
      } else {
        result.push(chunk);
        resultsCount += chunk.length;
      }
    }

    return concatChunks(result);
  }
}
