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

  const result = new Array<T>(total);
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

  private remainingResults: Array<T> = [];

  private remainingOffset: number = 0;

  constructor(context: IClientContext, source: IResultsProvider<Array<T>>) {
    this.context = context;
    this.source = source;
  }

  private get remainingCount(): number {
    return this.remainingResults.length - this.remainingOffset;
  }

  private takeRemaining(maxCount: number): Array<T> {
    const end = Math.min(this.remainingResults.length, this.remainingOffset + maxCount);
    const result = this.remainingResults.slice(this.remainingOffset, end);
    this.remainingOffset = end;
    if (this.remainingCount === 0) {
      this.remainingResults = [];
      this.remainingOffset = 0;
    }
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

    const result: Array<Array<T>> = [];
    let resultsCount = 0;

    // First, use remaining items from the previous fetch
    if (this.remainingCount > 0) {
      const remaining = this.takeRemaining(options.limit);
      result.push(remaining);
      resultsCount += remaining.length;
    }

    // Fetch items from source results provider until we reach a requested count
    while (resultsCount < options.limit) {
      // eslint-disable-next-line no-await-in-loop
      const hasMore = await this.source.hasMore();
      if (!hasMore) {
        break;
      }

      // eslint-disable-next-line no-await-in-loop
      const chunk = await this.source.fetchNext(options);
      const neededCount = options.limit - resultsCount;
      if (chunk.length > neededCount) {
        result.push(chunk.slice(0, neededCount));
        this.remainingResults = chunk;
        this.remainingOffset = neededCount;
        resultsCount += neededCount;
      } else {
        result.push(chunk);
        resultsCount += chunk.length;
      }
    }

    return concatChunks(result);
  }
}
