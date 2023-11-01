import IClientContext from '../contracts/IClientContext';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';

export default class ResultSlicer<T> implements IResultsProvider<Array<T>> {
  private readonly context: IClientContext;

  private readonly source: IResultsProvider<Array<T>>;

  private remainingResults: Array<T> = [];

  constructor(context: IClientContext, source: IResultsProvider<Array<T>>) {
    this.context = context;
    this.source = source;
  }

  public async hasMore(): Promise<boolean> {
    if (this.remainingResults.length > 0) {
      return true;
    }
    return this.source.hasMore();
  }

  public async fetchNext(options: ResultsProviderFetchNextOptions): Promise<Array<T>> {
    const result: Array<Array<T>> = [];
    let resultsCount = 0;

    // First, use remaining items from the previous fetch
    if (this.remainingResults.length > 0) {
      result.push(this.remainingResults);
      resultsCount += this.remainingResults.length;
      this.remainingResults = [];
    }

    // Fetch items from source results provider until we reach a requested count
    while (resultsCount < options.limit) {
      // eslint-disable-next-line no-await-in-loop
      const chunk = await this.source.fetchNext(options);
      if (chunk.length === 0) {
        break;
      }

      result.push(chunk);
      resultsCount += chunk.length;
    }

    // If we collected more results than requested, slice the excess items and store them for the next time
    if (resultsCount > options.limit) {
      const lastChunk = result.pop() ?? [];
      const neededCount = options.limit - (resultsCount - lastChunk.length);
      result.push(lastChunk.splice(0, neededCount));
      this.remainingResults = lastChunk;
    }

    return result.flat();
  }
}
