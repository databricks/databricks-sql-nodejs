export interface ResultsProviderFetchNextOptions {
  limit: number;
}

export default interface IResultsProvider<T> {
  fetchNext(options: ResultsProviderFetchNextOptions): Promise<T>;

  hasMore(): Promise<boolean>;
}

export interface RowTarget<T> {
  rows: Array<T>;
  count: number;
}

export interface RowResultsProvider<T> extends IResultsProvider<Array<T>> {
  fetchNextInto?(options: ResultsProviderFetchNextOptions, target: RowTarget<T>, limit: number): Promise<Array<T>>;
}
