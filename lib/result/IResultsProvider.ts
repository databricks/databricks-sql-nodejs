export interface ResultsProviderFetchNextOptions {
  limit: number;
}

export default interface IResultsProvider<T> {
  fetchNext(options: ResultsProviderFetchNextOptions): Promise<T>;

  hasMore(): Promise<boolean>;
}
