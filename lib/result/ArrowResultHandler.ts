import LZ4 from 'lz4';
import { TRowSet } from '../../thrift/TCLIService_types';
import IClientContext from '../contracts/IClientContext';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';

export default class ArrowResultHandler implements IResultsProvider<Array<Buffer>> {
  protected readonly context: IClientContext;

  private readonly source: IResultsProvider<TRowSet | undefined>;

  private readonly arrowSchema?: Buffer;

  private readonly isLZ4Compressed: boolean;

  constructor(
    context: IClientContext,
    source: IResultsProvider<TRowSet | undefined>,
    arrowSchema?: Buffer,
    isLZ4Compressed?: boolean,
  ) {
    this.context = context;
    this.source = source;
    this.arrowSchema = arrowSchema;
    this.isLZ4Compressed = isLZ4Compressed ?? false;
  }

  public async hasMore() {
    if (!this.arrowSchema) {
      return false;
    }
    return this.source.hasMore();
  }

  public async fetchNext(options: ResultsProviderFetchNextOptions) {
    if (!this.arrowSchema) {
      return [];
    }

    const rowSet = await this.source.fetchNext(options);

    const batches: Array<Buffer> = [];
    rowSet?.arrowBatches?.forEach(({ batch }) => {
      if (batch) {
        batches.push(this.isLZ4Compressed ? LZ4.decode(batch) : batch);
      }
    });

    if (batches.length === 0) {
      return [];
    }

    return [this.arrowSchema, ...batches];
  }
}
