import { Buffer } from 'buffer';
import { TRowSet } from '../../thrift/TCLIService_types';
import IClientContext from '../contracts/IClientContext';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';

export default class ArrowResultHandler implements IResultsProvider<Array<Buffer>> {
  protected readonly context: IClientContext;

  private readonly source: IResultsProvider<TRowSet | undefined>;

  private readonly arrowSchema?: Buffer;

  constructor(context: IClientContext, source: IResultsProvider<TRowSet | undefined>, arrowSchema?: Buffer) {
    this.context = context;
    this.source = source;
    this.arrowSchema = arrowSchema;
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
    rowSet?.arrowBatches?.forEach((arrowBatch) => {
      if (arrowBatch.batch) {
        batches.push(arrowBatch.batch);
      }
    });

    if (batches.length === 0) {
      return [];
    }

    return [this.arrowSchema, ...batches];
  }
}
