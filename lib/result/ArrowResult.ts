import { Buffer } from 'buffer';
import { tableFromIPC } from 'apache-arrow';
import { TRowSet } from '../../thrift/TCLIService_types';
import IOperationResult from './IOperationResult';

export default class ArrowResult implements IOperationResult {
  private readonly schema?: Buffer;

  constructor(schema?: Buffer) {
    this.schema = schema;
  }

  getValue(data?: Array<TRowSet>) {
    if (!this.schema || !data) {
      return [];
    }

    const batches = this.getBatches(data);
    if (batches.length === 0) {
      return [];
    }

    const table = tableFromIPC([this.schema, ...batches]);
    return table.toArray();
  }

  private getBatches(data: Array<TRowSet>): Array<Buffer> {
    const result: Array<Buffer> = [];

    data.forEach((rowSet) => {
      rowSet.arrowBatches?.forEach((arrowBatch) => {
        if (arrowBatch.batch) {
          result.push(arrowBatch.batch);
        }
      });
    });

    return result;
  }
}
