import { expect } from 'chai';
import fs from 'fs';
import path from 'path';
import { Table, tableFromArrays, tableToIPC, RecordBatch, TypeMap } from 'apache-arrow';
import ArrowResultConverter from '../../../lib/result/ArrowResultConverter';
import { ArrowBatch } from '../../../lib/result/utils';
import ResultsProviderStub from '../.stubs/ResultsProviderStub';
import { TStatusCode, TTableSchema, TTypeId } from '../../../thrift/TCLIService_types';

import ClientContextStub from '../.stubs/ClientContextStub';

import thriftSchemaAllNulls from './.stubs/thriftSchemaAllNulls';

function createSampleThriftSchema(columnName: string): TTableSchema {
  return {
    columns: [
      {
        columnName,
        typeDesc: {
          types: [
            {
              primitiveEntry: {
                type: TTypeId.INT_TYPE,
              },
            },
          ],
        },
        position: 1,
      },
    ],
  };
}

const sampleThriftSchema = createSampleThriftSchema('1');

const sampleArrowSchema = Buffer.from([
  255, 255, 255, 255, 208, 0, 0, 0, 16, 0, 0, 0, 0, 0, 10, 0, 14, 0, 6, 0, 13, 0, 8, 0, 10, 0, 0, 0, 0, 0, 4, 0, 16, 0,
  0, 0, 0, 1, 10, 0, 12, 0, 0, 0, 8, 0, 4, 0, 10, 0, 0, 0, 8, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 24, 0, 0, 0,
  0, 0, 18, 0, 24, 0, 20, 0, 0, 0, 19, 0, 12, 0, 0, 0, 8, 0, 4, 0, 18, 0, 0, 0, 20, 0, 0, 0, 80, 0, 0, 0, 88, 0, 0, 0,
  0, 0, 0, 2, 92, 0, 0, 0, 1, 0, 0, 0, 12, 0, 0, 0, 8, 0, 12, 0, 8, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 12, 0, 0, 0, 3, 0,
  0, 0, 73, 78, 84, 0, 22, 0, 0, 0, 83, 112, 97, 114, 107, 58, 68, 97, 116, 97, 84, 121, 112, 101, 58, 83, 113, 108, 78,
  97, 109, 101, 0, 0, 0, 0, 0, 0, 8, 0, 12, 0, 8, 0, 7, 0, 8, 0, 0, 0, 0, 0, 0, 1, 32, 0, 0, 0, 1, 0, 0, 0, 49, 0, 0, 0,
  0, 0, 0, 0,
]);

const sampleArrowBatch = [
  sampleArrowSchema,
  Buffer.from([
    255, 255, 255, 255, 136, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 12, 0, 22, 0, 14, 0, 21, 0, 16, 0, 4, 0, 12, 0, 0, 0, 16,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 16, 0, 0, 0, 0, 3, 10, 0, 24, 0, 12, 0, 8, 0, 4, 0, 10, 0, 0, 0, 20, 0, 0, 0, 56,
    0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0,
    0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
    0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
  ]),
];

const arrowBatchAllNulls = [
  fs.readFileSync(path.join(__dirname, './.stubs/arrowSchemaAllNulls.arrow')),
  fs.readFileSync(path.join(__dirname, './.stubs/dataAllNulls.arrow')),
];

const emptyItem: ArrowBatch = {
  batches: [],
  rowCount: 0,
};

function createSampleRecordBatch(start: number, count: number) {
  const table = tableFromArrays({
    id: Float64Array.from({ length: count }, (unused, index) => index + start),
  });
  return table.batches[0];
}

function createSampleArrowBatch<T extends TypeMap>(...recordBatches: RecordBatch<T>[]) {
  const table = new Table(recordBatches);
  return Buffer.from(tableToIPC(table));
}

describe('ArrowResultConverter', () => {
  it('should convert data', async () => {
    const rowSetProvider = new ResultsProviderStub(
      [
        {
          batches: sampleArrowBatch,
          rowCount: 1,
        },
      ],
      emptyItem,
    );
    const result = new ArrowResultConverter(new ClientContextStub(), rowSetProvider, {
      schema: sampleThriftSchema,
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });
    expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq([{ 1: 1 }]);
  });

  it('should return empty array if no data to process', async () => {
    const rowSetProvider = new ResultsProviderStub([], emptyItem);
    const result = new ArrowResultConverter(new ClientContextStub(), rowSetProvider, {
      schema: sampleThriftSchema,
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });
    expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq([]);
    expect(await result.hasMore()).to.be.false;
  });

  it('should return empty array if no schema available', async () => {
    const rowSetProvider = new ResultsProviderStub(
      [
        {
          batches: sampleArrowBatch,
          rowCount: 1,
        },
      ],
      emptyItem,
    );
    const result = new ArrowResultConverter(new ClientContextStub(), rowSetProvider, {
      schema: undefined,
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });
    expect(await result.hasMore()).to.be.false;
    expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq([]);
  });

  it('should detect nulls', async () => {
    const rowSetProvider = new ResultsProviderStub(
      [
        {
          batches: arrowBatchAllNulls,
          rowCount: 1,
        },
      ],
      emptyItem,
    );
    const result = new ArrowResultConverter(new ClientContextStub(), rowSetProvider, {
      schema: thriftSchemaAllNulls,
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });
    expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq([
      {
        boolean_field: null,

        tinyint_field: null,
        smallint_field: null,
        int_field: null,
        bigint_field: null,

        float_field: null,
        double_field: null,
        decimal_field: null,

        string_field: null,
        char_field: null,
        varchar_field: null,

        timestamp_field: null,
        date_field: null,
        day_interval_field: null,
        month_interval_field: null,

        binary_field: null,

        struct_field: null,
        array_field: null,
      },
    ]);
  });

  it('should respect row count in batch', async () => {
    const rowSetProvider = new ResultsProviderStub(
      [
        // First Arrow batch: contains two record batches of 5 and 5 record,
        // but declared count of rows is 8. It means that result should
        // contain all 5 records from the first record batch, but only 3 records
        // from the second record batch
        {
          batches: [createSampleArrowBatch(createSampleRecordBatch(10, 5), createSampleRecordBatch(20, 5))],
          rowCount: 8,
        },
        // Second Arrow batch: contains one record batch of 5 records.
        // Declared count of rows is 2, and only 2 rows from this batch
        // should be returned in result
        {
          batches: [createSampleArrowBatch(createSampleRecordBatch(30, 5))],
          rowCount: 2,
        },
      ],
      emptyItem,
    );
    const result = new ArrowResultConverter(new ClientContextStub(), rowSetProvider, {
      schema: createSampleThriftSchema('id'),
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    const rows1 = await result.fetchNext({ limit: 10000 });
    expect(rows1).to.deep.equal([{ id: 10 }, { id: 11 }, { id: 12 }, { id: 13 }, { id: 14 }]);
    expect(await result.hasMore()).to.be.true;

    const rows2 = await result.fetchNext({ limit: 10000 });
    expect(rows2).to.deep.equal([{ id: 20 }, { id: 21 }, { id: 22 }]);
    expect(await result.hasMore()).to.be.true;

    const rows3 = await result.fetchNext({ limit: 10000 });
    expect(rows3).to.deep.equal([{ id: 30 }, { id: 31 }]);
    expect(await result.hasMore()).to.be.false;
  });
});
