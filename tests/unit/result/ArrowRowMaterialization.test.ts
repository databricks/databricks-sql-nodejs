import { expect } from 'chai';
import sinon from 'sinon';
import * as arrow from 'apache-arrow';
import ArrowResultConverter, { DURATION_UNIT_METADATA_KEY } from '../../../lib/result/ArrowResultConverter';
import ArrowResultHandler from '../../../lib/result/ArrowResultHandler';
import { ArrowBatch, getSchemaColumns } from '../../../lib/result/utils';
import { RowMode } from '../../../lib/contracts/IDBSQLSession';
import { TStatusCode, TTypeId } from '../../../thrift/TCLIService_types';
import ClientContextStub from '../.stubs/ClientContextStub';
import ResultsProviderStub from '../.stubs/ResultsProviderStub';
import * as fixtureArrow from '../../fixtures/compatibility/arrow';
import * as fixtureNative from '../../fixtures/compatibility/arrow_native_types';
import { fixArrowResult } from '../../fixtures/compatibility';

interface Column {
  field: arrow.Field;
  thriftType: TTypeId;
  values: any[];
  expected: any[];
  precise?: any[];
}

function ipc(columns: Column[]): Buffer {
  const fields = columns.map((column) => column.field);
  const data = arrow.makeData({
    type: new arrow.Struct(fields),
    children: columns.map((column) => arrow.vectorFromArray(column.values, column.field.type).data[0]),
    length: columns[0].values.length,
    nullCount: 0,
  });
  return Buffer.from(arrow.tableToIPC(new arrow.Table(new arrow.RecordBatch(new arrow.Schema(fields), data))));
}

function converterFor(columns: Column[], rowMode: RowMode, preserveBigNumericPrecision = false, rowCount?: number) {
  return new ArrowResultConverter(
    new ClientContextStub(),
    new ResultsProviderStub<ArrowBatch>([{ batches: [ipc(columns)], rowCount: rowCount ?? columns[0].values.length }], {
      batches: [],
      rowCount: 0,
    }),
    {
      schema: {
        columns: columns.map((column, index) => ({
          columnName: column.field.name,
          position: index + 1,
          typeDesc: { types: [{ primitiveEntry: { type: column.thriftType } }] },
        })),
      },
    },
    { rowMode, preserveBigNumericPrecision },
  );
}

function decimal(value: string): Uint32Array {
  const unsigned = BigInt.asUintN(128, BigInt(value));
  return Uint32Array.from([0, 32, 64, 96], (shift) => Number((unsigned >> BigInt(shift)) & BigInt('4294967295')));
}

describe('Arrow row materialization', () => {
  for (const rowMode of ['object', 'array'] as const) {
    for (const preservePrecision of [false, true]) {
      it(`preserves scalar values, types and nulls (${rowMode}, precision=${preservePrecision})`, async () => {
        const timestamp = new Date('2026-01-02T03:04:05.000Z');
        const columns: Column[] = [
          {
            field: new arrow.Field('boolean', new arrow.Bool()),
            thriftType: TTypeId.BOOLEAN_TYPE,
            values: [true, null, false],
            expected: [true, null, false],
          },
          ...[new arrow.Int8(), new arrow.Int16(), new arrow.Int32(), new arrow.Uint32()].map((type, index) => ({
            field: new arrow.Field(`int${index}`, type),
            thriftType: TTypeId.INT_TYPE,
            values: [0, null, 17],
            expected: [0, null, 17],
          })),
          ...[new arrow.Float32(), new arrow.Float64()].map((type, index) => ({
            field: new arrow.Field(`float${index}`, type),
            thriftType: TTypeId.DOUBLE_TYPE,
            values: [1.5, null, -2.25],
            expected: [1.5, null, -2.25],
          })),
          {
            field: new arrow.Field('big', new arrow.Int64()),
            thriftType: TTypeId.BIGINT_TYPE,
            values: [BigInt('9007199254740993'), null, BigInt('-9223372036854775808')],
            expected: [9007199254740992, null, -9223372036854775808],
            precise: [BigInt('9007199254740993'), null, BigInt('-9223372036854775808')],
          },
          {
            field: new arrow.Field('decimal', new arrow.Decimal(4, 38, 128)),
            thriftType: TTypeId.DECIMAL_TYPE,
            values: [decimal('90071992547409931234'), null, decimal('-12500')],
            expected: [Number(BigInt('90071992547409931234')) / 10000, null, -1.25],
            precise: ['9007199254740993.1234', null, '-1.2500'],
          },
          {
            field: new arrow.Field('text', new arrow.Utf8()),
            thriftType: TTypeId.STRING_TYPE,
            values: ['hello🌍', null, ''],
            expected: ['hello🌍', null, ''],
          },
          ...[new arrow.DateDay(), new arrow.DateMillisecond()].map((type, index) => ({
            field: new arrow.Field(`date${index}`, type),
            thriftType: TTypeId.DATE_TYPE,
            values: [new Date(86400000), null, new Date(0)],
            expected: [new Date(86400000), null, new Date(0)],
          })),
          ...[
            new arrow.TimestampSecond(),
            new arrow.TimestampMillisecond(),
            new arrow.TimestampMicrosecond(),
            new arrow.TimestampNanosecond(),
          ].map((type, index) => ({
            field: new arrow.Field(`timestamp${index}`, type),
            thriftType: TTypeId.TIMESTAMP_TYPE,
            values: [timestamp.getTime(), null, 0],
            expected: [timestamp, null, new Date(0)],
          })),
          ...[new arrow.Binary(), new arrow.FixedSizeBinary(3)].map((type, index) => ({
            field: new arrow.Field(`binary${index}`, type),
            thriftType: TTypeId.BINARY_TYPE,
            values: [new Uint8Array([0, 128, 255]), null, new Uint8Array([1, 2, 3])],
            expected: [Buffer.from([0, 128, 255]), null, Buffer.from([1, 2, 3])],
          })),
          {
            field: new arrow.Field('interval', new arrow.IntervalYearMonth()),
            thriftType: TTypeId.INTERVAL_YEAR_MONTH_TYPE,
            values: [[1, 2], null, [0, -1]],
            expected: ['1-2', null, '-0-1'],
          },
          ...['SECOND', 'MILLISECOND', 'MICROSECOND', 'NANOSECOND'].map((unit, index) => ({
            field: new arrow.Field(
              `duration${index}`,
              new arrow.Int64(),
              true,
              new Map([[DURATION_UNIT_METADATA_KEY, unit]]),
            ),
            thriftType: TTypeId.INTERVAL_DAY_TIME_TYPE,
            values: [BigInt(1), null, BigInt(-1)],
            expected: [
              `0 00:00:${
                index === 0 ? '01.000000000' : `00.${'0'.repeat(index * 3 - 1)}1${'0'.repeat(9 - index * 3)}`
              }`,
              null,
              `-0 00:00:${
                index === 0 ? '01.000000000' : `00.${'0'.repeat(index * 3 - 1)}1${'0'.repeat(9 - index * 3)}`
              }`,
            ],
          })),
        ];
        const result = await converterFor(columns, rowMode, preservePrecision).fetchNext({ limit: 100 });
        const expected = [0, 1, 2].map((rowIndex) => {
          const values = columns.map(
            (column) => (preservePrecision ? column.precise ?? column.expected : column.expected)[rowIndex],
          );
          return rowMode === 'array'
            ? values
            : Object.fromEntries(columns.map((column, index) => [column.field.name, values[index]]));
        });
        expect(result).to.deep.equal(expected);
        const binaryIndex = columns.findIndex((column) => column.field.name === 'binary0');
        const binary = rowMode === 'array' ? result[0][binaryIndex] : result[0].binary0;
        expect(Buffer.isBuffer(binary)).to.be.true;
      });
    }

    for (const [name, fixture] of [
      ['native', fixtureNative],
      ['string-encoded', fixtureArrow],
    ] as const) {
      it(`preserves nested containers and string encodings (${rowMode}, ${name})`, async () => {
        const context = new ClientContextStub();
        const converter = new ArrowResultConverter(
          context,
          new ArrowResultHandler(context, new ResultsProviderStub(fixture.rowSets, undefined), {
            arrowSchema: fixture.arrowSchema,
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
          }),
          { schema: fixture.schema },
          { rowMode },
        );
        const rows = await converter.fetchNext({ limit: 10000 });
        const columns = getSchemaColumns(fixture.schema);
        const asObjects =
          rowMode === 'array'
            ? rows.map((row) => Object.fromEntries(columns.map((column, index) => [column.columnName, row[index]])))
            : rows;
        for (const row of asObjects) if (row.flt !== null) row.flt = Number(row.flt.toFixed(4));
        expect(asObjects).to.deep.equal(fixArrowResult(fixture.expected));
      });
    }

    it(`resolves each column once per batch, not per row (${rowMode})`, async () => {
      const columns: Column[] = [
        {
          field: new arrow.Field('value', new arrow.Int32()),
          thriftType: TTypeId.INT_TYPE,
          values: [1, 2, 3],
          expected: [1, 2, 3],
        },
      ];
      const converter = converterFor(columns, rowMode, false, 2);
      const spy = sinon.spy(converter as any, 'getArrowValueConverter');
      expect(await converter.fetchNext({ limit: 100 })).to.deep.equal(
        rowMode === 'array' ? [[1], [2]] : [{ value: 1 }, { value: 2 }],
      );
      expect(spy.callCount).to.equal(1);
      expect(await converter.hasMore()).to.equal(false);
    });

    it(`rebuilds converters when a later batch changes its Arrow type (${rowMode})`, async () => {
      const columns: Column[] = [
        {
          field: new arrow.Field('value', new arrow.Int32()),
          thriftType: TTypeId.TIMESTAMP_TYPE,
          values: [7, null],
          expected: [7, null],
        },
      ];
      const converter = new ArrowResultConverter(
        new ClientContextStub(),
        new ResultsProviderStub<ArrowBatch>(
          [
            { batches: [ipc(columns)], rowCount: 2 },
            {
              batches: [ipc([{ ...columns[0], field: new arrow.Field('value', new arrow.TimestampMillisecond()) }])],
              rowCount: 2,
            },
          ],
          { batches: [], rowCount: 0 },
        ),
        {
          schema: {
            columns: [
              {
                columnName: 'value',
                position: 1,
                typeDesc: { types: [{ primitiveEntry: { type: TTypeId.TIMESTAMP_TYPE } }] },
              },
            ],
          },
        },
        { rowMode },
      );
      expect(await converter.fetchNext({ limit: 100 })).to.deep.equal(
        rowMode === 'array' ? [[7], [null]] : [{ value: 7 }, { value: null }],
      );
      expect(await converter.fetchNext({ limit: 100 })).to.deep.equal(
        rowMode === 'array' ? [[new Date(7)], [null]] : [{ value: new Date(7) }, { value: null }],
      );
    });

    it(`keeps dictionary conversion semantics (${rowMode})`, async () => {
      const columns: Column[] = [
        {
          field: new arrow.Field('value', new arrow.Dictionary(new arrow.TimestampMillisecond(), new arrow.Int32())),
          thriftType: TTypeId.TIMESTAMP_TYPE,
          values: [7, null, 7],
          expected: [7, null, 7],
        },
      ];
      expect(await converterFor(columns, rowMode).fetchNext({ limit: 100 })).to.deep.equal(
        rowMode === 'array' ? [[7], [null], [7]] : [{ value: 7 }, { value: null }, { value: 7 }],
      );
    });

    it(`copies binary values out of the batch (${rowMode})`, async () => {
      const columns: Column[] = [
        {
          field: new arrow.Field('value', new arrow.Binary()),
          thriftType: TTypeId.BINARY_TYPE,
          values: [new Uint8Array([1, 2])],
          expected: [Buffer.from([1, 2])],
        },
      ];
      const converter = converterFor(columns, rowMode);
      const batch = arrow.tableFromIPC(ipc(columns)).batches[0];
      const first = (converter as any).getRows(batch, 1);
      const firstValue = rowMode === 'array' ? first[0][0] : first[0].value;
      firstValue[0] = 255;
      expect(batch.getChildAt(0)?.get(0)).to.deep.equal(new Uint8Array([1, 2]));
    });
  }

  it('retains the legacy __proto__ column behavior in object mode', async () => {
    const columns: Column[] = [
      {
        field: new arrow.Field('__proto__', new arrow.Int32()),
        thriftType: TTypeId.INT_TYPE,
        values: [7],
        expected: [7],
      },
    ];
    const [row] = await converterFor(columns, 'object').fetchNext({ limit: 100 });
    expect(Object.keys(row)).to.deep.equal([]);
    expect(Object.getPrototypeOf(row)).to.equal(Object.prototype);
  });
});
