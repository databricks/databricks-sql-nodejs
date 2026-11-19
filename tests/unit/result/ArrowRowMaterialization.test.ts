import { deepStrictEqual } from 'assert';
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
  const uniqueFields = fields.map(
    (field, index) => new arrow.Field(`column${index}`, field.type, field.nullable, field.metadata),
  );
  const data = arrow.makeData({
    type: new arrow.Struct(uniqueFields),
    children: columns.map((column) => arrow.vectorFromArray(column.values, column.field.type).data[0]),
    length: columns[0].values.length,
    nullCount: 0,
  });
  const batch = new arrow.RecordBatch(new arrow.Schema(uniqueFields), data);
  Object.assign(batch, { schema: new arrow.Schema(fields) });
  const writer = new arrow.RecordBatchStreamWriter();
  writer.write(batch);
  writer.finish();
  return Buffer.from(writer.toUint8Array(true));
}

function converterFor(
  columns: Column[],
  rowMode: RowMode,
  preserveBigNumericPrecision = false,
  rowCount?: number,
  schemaColumns: readonly Column[] = columns,
) {
  return new ArrowResultConverter(
    new ClientContextStub(),
    new ResultsProviderStub<ArrowBatch>([{ batches: [ipc(columns)], rowCount: rowCount ?? columns[0].values.length }], {
      batches: [],
      rowCount: 0,
    }),
    {
      schema: {
        columns: schemaColumns.map((column, index) => ({
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
    it(`avoids decimal string conversion for safe signed values (${rowMode})`, () => {
      const unscaled = ['0', '1', '-1', '4294967297', '-4294967297', '9007199254740991', '-9007199254740991'];
      const columns: Column[] = [
        {
          field: new arrow.Field('amount', new arrow.Decimal(2, 18, 128)),
          thriftType: TTypeId.DECIMAL_TYPE,
          values: unscaled.map(decimal),
          expected: unscaled.map((value) => Number(value) / 100),
        },
      ];
      const batch = arrow.tableFromIPC(ipc(columns)).batches[0];
      const vector = batch.getChildAt(0)!;
      const values = unscaled.map((value, index) => vector.get(index));
      const byteLengthReads = values.map((value) => {
        const read = sinon.stub().returns(value.byteLength);
        Object.defineProperty(value, 'byteLength', { get: read });
        return read;
      });
      const sandbox = sinon.createSandbox();
      try {
        sandbox.stub(vector, 'get').callsFake((index) => values[index]);
        sandbox.stub(batch, 'getChildAt').returns(vector);
        const converter = converterFor(columns, rowMode);
        deepStrictEqual(
          (converter as any).getRows(batch, values.length),
          columns[0].expected.map((amount) => (rowMode === 'array' ? [amount] : { amount })),
        );
        byteLengthReads.forEach((read) => sinon.assert.notCalled(read));
      } finally {
        sandbox.restore();
      }
    });

    it(`preserves Decimal128 rounding, signs and scales (${rowMode})`, async () => {
      const unscaled = [
        '0',
        '1',
        '-1',
        '4294967295',
        '-4294967295',
        '4294967296',
        '-4294967296',
        '9007199254740991',
        '-9007199254740991',
        '9007199254740992',
        '-9007199254740992',
        '9007199254740993',
        '-9007199254740993',
        '18446744073709551615',
        '-18446744073709551615',
        '18446744073709551616',
        '-18446744073709551616',
        '79228162514264337593543950336',
        '-79228162514264337593543950336',
        '99999999999999999999999999999999999999',
        '-99999999999999999999999999999999999999',
        null,
      ];
      const columns: Column[] = [-2, 0, 2, 18, 38].map((scale) => ({
        field: new arrow.Field(`decimal${scale}`, new arrow.Decimal(scale, 38, 128)),
        thriftType: TTypeId.DECIMAL_TYPE,
        values: unscaled.map((value) => (value === null ? null : decimal(value))),
        expected: unscaled.map((value) => (value === null ? null : Number(value) / 10 ** scale)),
      }));
      const converter = converterFor(columns, rowMode);
      const expected = unscaled.map((value, rowIndex) =>
        rowMode === 'array'
          ? columns.map((column) => column.expected[rowIndex])
          : Object.fromEntries(columns.map((column) => [column.field.name, column.expected[rowIndex]])),
      );
      deepStrictEqual(await converter.fetchNext({ limit: 100 }), expected);
      const batch = arrow.tableFromIPC(ipc(columns)).batches[0];
      deepStrictEqual((converter as any).getRows(batch.slice(3, 15), 8), expected.slice(3, 11));
      const legacyRows = (converter as any).getObjectRows(batch.schema, batch.toArray());
      deepStrictEqual(
        rowMode === 'array'
          ? legacyRows.map((row: any) => columns.map((column) => row[column.field.name]))
          : legacyRows,
        expected,
      );
    });

    it(`preserves nested numeric decimals and nulls (${rowMode})`, async () => {
      const amount = new arrow.Decimal(2, 38, 128);
      const array = new arrow.List(new arrow.Field('element', amount));
      const columns: Column[] = [
        {
          field: new arrow.Field('array', array),
          thriftType: TTypeId.ARRAY_TYPE,
          values: [[decimal('-4294967297'), null, decimal('9007199254740993')], null, []],
          expected: [[-42949672.97, null, Number('9007199254740993') / 100], null, []],
        },
        {
          field: new arrow.Field(
            'struct',
            new arrow.Struct([new arrow.Field('amount', amount), new arrow.Field('items', array)]),
          ),
          thriftType: TTypeId.STRUCT_TYPE,
          values: [{ amount: decimal('-1'), items: [null, decimal('4294967296')] }, null, { amount: null, items: [] }],
          expected: [{ amount: -0.01, items: [null, 42949672.96] }, null, { amount: null, items: [] }],
        },
        {
          field: new arrow.Field(
            'map',
            new arrow.Map_(
              new arrow.Field(
                'entries',
                new arrow.Struct<{ key: arrow.Utf8; value: arrow.Decimal }>([
                  new arrow.Field('key', new arrow.Utf8()),
                  new arrow.Field('value', amount),
                ]),
              ),
            ),
          ),
          thriftType: TTypeId.MAP_TYPE,
          values: [
            new Map([
              ['negative', decimal('-9007199254740991')],
              ['null', null],
            ]),
            null,
            new Map(),
          ],
          expected: [{ negative: Number('-9007199254740991') / 100, null: null }, null, {}],
        },
      ];
      deepStrictEqual(
        await converterFor(columns, rowMode).fetchNext({ limit: 100 }),
        [0, 1, 2].map((rowIndex) =>
          rowMode === 'array'
            ? columns.map((column) => column.expected[rowIndex])
            : Object.fromEntries(columns.map((column) => [column.field.name, column.expected[rowIndex]])),
        ),
      );
    });

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
        const converter = converterFor(columns, rowMode, preservePrecision);
        const result = await converter.fetchNext({ limit: 100 });
        const expected = [0, 1, 2].map((rowIndex) => {
          const values = columns.map(
            (column) => (preservePrecision ? column.precise ?? column.expected : column.expected)[rowIndex],
          );
          return rowMode === 'array'
            ? values
            : Object.fromEntries(columns.map((column, index) => [column.field.name, values[index]]));
        });
        deepStrictEqual(result, expected);
        const table = arrow.tableFromIPC(ipc(columns));
        const legacyRows = (converter as any).getObjectRows(table.schema, table.toArray());
        deepStrictEqual(
          result,
          rowMode === 'array'
            ? legacyRows.map((row: any) => columns.map((column) => row[column.field.name]))
            : legacyRows,
        );
      });

      it(`retains complex and user-defined Thrift casts (${rowMode}, precision=${preservePrecision})`, async () => {
        const columns: Column[] = [
          {
            field: new arrow.Field('array', new arrow.Utf8()),
            thriftType: TTypeId.ARRAY_TYPE,
            values: ['[1,null]', null, '[]'],
            expected: [[1, null], null, []],
          },
          ...[TTypeId.MAP_TYPE, TTypeId.STRUCT_TYPE].map((thriftType) => ({
            field: new arrow.Field(`complex${thriftType}`, new arrow.Utf8()),
            thriftType,
            values: ['{"value":7}', null, '{}'],
            expected: [{ value: 7 }, null, {}],
          })),
          ...[TTypeId.USER_DEFINED_TYPE, TTypeId.UNION_TYPE].map((thriftType) => ({
            field: new arrow.Field(`custom${thriftType}`, new arrow.Int32()),
            thriftType,
            values: [7, null, -1],
            expected: ['7', null, '-1'],
          })),
        ];
        const rows = await converterFor(columns, rowMode, preservePrecision).fetchNext({ limit: 100 });
        deepStrictEqual(
          rows,
          [0, 1, 2].map((rowIndex) =>
            rowMode === 'array'
              ? columns.map((column) => column.expected[rowIndex])
              : Object.fromEntries(columns.map((column) => [column.field.name, column.expected[rowIndex]])),
          ),
        );
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

    for (const numeric of [
      {
        name: 'BIGINT',
        type: () => new arrow.Int64(),
        thriftType: TTypeId.BIGINT_TYPE,
        values: [BigInt('9007199254740993'), BigInt('-9223372036854775808'), BigInt(0)],
        expected: [BigInt('9007199254740993'), BigInt('-9223372036854775808'), BigInt(0)],
      },
      {
        name: 'DECIMAL',
        type: () => new arrow.Decimal(4, 38, 128),
        thriftType: TTypeId.DECIMAL_TYPE,
        values: [decimal('90071992547409931234'), decimal('-12500'), decimal('0')],
        expected: ['9007199254740993.1234', '-1.2500', '0.0000'],
        // Dictionary decimals retain the legacy unscaled string representation.
        dictionaryExpected: ['90071992547409931234', '-12500', '0'],
      },
    ]) {
      for (const dictionaryEncoded of [false, true]) {
        it(`matches legacy precise ${numeric.name} in scalars and containers (${rowMode}, dictionary=${dictionaryEncoded})`, async () => {
          const type = () =>
            dictionaryEncoded ? new arrow.Dictionary(numeric.type(), new arrow.Int32()) : numeric.type();
          const [positive, negative, zero] = numeric.values;
          const [expectedPositive, expectedNegative, expectedZero] =
            (dictionaryEncoded && numeric.dictionaryExpected) || numeric.expected;
          const columns: Column[] = [
            {
              field: new arrow.Field('scalar', type(), true),
              thriftType: numeric.thriftType,
              values: [positive, null, negative, positive, zero],
              expected: [expectedPositive, null, expectedNegative, expectedPositive, expectedZero],
            },
            {
              field: new arrow.Field('struct', new arrow.Struct([new arrow.Field('value', type(), true)]), true),
              thriftType: TTypeId.STRUCT_TYPE,
              values: [{ value: positive }, null, { value: null }, { value: negative }, { value: zero }],
              expected: [
                { value: expectedPositive },
                null,
                { value: null },
                { value: expectedNegative },
                { value: expectedZero },
              ],
            },
            {
              field: new arrow.Field('list', new arrow.List(new arrow.Field('element', type(), true)), true),
              thriftType: TTypeId.ARRAY_TYPE,
              values: [[positive, null, negative, positive, zero], null, [], [null], [negative]],
              expected: [
                [expectedPositive, null, expectedNegative, expectedPositive, expectedZero],
                null,
                [],
                [null],
                [expectedNegative],
              ],
            },
            {
              field: new arrow.Field(
                'map',
                new arrow.Map_(
                  new arrow.Field(
                    'entries',
                    new arrow.Struct<{ key: arrow.Utf8; value: ReturnType<typeof type> }>([
                      new arrow.Field('key', new arrow.Utf8(), false),
                      new arrow.Field('value', type(), true),
                    ]),
                    false,
                  ),
                ),
                true,
              ),
              thriftType: TTypeId.MAP_TYPE,
              values: [
                new Map<string, any>([
                  ['positive', positive],
                  ['missing', null],
                  ['negative', negative],
                  ['repeated', positive],
                  ['zero', zero],
                ]),
                null,
                new Map(),
                new Map([['missing', null]]),
                new Map([['negative', negative]]),
              ],
              expected: [
                {
                  positive: expectedPositive,
                  missing: null,
                  negative: expectedNegative,
                  repeated: expectedPositive,
                  zero: expectedZero,
                },
                null,
                {},
                { missing: null },
                { negative: expectedNegative },
              ],
            },
          ];
          const converter = converterFor(columns, rowMode, true);
          const rows = await converter.fetchNext({ limit: 100 });
          const expected = columns[0].values.map((_, rowIndex) => {
            const values = columns.map((column) => column.expected[rowIndex]);
            return rowMode === 'array'
              ? values
              : Object.fromEntries(columns.map((column, index) => [column.field.name, values[index]]));
          });
          deepStrictEqual(rows, expected);
          const table = arrow.tableFromIPC(ipc(columns));
          const legacyRows = (converter as any).getObjectRows(table.schema, table.toArray());
          deepStrictEqual(
            rows,
            rowMode === 'array'
              ? legacyRows.map((row: any) => columns.map((column) => row[column.field.name]))
              : legacyRows,
          );
        });
      }
    }

    for (const type of [new arrow.Binary(), new arrow.FixedSizeBinary(2)]) {
      it(`copies ${type} values out of the batch (${rowMode})`, async () => {
        const columns: Column[] = [
          {
            field: new arrow.Field('value', type),
            thriftType: TTypeId.BINARY_TYPE,
            values: [new Uint8Array([1, 2])],
            expected: [Buffer.from([1, 2])],
          },
        ];
        const converter = converterFor(columns, rowMode);
        const batch = arrow.tableFromIPC(ipc(columns)).batches[0];
        const first = (converter as any).getRows(batch, 1);
        const firstValue = rowMode === 'array' ? first[0][0] : first[0].value;
        expect(Buffer.isBuffer(firstValue)).to.be.true;
        firstValue[0] = 255;
        expect(batch.getChildAt(0)?.get(0)).to.deep.equal(new Uint8Array([1, 2]));
      });
    }
  }

  for (const column of [
    {
      field: new arrow.Field('__proto__', new arrow.Int32()),
      thriftType: TTypeId.INT_TYPE,
      values: [null, 7],
      expected: [null, 7],
    },
    {
      field: new arrow.Field('__proto__', new arrow.TimestampMillisecond()),
      thriftType: TTypeId.TIMESTAMP_TYPE,
      values: [null, 7],
      expected: [null, new Date(7)],
    },
    {
      field: new arrow.Field('__proto__', new arrow.Struct([new arrow.Field('value', new arrow.Int32())])),
      thriftType: TTypeId.STRUCT_TYPE,
      values: [null, { value: 7 }],
      expected: [null, { value: 7 }],
    },
  ]) {
    it(`retains the legacy __proto__ behavior for ${column.field.type} in object mode`, async () => {
      const converter = converterFor([column], 'object');
      const rows = await converter.fetchNext({ limit: 100 });
      const structColumn = arrow.DataType.isStruct(column.field.type);
      deepStrictEqual(
        rows.map((row) => ({
          hasPlainPrototype: Object.getPrototypeOf(row) === Object.prototype,
          hasStructRowPrototype: Object.getPrototypeOf(row) === arrow.StructRow.prototype,
          keys: Object.keys(row),
          value: row.value,
        })),
        [
          { hasPlainPrototype: true, hasStructRowPrototype: false, keys: [], value: undefined },
          {
            hasPlainPrototype: !structColumn,
            hasStructRowPrototype: structColumn,
            keys: [],
            value: undefined,
          },
        ],
      );
    });
  }

  const ordinaryColumns: Column[] = [
    {
      field: new arrow.Field('number', new arrow.Float64()),
      thriftType: TTypeId.DOUBLE_TYPE,
      values: [7, null],
      expected: [7, null],
    },
    {
      field: new arrow.Field('text', new arrow.Utf8()),
      thriftType: TTypeId.STRING_TYPE,
      values: ['value', null],
      expected: ['value', null],
    },
  ];
  const duplicateColumns: Column[] = [
    { ...ordinaryColumns[0], field: new arrow.Field('same', new arrow.Float64()), values: [7.5, null] },
    {
      field: new arrow.Field('same', new arrow.Int64(), true, new Map([[DURATION_UNIT_METADATA_KEY, 'SECOND']])),
      thriftType: TTypeId.INTERVAL_DAY_TIME_TYPE,
      values: [BigInt(1), null],
      expected: ['0 00:00:01.000000000', null],
    },
  ];
  for (const [name, columns, schemaColumns] of [
    ['duplicate names with different types', duplicateColumns, duplicateColumns],
    ['permuted Thrift names', ordinaryColumns, [...ordinaryColumns].reverse()],
    ['narrower Thrift schema', ordinaryColumns, ordinaryColumns.slice(0, 1)],
    [
      'wider Thrift schema',
      ordinaryColumns,
      [...ordinaryColumns, { ...ordinaryColumns[0], field: new arrow.Field('extra', new arrow.Float64()) }],
    ],
  ] as const) {
    it(`retains legacy object-mode conversion for ${name}`, async () => {
      const converter = converterFor(columns, 'object', false, undefined, schemaColumns);
      const table = arrow.tableFromIPC(ipc(columns));
      const expected = (converter as any).getObjectRows(table.schema, table.toArray());
      const fallback = sinon.spy(converter as any, 'getObjectRows');
      const rows = await converter.fetchNext({ limit: 100 });
      sinon.assert.calledOnce(fallback);
      deepStrictEqual(rows, expected);
      rows.forEach((row, index) => {
        deepStrictEqual(Object.keys(row), Object.keys(expected[index]));
        expect(Object.getPrototypeOf(row)).to.equal(Object.prototype);
      });
    });
  }
});
