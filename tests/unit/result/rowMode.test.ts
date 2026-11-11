import { expect } from 'chai';
import sinon from 'sinon';
import Int64 from 'node-int64';
import {
  Decimal,
  Field,
  Float64,
  Int32,
  Int64 as ArrowInt64,
  makeData,
  RecordBatch,
  RecordBatchStreamWriter,
  Schema,
  Struct,
  TimestampMillisecond,
  Utf8,
  vectorFromArray,
} from 'apache-arrow';
import {
  TColumnDesc,
  TRowSet,
  TSparkRowSetType,
  TStatusCode,
  TTableSchema,
  TTypeId,
} from '../../../thrift/TCLIService_types';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import ArrowResultConverter from '../../../lib/result/ArrowResultConverter';
import ArrowResultHandler from '../../../lib/result/ArrowResultHandler';
import JsonResultHandler from '../../../lib/result/JsonResultHandler';
import { ArrowBatch, getSchemaColumns } from '../../../lib/result/utils';
import KernelOperationBackend from '../../../lib/kernel/KernelOperationBackend';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import ResultsProviderStub from '../.stubs/ResultsProviderStub';
import ClientContextStub from '../.stubs/ClientContextStub';
import { createSessionForTest } from '../.stubs/createSessionForTest';
import { fixArrowResult } from '../../fixtures/compatibility';
import * as fixtureColumn from '../../fixtures/compatibility/column';
import * as fixtureArrow from '../../fixtures/compatibility/arrow';
import * as fixtureArrowNT from '../../fixtures/compatibility/arrow_native_types';

function column(columnName: string, type: TTypeId, position: number): TColumnDesc {
  return { columnName, typeDesc: { types: [{ primitiveEntry: { type } }] }, position };
}

// `SELECT carat AS a, color AS a` from the issue report.
const duplicateThriftSchema: TTableSchema = {
  columns: [column('a', TTypeId.DOUBLE_TYPE, 1), column('a', TTypeId.STRING_TYPE, 2)],
};

const duplicateArrowSchema = new Schema([new Field('a', new Float64(), true), new Field('a', new Utf8(), true)]);

function arrowIpc(schema: Schema, columns: Array<Array<unknown>>): Buffer {
  // `new RecordBatch(schema, ...)` merges same-named fields, so build it with unique
  // names and attach the real schema before serializing.
  const uniqueFields = schema.fields.map((field, i) => new Field(`c${i}`, field.type, field.nullable, field.metadata));
  const children = schema.fields.map((field, i) => vectorFromArray(columns[i], field.type).data[0]);
  const struct = makeData({
    type: new Struct(uniqueFields),
    children,
    length: children[0]?.length ?? 0,
    nullCount: 0,
  });
  const batch = new RecordBatch(new Schema(uniqueFields), struct);
  Object.assign(batch, { schema });
  const writer = new RecordBatchStreamWriter();
  writer.write(batch);
  writer.finish();
  return Buffer.from(writer.toUint8Array(true));
}

function duplicateRowSet(): TRowSet {
  return {
    startRowOffset: new Int64(0),
    rows: [],
    columns: [
      { doubleVal: { values: [0.23, 0.21], nulls: Buffer.from([]) } },
      { stringVal: { values: ['E', 'F'], nulls: Buffer.from([0b10]) } },
    ],
  };
}

function toArrays(rows: Array<Record<string, unknown>>, schema: TTableSchema) {
  const columns = getSchemaColumns(schema);
  return rows.map((row) => columns.map((c) => row[c.columnName]));
}

function warnings(context: ClientContextStub, spy: sinon.SinonSpy) {
  return spy.getCalls().filter((call) => call.args[0] === LogLevel.warn && /duplicate column/i.test(call.args[1]));
}

describe('rowMode', () => {
  describe('JsonResultHandler', () => {
    it('keeps every column of duplicate-named columns in array mode', async () => {
      const handler = new JsonResultHandler(
        new ClientContextStub(),
        new ResultsProviderStub([duplicateRowSet()], undefined),
        { schema: duplicateThriftSchema, status: { statusCode: TStatusCode.SUCCESS_STATUS } },
        { rowMode: 'array' },
      );
      expect(await handler.fetchNext({ limit: 100 })).to.deep.equal([
        [0.23, 'E'],
        [0.21, null],
      ]);
    });

    it('orders array values by column position', async () => {
      const schema: TTableSchema = {
        columns: [column('second', TTypeId.STRING_TYPE, 2), column('first', TTypeId.INT_TYPE, 1)],
      };
      const rowSet: TRowSet = {
        startRowOffset: new Int64(0),
        rows: [],
        columns: [
          { i32Val: { values: [1, 2], nulls: Buffer.from([]) } },
          { stringVal: { values: ['x', 'y'], nulls: Buffer.from([]) } },
        ],
      };
      const handler = new JsonResultHandler(
        new ClientContextStub(),
        new ResultsProviderStub([rowSet], undefined),
        { schema, status: { statusCode: TStatusCode.SUCCESS_STATUS } },
        { rowMode: 'array' },
      );
      expect(await handler.fetchNext({ limit: 100 })).to.deep.equal([
        [1, 'x'],
        [2, 'y'],
      ]);
    });

    it('keeps the default object rows unchanged', async () => {
      const handler = new JsonResultHandler(
        new ClientContextStub(),
        new ResultsProviderStub([duplicateRowSet()], undefined),
        { schema: duplicateThriftSchema, status: { statusCode: TStatusCode.SUCCESS_STATUS } },
      );
      expect(await handler.fetchNext({ limit: 100 })).to.deep.equal([{ a: 'E' }, { a: null }]);
    });

    it('matches the column-based compatibility fixture', async () => {
      const handler = new JsonResultHandler(
        new ClientContextStub(),
        new ResultsProviderStub(fixtureColumn.rowSets, undefined),
        { schema: fixtureColumn.schema, status: { statusCode: TStatusCode.SUCCESS_STATUS } },
        { rowMode: 'array' },
      );
      const rows = await handler.fetchNext({ limit: 10000 });
      expect(rows).to.deep.equal(toArrays(fixtureColumn.expected, fixtureColumn.schema));
    });
  });

  describe('ArrowResultConverter', () => {
    const duplicateBatch: ArrowBatch = {
      batches: [
        arrowIpc(duplicateArrowSchema, [
          [0.23, 0.21],
          ['E', null],
        ]),
      ],
      rowCount: 2,
    };

    it('keeps every column of duplicate-named columns in array mode', async () => {
      const converter = new ArrowResultConverter(
        new ClientContextStub(),
        new ResultsProviderStub([duplicateBatch], { batches: [], rowCount: 0 }),
        { schema: duplicateThriftSchema },
        { rowMode: 'array' },
      );
      expect(await converter.fetchNext({ limit: 100 })).to.deep.equal([
        [0.23, 'E'],
        [0.21, null],
      ]);
    });

    it('converts each same-named column with its own type', async () => {
      const ts = new Date('2026-01-02T03:04:05.000Z');
      const schema = new Schema([new Field('a', new TimestampMillisecond(), true), new Field('a', new Utf8(), true)]);
      const converter = new ArrowResultConverter(
        new ClientContextStub(),
        new ResultsProviderStub([{ batches: [arrowIpc(schema, [[ts.valueOf()], ['x']])], rowCount: 1 }], {
          batches: [],
          rowCount: 0,
        }),
        { schema: { columns: [column('a', TTypeId.TIMESTAMP_TYPE, 1), column('a', TTypeId.STRING_TYPE, 2)] } },
        { rowMode: 'array' },
      );
      expect(await converter.fetchNext({ limit: 100 })).to.deep.equal([[ts, 'x']]);
    });

    it('keeps the default object rows unchanged', async () => {
      const converter = new ArrowResultConverter(
        new ClientContextStub(),
        new ResultsProviderStub([duplicateBatch], { batches: [], rowCount: 0 }),
        { schema: duplicateThriftSchema },
      );
      expect(await converter.fetchNext({ limit: 100 })).to.deep.equal([{ a: 'E' }, { a: null }]);
    });

    it('honours the batch row count across multiple record batches', async () => {
      const schema = new Schema([new Field('id', new Int32(), true)]);
      const thriftSchema: TTableSchema = { columns: [column('id', TTypeId.INT_TYPE, 1)] };
      const converter = new ArrowResultConverter(
        new ClientContextStub(),
        new ResultsProviderStub(
          [
            { batches: [arrowIpc(schema, [[1, 2]])], rowCount: 2 },
            // The server may send more rows than `rowCount` covers; extra rows are dropped.
            { batches: [arrowIpc(schema, [[3, 4, 5]])], rowCount: 2 },
          ],
          { batches: [], rowCount: 0 },
        ),
        { schema: thriftSchema },
        { rowMode: 'array' },
      );
      expect(await converter.fetchNext({ limit: 100 })).to.deep.equal([[1], [2]]);
      expect(await converter.fetchNext({ limit: 100 })).to.deep.equal([[3], [4]]);
      expect(await converter.hasMore()).to.be.false;
    });

    it('honours preserveBigNumericPrecision', async () => {
      const schema = new Schema([
        new Field('n', new ArrowInt64(), true),
        new Field('n', new Decimal(2, 20, 128), true),
      ]);
      const unscaled = new Uint32Array([150, 0, 0, 0]);
      const converter = new ArrowResultConverter(
        new ClientContextStub(),
        new ResultsProviderStub(
          [{ batches: [arrowIpc(schema, [[BigInt('9007199254740993')], [unscaled]])], rowCount: 1 }],
          { batches: [], rowCount: 0 },
        ),
        { schema: { columns: [column('n', TTypeId.BIGINT_TYPE, 1), column('n', TTypeId.DECIMAL_TYPE, 2)] } },
        { rowMode: 'array', preserveBigNumericPrecision: true },
      );
      expect(await converter.fetchNext({ limit: 100 })).to.deep.equal([[BigInt('9007199254740993'), '1.50']]);
    });

    it('still returns null placeholders when row materialization is disabled', async () => {
      const converter = new ArrowResultConverter(
        new ClientContextStub(),
        new ResultsProviderStub([duplicateBatch], { batches: [], rowCount: 0 }),
        { schema: duplicateThriftSchema },
        { rowMode: 'array', disableRowMaterialization: true },
      );
      expect(await converter.fetchNext({ limit: 100 })).to.deep.equal([null, null]);
    });

    for (const [name, fixture] of [
      ['without native types', fixtureArrow],
      ['with native types', fixtureArrowNT],
    ] as const) {
      it(`matches the arrow-based compatibility fixture ${name}`, async () => {
        const context = new ClientContextStub();
        const converter = new ArrowResultConverter(
          context,
          new ArrowResultHandler(context, new ResultsProviderStub(fixture.rowSets, undefined), {
            arrowSchema: fixture.arrowSchema,
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
          }),
          { schema: fixture.schema },
          { rowMode: 'array' },
        );
        const rows = (await converter.fetchNext({ limit: 10000 })) as Array<Array<unknown>>;
        const flt = getSchemaColumns(fixture.schema).findIndex((c) => c.columnName === 'flt');
        const fixed = rows.map((row) =>
          row.map((value, i) => (i === flt ? Number((value as number).toFixed(4)) : value)),
        );
        expect(fixed).to.deep.equal(toArrays(fixArrowResult(fixture.expected), fixture.schema));
      });
    }
  });

  describe('duplicate column warning', () => {
    it('warns once per result set in object mode', async () => {
      const context = new ClientContextStub();
      const spy = sinon.spy(context.logger, 'log');
      const handler = new JsonResultHandler(
        context,
        new ResultsProviderStub([duplicateRowSet(), duplicateRowSet()], undefined),
        { schema: duplicateThriftSchema, status: { statusCode: TStatusCode.SUCCESS_STATUS } },
      );
      await handler.fetchNext({ limit: 100 });
      await handler.fetchNext({ limit: 100 });
      const calls = warnings(context, spy);
      expect(calls).to.have.length(1);
      expect(calls[0].args[1]).to.contain('"a"');
      expect(calls[0].args[1]).to.contain("rowMode: 'array'");

      const arrowContext = new ClientContextStub();
      const arrowSpy = sinon.spy(arrowContext.logger, 'log');
      // eslint-disable-next-line no-new
      new ArrowResultConverter(arrowContext, new ResultsProviderStub([], { batches: [], rowCount: 0 }), {
        schema: duplicateThriftSchema,
      });
      expect(warnings(arrowContext, arrowSpy)).to.have.length(1);
    });

    it('does not warn in array mode or for unique column names', async () => {
      const context = new ClientContextStub();
      const spy = sinon.spy(context.logger, 'log');
      // eslint-disable-next-line no-new
      new JsonResultHandler(
        context,
        new ResultsProviderStub([], undefined),
        { schema: duplicateThriftSchema, status: { statusCode: TStatusCode.SUCCESS_STATUS } },
        { rowMode: 'array' },
      );
      // eslint-disable-next-line no-new
      new ArrowResultConverter(
        context,
        new ResultsProviderStub([], { batches: [], rowCount: 0 }),
        { schema: duplicateThriftSchema },
        { rowMode: 'array' },
      );
      // eslint-disable-next-line no-new
      new JsonResultHandler(context, new ResultsProviderStub([], undefined), {
        schema: fixtureColumn.schema,
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
      });
      expect(warnings(context, spy)).to.have.length(0);
    });
  });

  describe('executeStatement', () => {
    // RowSetProvider treats a non-empty page as "more rows", so serve one page then an empty one.
    function serveRowSet(context: ClientContextStub, results: TRowSet) {
      const empty = {
        ...results,
        columns: results.columns?.map(() => ({ stringVal: { values: [], nulls: Buffer.from([]) } })),
      };
      sinon
        .stub(context.driver, 'fetchResults')
        .resolves({ status: { statusCode: TStatusCode.SUCCESS_STATUS }, hasMoreRows: false, results: empty })
        .onFirstCall()
        .resolves({ status: { statusCode: TStatusCode.SUCCESS_STATUS }, hasMoreRows: false, results });
    }

    function createDuplicateColumnSession(context: ClientContextStub) {
      context.driver.executeStatementResp.operationHandle!.hasResultSet = true;
      context.driver.getResultSetMetadataResp = {
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
        resultFormat: TSparkRowSetType.COLUMN_BASED_SET,
        schema: duplicateThriftSchema,
      };
      return createSessionForTest({
        handle: { sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) } },
        context,
      });
    }

    it('returns array rows from the Thrift backend', async () => {
      const context = new ClientContextStub();
      const session = createDuplicateColumnSession(context);
      serveRowSet(context, duplicateRowSet());
      const operation = await session.executeStatement('SELECT carat AS a, color AS a', { rowMode: 'array' });
      expect(await operation.fetchAll()).to.deep.equal([
        [0.23, 'E'],
        [0.21, null],
      ]);
    });

    it('keeps object rows by default', async () => {
      const context = new ClientContextStub();
      const session = createDuplicateColumnSession(context);
      serveRowSet(context, duplicateRowSet());
      const operation = await session.executeStatement('SELECT carat AS a, color AS a');
      expect(await operation.fetchAll()).to.deep.equal([{ a: 'E' }, { a: null }]);
    });

    it('rejects an unsupported rowMode', async () => {
      const context = new ClientContextStub();
      const session = createDuplicateColumnSession(context);
      const spy = sinon.spy(context.driver, 'executeStatement');
      try {
        await session.executeStatement('SELECT 1', { rowMode: 'arrays' as never });
        expect.fail('It should throw an error');
      } catch (error) {
        expect(error).to.be.instanceOf(HiveDriverError);
        expect((error as Error).message).to.contain('rowMode');
      }
      expect(spy.called).to.be.false;
    });

    it('still parses staging responses in array mode', async () => {
      const context = new ClientContextStub();
      const session = createDuplicateColumnSession(context);
      context.driver.getResultSetMetadataResp = {
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
        resultFormat: TSparkRowSetType.COLUMN_BASED_SET,
        isStagingOperation: true,
        schema: {
          columns: [
            column('operation', TTypeId.STRING_TYPE, 1),
            column('presignedUrl', TTypeId.STRING_TYPE, 2),
            column('headers', TTypeId.STRING_TYPE, 3),
          ],
        },
      };
      serveRowSet(context, {
        startRowOffset: new Int64(0),
        rows: [],
        columns: [
          { stringVal: { values: ['REMOVE'], nulls: Buffer.from([]) } },
          { stringVal: { values: ['https://example.com/file'], nulls: Buffer.from([]) } },
          { stringVal: { values: ['{}'], nulls: Buffer.from([]) } },
        ],
      });
      const remove = sinon.stub(session as any, 'handleStagingRemove').resolves();
      await session.executeStatement('REMOVE "/Volumes/x"', { rowMode: 'array', stagingAllowedLocalPath: '/tmp' });
      expect(remove.calledOnceWith('https://example.com/file', '{}')).to.be.true;
    });
  });

  describe('KernelOperationBackend', () => {
    class StatementStub {
      public readonly statementId = 'kernel-row-mode';

      private readonly batches: Buffer[];

      constructor(private readonly schemaIpc: Buffer, batches: Buffer[]) {
        this.batches = [...batches];
      }

      public async fetchNextBatch() {
        const ipcBytes = this.batches.shift();
        return ipcBytes ? { ipcBytes } : null;
      }

      public schema() {
        return { ipcBytes: this.schemaIpc };
      }

      public async cancel() {}

      public async close() {}
    }

    function withTypeName(field: Field, typeName: string): Field {
      return new Field(field.name, field.type, field.nullable, new Map([['databricks.type_name', typeName]]));
    }

    const schema = new Schema([
      withTypeName(new Field('a', new Float64(), true), 'DOUBLE'),
      withTypeName(new Field('a', new Utf8(), true), 'STRING'),
    ]);

    it('returns array rows when rowMode is array', async () => {
      const backend = new KernelOperationBackend({
        statement: new StatementStub(arrowIpc(schema, [[], []]), [
          arrowIpc(schema, [
            [0.23, 0.21],
            ['E', null],
          ]),
        ]),
        context: new ClientContextStub(),
        rowMode: 'array',
      });
      expect(await backend.fetchChunk({ limit: 100 })).to.deep.equal([
        [0.23, 'E'],
        [0.21, null],
      ]);
    });

    it('keeps object rows by default', async () => {
      const backend = new KernelOperationBackend({
        statement: new StatementStub(arrowIpc(schema, [[], []]), [
          arrowIpc(schema, [
            [0.23, 0.21],
            ['E', null],
          ]),
        ]),
        context: new ClientContextStub(),
      });
      expect(await backend.fetchChunk({ limit: 100 })).to.deep.equal([{ a: 'E' }, { a: null }]);
    });
  });
});
