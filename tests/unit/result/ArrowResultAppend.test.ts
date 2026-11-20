import { expect } from 'chai';
import sinon from 'sinon';
import * as arrow from 'apache-arrow';
import ArrowResultConverter from '../../../lib/result/ArrowResultConverter';
import { ArrowBatch } from '../../../lib/result/utils';
import ResultSlicer from '../../../lib/result/ResultSlicer';
import { RowTarget } from '../../../lib/result/IResultsProvider';
import { TTypeId } from '../../../thrift/TCLIService_types';
import ClientContextStub from '../.stubs/ClientContextStub';
import ResultsProviderStub from '../.stubs/ResultsProviderStub';

function fixture(rowMode: 'object' | 'array', disableRowMaterialization = false) {
  const values = [true, null, false, true, null, false];
  const bytes = Buffer.from(
    arrow.tableToIPC(new arrow.Table({ value: arrow.vectorFromArray(values, new arrow.Bool()) })),
  );
  const converter = new ArrowResultConverter(
    new ClientContextStub(),
    new ResultsProviderStub<ArrowBatch>(
      [
        { batches: [bytes], rowCount: 6 },
        { batches: [bytes], rowCount: 5 },
        { batches: [], rowCount: 0 },
        { batches: [bytes], rowCount: 2 },
      ],
      { batches: [], rowCount: 0 },
    ),
    {
      schema: {
        columns: [
          {
            columnName: 'value',
            position: 1,
            typeDesc: { types: [{ primitiveEntry: { type: TTypeId.BOOLEAN_TYPE } }] },
          },
        ],
      },
    },
    { rowMode, disableRowMaterialization },
  );
  const rows = [...values, ...values.slice(0, 5), ...values.slice(0, 2)].map((value) =>
    disableRowMaterialization ? null : rowMode === 'array' ? [value] : { value },
  );
  return { converter, rows, slicer: new ResultSlicer(new ClientContextStub(), converter) };
}

describe('Arrow result append', () => {
  for (const rowMode of ['object', 'array'] as const) {
    for (const placeholders of [false, true]) {
      for (const limit of [1, 4, 6, 10, 100, Infinity]) {
        it(`preserves boundaries, nulls and owned tails (${rowMode}, placeholders=${placeholders}, limit=${limit})`, async () => {
          const { converter, rows, slicer } = fixture(rowMode, placeholders);
          const fetch = sinon.spy(converter, 'fetchNext');
          const chunks = [];
          let count = 0;
          while (await slicer.hasMore()) {
            const chunk = await slicer.fetchNext({ limit });
            expect(chunk).to.deep.equal(rows.slice(count, count + limit));
            count += chunk.length;
            chunks.push(...chunk);
            const pending = slicer['remainingResults'].slice(slicer['remainingOffset']);
            expect(pending).to.deep.equal(rows.slice(count, count + pending.length));
            expect(slicer['remainingResults'].slice(0, slicer['remainingOffset'])).to.satisfy((cleared: unknown[]) =>
              cleared.every((value) => value === undefined),
            );
          }
          expect(chunks).to.deep.equal(rows);
          sinon.assert.notCalled(fetch);
        });
      }
    }

    it(`writes fitted materialized rows directly to the output once (${rowMode})`, async () => {
      const { converter, rows } = fixture(rowMode);
      const writes: number[] = [];
      const target: RowTarget<any> = {
        count: 0,
        rows: new Proxy([], {
          set(array, property, value) {
            if (typeof property === 'string' && /^\d+$/.test(property)) writes.push(Number(property));
            return Reflect.set(array, property, value);
          },
        }),
      };
      const materialize = sinon.spy(converter as any, 'getRows');
      while (await converter.hasMore()) {
        expect(await converter.fetchNextInto({ limit: 100 }, target, 100)).to.deep.equal([]);
      }
      target.rows.length = target.count;
      expect(target.rows).to.deep.equal(rows);
      expect(writes).to.deep.equal(rows.map((_, index) => index));
      materialize.getCalls().forEach((call) => {
        expect(call.args[2]).to.equal(target.rows);
        expect(call.returnValue).to.equal(target.rows);
      });
    });

    for (const placeholders of [false, true]) {
      it(`grows sequentially beyond the initial allocation guard (${rowMode}, placeholders=${placeholders})`, async () => {
        const { converter, rows } = fixture(rowMode, placeholders);
        const start = 2 ** 25 - 2;
        let length = start;
        const values = new Map<number, unknown>();
        const reserved: number[] = [];
        const target: RowTarget<any> = {
          count: start,
          rows: new Proxy([], {
            get(array, property) {
              return property === 'length' ? length : Reflect.get(array, property);
            },
            set(_array, property, value) {
              if (property === 'length') {
                reserved.push(value);
                length = value;
              } else {
                const index = Number(property);
                values.set(index, value);
                length = Math.max(length, index + 1);
              }
              return true;
            },
          }),
        };
        while (await converter.hasMore()) {
          expect(await converter.fetchNextInto({ limit: Infinity }, target, Infinity)).to.deep.equal([]);
        }
        expect(reserved).to.deep.equal([2 ** 25]);
        expect(target.count).to.equal(start + rows.length);
        expect([...values.keys()]).to.deep.equal(rows.map((_, index) => start + index));
        expect([...values.values()]).to.deep.equal(rows);
      });
    }
  }

  it('does not read back fitted rows to copy them in the slicer', async () => {
    const { converter, slicer, rows } = fixture('object');
    const append = converter.fetchNextInto.bind(converter);
    let reading = false;
    sinon.stub(converter, 'fetchNextInto').callsFake(async (options, target, limit) => {
      target.rows = new Proxy(target.rows, {
        get(array, property) {
          if (!reading && typeof property === 'string' && /^\d+$/.test(property)) throw new Error('copied output row');
          return Reflect.get(array, property);
        },
      });
      return append(options, target, limit);
    });
    const chunk = await slicer.fetchNext({ limit: 100 });
    reading = true;
    expect(chunk).to.deep.equal(rows);
  });

  it('returns the owned tail before an unbuffered batch and never mutates returned chunks', async () => {
    const { slicer, rows } = fixture('object');
    const first = await slicer.fetchNext({ limit: 4 });
    const second = await slicer.fetchNext({ limit: 100, disableBuffering: true });
    const third = await slicer.fetchNext({ limit: 100, disableBuffering: true });
    const fourth = await slicer.fetchNext({ limit: 100 });
    expect([first.length, second.length, third.length, fourth.length]).to.deep.equal([4, 2, 5, 2]);
    expect([...first, ...second, ...third, ...fourth]).to.deep.equal(rows);
    expect(await slicer.hasMore()).to.equal(false);
  });

  it('retains fractional and invalid limit behavior when the source supports append', async () => {
    const { slicer, rows } = fixture('object');
    const first = await slicer.fetchNext({ limit: 3.5 });
    expect(first).to.deep.equal(rows.slice(0, 3));
    expect(await slicer.fetchNext({ limit: 0 })).to.deep.equal([]);
    expect(await slicer.fetchNext({ limit: -1 })).to.deep.equal([]);
    expect(await slicer.fetchNext({ limit: NaN })).to.deep.equal(rows.slice(3, 6));
    expect(await slicer.fetchNext({ limit: 100 })).to.deep.equal(rows.slice(6));
  });

  it('propagates prefetch errors rather than returning a partial chunk', async () => {
    const { converter, slicer } = fixture('object');
    const error = new Error('prefetch failed');
    sinon.stub(converter['source'], 'fetchNext').rejects(error);
    try {
      await slicer.fetchNext({ limit: 100 });
      expect.fail('fetch should reject');
    } catch (actual) {
      expect(actual).to.equal(error);
    }
  });

  for (const limit of [2 ** 25, 2 ** 25 + 1, Infinity]) {
    it(`does not reserve a huge requested limit for a small result (${limit})`, async () => {
      const { converter, rows } = fixture('object');
      const target: RowTarget<any> = { rows: [], count: 0 };
      while (await converter.hasMore()) {
        expect(await converter.fetchNextInto({ limit }, target, limit)).to.deep.equal([]);
        expect(target.rows.length).to.be.at.most(6 * 32);
      }
      target.rows.length = target.count;
      expect(target.rows).to.deep.equal(rows);
    });
  }
});
