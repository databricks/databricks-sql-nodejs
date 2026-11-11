import { expect } from 'chai';
import sinon, { SinonSpy } from 'sinon';
import ResultSlicer, { concatChunks } from '../../../lib/result/ResultSlicer';
import ResultsProviderStub from '../.stubs/ResultsProviderStub';

import ClientContextStub from '../.stubs/ClientContextStub';

describe('ResultSlicer', () => {
  it('should carry leftovers across several calls without mutating the source batch', async () => {
    const rows = Array.from({ length: 250 }, (_, index) => index);
    Object.freeze(rows);
    const provider = new ResultsProviderStub([rows], []);
    const fetchNext = sinon.spy(provider, 'fetchNext');
    const slicer = new ResultSlicer(new ClientContextStub(), provider);

    expect(await slicer.fetchNext({ limit: 100 })).to.deep.eq(rows.slice(0, 100));
    expect(await provider.hasMore()).to.be.false;
    expect(await slicer.hasMore()).to.be.true;

    expect(await slicer.fetchNext({ limit: 100 })).to.deep.eq(rows.slice(100, 200));
    expect(await slicer.hasMore()).to.be.true;

    expect(await slicer.fetchNext({ limit: 100 })).to.deep.eq(rows.slice(200));
    expect(await slicer.hasMore()).to.be.false;
    expect(fetchNext.callCount).to.eq(1);
  });

  it('should join many small batches into one chunk', async () => {
    const batches = Array.from({ length: 100 }, (_, index) => [index * 2, index * 2 + 1]);
    const provider = new ResultsProviderStub(batches, []);
    const slicer = new ResultSlicer(new ClientContextStub(), provider);

    expect(await slicer.fetchNext({ limit: 200 })).to.deep.eq(Array.from({ length: 200 }, (_, index) => index));
    expect(await slicer.hasMore()).to.be.false;
  });

  it('should return an exact-fit single batch as-is', async () => {
    const rows = [1, 2, 3];
    const provider = new ResultsProviderStub([rows], []);
    const slicer = new ResultSlicer(new ClientContextStub(), provider);

    expect(await slicer.fetchNext({ limit: 3 })).to.eq(rows);
    expect(await slicer.hasMore()).to.be.false;
    expect(await slicer.fetchNext({ limit: 3 })).to.deep.eq([]);
  });

  it('should return the partial leftover before proxying unbuffered fetches', async () => {
    const rows = Array.from({ length: 250 }, (_, index) => index);
    const nextRows = [250, 251, 252];
    const provider = new ResultsProviderStub([rows, nextRows], []);
    const fetchNext = sinon.spy(provider, 'fetchNext');
    const slicer = new ResultSlicer(new ClientContextStub(), provider);

    await slicer.fetchNext({ limit: 100 });
    await slicer.fetchNext({ limit: 100 });

    expect(await slicer.fetchNext({ limit: 10, disableBuffering: true })).to.deep.eq(rows.slice(200));
    expect(fetchNext.callCount).to.eq(1);
    expect(await slicer.hasMore()).to.be.true;

    const options = { limit: 10, disableBuffering: true };
    expect(await slicer.fetchNext(options)).to.eq(nextRows);
    expect(fetchNext.callCount).to.eq(2);
    expect(fetchNext.lastCall.args).to.deep.eq([options]);
    expect(await slicer.hasMore()).to.be.false;

    expect(await slicer.fetchNext({ limit: 100 })).to.deep.eq([]);
  });

  it('should skip empty batches while buffering', async () => {
    const provider = new ResultsProviderStub([[], [1], [], [2, 3], []], []);
    const slicer = new ResultSlicer(new ClientContextStub(), provider);

    expect(await slicer.fetchNext({ limit: 2 })).to.deep.eq([1, 2]);
    expect(await slicer.hasMore()).to.be.true;
    expect(await slicer.fetchNext({ limit: 2 })).to.deep.eq([3]);
    expect(await slicer.hasMore()).to.be.false;
  });

  it('should return an empty chunk when all batches are empty', async () => {
    const provider = new ResultsProviderStub([[], []], []);
    const slicer = new ResultSlicer(new ClientContextStub(), provider);

    expect(await slicer.fetchNext({ limit: 100 })).to.deep.eq([]);
    expect(await slicer.hasMore()).to.be.false;
  });

  it('should keep leftovers when the requested limit is zero', async () => {
    const provider = new ResultsProviderStub([[1, 2, 3]], []);
    const slicer = new ResultSlicer(new ClientContextStub(), provider);

    expect(await slicer.fetchNext({ limit: 0 })).to.deep.eq([]);
    expect(await slicer.fetchNext({ limit: 1 })).to.deep.eq([1]);
    expect(await slicer.fetchNext({ limit: 0 })).to.deep.eq([]);
    expect(await slicer.hasMore()).to.be.true;
    expect(await slicer.fetchNext({ limit: 2 })).to.deep.eq([2, 3]);
    expect(await slicer.hasMore()).to.be.false;
  });

  for (const limit of [-1, -1.5, -Infinity]) {
    it(`should keep leftovers when the requested limit is ${limit}`, async () => {
      const provider = new ResultsProviderStub([[1, 2, 3, 4, 5, 6]], []);
      const slicer = new ResultSlicer(new ClientContextStub(), provider);

      expect(await slicer.fetchNext({ limit: 3 })).to.deep.eq([1, 2, 3]);
      expect(await slicer.fetchNext({ limit })).to.deep.eq([]);
      expect(await slicer.hasMore()).to.be.true;
      expect(await slicer.fetchNext({ limit: 3 })).to.deep.eq([4, 5, 6]);
      expect(await slicer.hasMore()).to.be.false;
    });
  }

  it('should return all leftovers for a NaN limit without fetching from the source', async () => {
    const nextRows = [7, 8, 9];
    const provider = new ResultsProviderStub([[1, 2, 3, 4, 5, 6], nextRows], []);
    const fetchNext = sinon.spy(provider, 'fetchNext');
    const slicer = new ResultSlicer(new ClientContextStub(), provider);

    expect(await slicer.fetchNext({ limit: 3 })).to.deep.eq([1, 2, 3]);
    expect(await slicer.fetchNext({ limit: NaN })).to.deep.eq([4, 5, 6]);
    expect(fetchNext.callCount).to.eq(1);
    expect(await slicer.hasMore()).to.be.true;
    expect(await slicer.fetchNext({ limit: 3 })).to.eq(nextRows);
    expect(await slicer.hasMore()).to.be.false;
  });

  it('should not fetch from the source for a NaN limit without leftovers', async () => {
    const provider = new ResultsProviderStub([[1, 2, 3]], []);
    const fetchNext = sinon.spy(provider, 'fetchNext');
    const slicer = new ResultSlicer(new ClientContextStub(), provider);

    expect(await slicer.fetchNext({ limit: NaN })).to.deep.eq([]);
    expect(fetchNext.called).to.be.false;
    expect(await slicer.hasMore()).to.be.true;
    expect(await slicer.fetchNext({ limit: 3 })).to.deep.eq([1, 2, 3]);
  });

  for (const limit of [3, 3.5]) {
    it(`should truncate fractional counts after fetching with limit ${limit}`, async () => {
      const provider = new ResultsProviderStub([[1, 2, 3, 4, 5, 6]], []);
      const slicer = new ResultSlicer(new ClientContextStub(), provider);

      expect(await slicer.fetchNext({ limit })).to.deep.eq([1, 2, 3]);
      expect(await slicer.fetchNext({ limit: 1.5 })).to.deep.eq([4]);
      expect(await slicer.fetchNext({ limit: 1.5 })).to.deep.eq([5]);
      expect(await slicer.fetchNext({ limit: 3 })).to.deep.eq([6]);
      expect(await slicer.hasMore()).to.be.false;
    });
  }

  it('should exhaust leftovers before fetching more rows with a fractional limit', async () => {
    const provider = new ResultsProviderStub(
      [
        [1, 2, 3, 4, 5, 6],
        [7, 8, 9],
      ],
      [],
    );
    const fetchNext = sinon.spy(provider, 'fetchNext');
    const slicer = new ResultSlicer(new ClientContextStub(), provider);

    expect(await slicer.fetchNext({ limit: 3 })).to.deep.eq([1, 2, 3]);
    expect(await slicer.fetchNext({ limit: 1.5 })).to.deep.eq([4]);
    expect(fetchNext.callCount).to.eq(1);
    expect(await slicer.fetchNext({ limit: 10 })).to.deep.eq([5, 6, 7, 8, 9]);
    expect(await slicer.hasMore()).to.be.false;
  });

  it('should return chunks of requested size', async () => {
    const provider = new ResultsProviderStub(
      [
        [10, 11, 12, 13, 14, 15],
        [20, 21, 22, 23, 24, 25],
        [30, 31, 32, 33, 34, 35],
      ],
      [],
    );

    const slicer = new ResultSlicer(new ClientContextStub(), provider);

    const chunk1 = await slicer.fetchNext({ limit: 4 });
    expect(chunk1).to.deep.eq([10, 11, 12, 13]);
    expect(await slicer.hasMore()).to.be.true;

    const chunk2 = await slicer.fetchNext({ limit: 10 });
    expect(chunk2).to.deep.eq([14, 15, 20, 21, 22, 23, 24, 25, 30, 31]);
    expect(await slicer.hasMore()).to.be.true;

    const chunk3 = await slicer.fetchNext({ limit: 10 });
    expect(chunk3).to.deep.eq([32, 33, 34, 35]);
    expect(await slicer.hasMore()).to.be.false;
  });

  it('should return raw chunks', async () => {
    const provider = new ResultsProviderStub(
      [
        [10, 11, 12, 13, 14, 15],
        [20, 21, 22, 23, 24, 25],
        [30, 31, 32, 33, 34, 35],
      ],
      [],
    );
    sinon.spy(provider, 'fetchNext');

    const slicer = new ResultSlicer(new ClientContextStub(), provider);

    const chunk1 = await slicer.fetchNext({ limit: 4, disableBuffering: true });
    expect(chunk1).to.deep.eq([10, 11, 12, 13, 14, 15]);
    expect(await slicer.hasMore()).to.be.true;
    expect((provider.fetchNext as SinonSpy).callCount).to.be.equal(1);

    const chunk2 = await slicer.fetchNext({ limit: 10, disableBuffering: true });
    expect(chunk2).to.deep.eq([20, 21, 22, 23, 24, 25]);
    expect(await slicer.hasMore()).to.be.true;
    expect((provider.fetchNext as SinonSpy).callCount).to.be.equal(2);
  });

  it('should switch between returning sliced and raw chunks', async () => {
    const provider = new ResultsProviderStub(
      [
        [10, 11, 12, 13, 14, 15],
        [20, 21, 22, 23, 24, 25],
        [30, 31, 32, 33, 34, 35],
      ],
      [],
    );

    const slicer = new ResultSlicer(new ClientContextStub(), provider);

    const chunk1 = await slicer.fetchNext({ limit: 4 });
    expect(chunk1).to.deep.eq([10, 11, 12, 13]);
    expect(await slicer.hasMore()).to.be.true;

    const chunk2 = await slicer.fetchNext({ limit: 10, disableBuffering: true });
    expect(chunk2).to.deep.eq([14, 15]);
    expect(await slicer.hasMore()).to.be.true;

    const chunk3 = await slicer.fetchNext({ limit: 10, disableBuffering: true });
    expect(chunk3).to.deep.eq([20, 21, 22, 23, 24, 25]);
    expect(await slicer.hasMore()).to.be.true;

    const chunk4 = await slicer.fetchNext({ limit: 4 });
    expect(chunk4).to.deep.eq([30, 31, 32, 33]);
    expect(await slicer.hasMore()).to.be.true;

    const chunk5 = await slicer.fetchNext({ limit: 4 });
    expect(chunk5).to.deep.eq([34, 35]);
    expect(await slicer.hasMore()).to.be.false;
  });
});

describe('concatChunks', () => {
  it('should return an empty array when there are no chunks', () => {
    expect(concatChunks([])).to.deep.eq([]);
  });

  it('should preserve row values and references across chunks', () => {
    const row = { value: 1 };
    const arrayRow = [2, 3];
    const result = concatChunks<unknown>([[], [row, null], [], [arrayRow, undefined], []]);

    expect(result).to.deep.eq([row, null, arrayRow, undefined]);
    expect(result[0]).to.eq(row);
    expect(result[2]).to.eq(arrayRow);
  });
});
