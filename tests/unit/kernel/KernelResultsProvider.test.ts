// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { expect } from 'chai';
import sinon from 'sinon';
import { tableFromArrays, tableToIPC } from 'apache-arrow';
import KernelResultsProvider, { KernelFetchHandle } from '../../../lib/kernel/KernelResultsProvider';

type FetchResult = Awaited<ReturnType<KernelFetchHandle['fetchNextBatch']>>;

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function makeBatch(rows: number[]) {
  return { ipcBytes: Buffer.from(tableToIPC(tableFromArrays({ value: Int32Array.from(rows) }), 'stream')) };
}

function nextTurn(): Promise<void> {
  return new Promise((resolve) => {
    setImmediate(resolve);
  });
}

describe('KernelResultsProvider', () => {
  const options = { limit: 1000 };

  it('starts the next fetch before the consumer receives the current batch', async () => {
    const first = deferred<FetchResult>();
    const second = deferred<FetchResult>();
    const fetchNextBatch = sinon.stub<[], Promise<FetchResult>>();
    fetchNextBatch.onFirstCall().returns(first.promise);
    fetchNextBatch.onSecondCall().returns(second.promise);
    const provider = new KernelResultsProvider({ fetchNextBatch }, true);

    const received = provider.fetchNext(options);
    expect(fetchNextBatch.callCount).to.equal(1);

    const batch = makeBatch([1, 2]);
    first.resolve(batch);
    expect(await received).to.deep.equal({ batches: [batch.ipcBytes], rowCount: 2 });
    expect(fetchNextBatch.callCount).to.equal(2);

    second.resolve(null);
    expect(await provider.hasMore()).to.be.false;
  });

  it('keeps at most one fetch in flight and does not fetch beyond the buffered batch', async () => {
    const requests = [deferred<FetchResult>(), deferred<FetchResult>(), deferred<FetchResult>()];
    let active = 0;
    let maxActive = 0;
    let index = 0;
    const fetchNextBatch = sinon.stub<[], Promise<FetchResult>>().callsFake(() => {
      const request = requests[index];
      index += 1;
      active += 1;
      maxActive = Math.max(maxActive, active);
      return request.promise.finally(() => {
        active -= 1;
      });
    });
    const provider = new KernelResultsProvider({ fetchNextBatch }, true);

    const hasMore = provider.hasMore();
    expect(active).to.equal(1);
    requests[0].resolve(makeBatch([1]));
    expect(await hasMore).to.be.true;
    expect(active).to.equal(1);
    expect(fetchNextBatch.callCount).to.equal(2);

    expect(await provider.hasMore()).to.be.true;
    expect(await provider.hasMore()).to.be.true;
    expect((await provider.fetchNext(options)).rowCount).to.equal(1);
    expect(fetchNextBatch.callCount).to.equal(2);

    requests[1].resolve(makeBatch([2, 3]));
    await nextTurn();
    expect(active).to.equal(0);
    expect(fetchNextBatch.callCount).to.equal(2);

    expect(await provider.hasMore()).to.be.true;
    expect(active).to.equal(1);
    expect((await provider.fetchNext(options)).rowCount).to.equal(2);

    requests[2].resolve(null);
    expect(await provider.hasMore()).to.be.false;
    expect(active).to.equal(0);
    expect(maxActive).to.equal(1);
    expect(fetchNextBatch.callCount).to.equal(3);
  });

  for (const prefetch of [false, true]) {
    const source = prefetch ? 'prefetch' : 'initial fetch';
    it(`propagates ${source} errors without fetching again`, async () => {
      const failed = deferred<FetchResult>();
      const fetchNextBatch = sinon.stub<[], Promise<FetchResult>>().returns(failed.promise);
      if (prefetch) {
        fetchNextBatch.onFirstCall().resolves(makeBatch([1]));
      }
      const provider = new KernelResultsProvider({ fetchNextBatch }, true);
      if (prefetch) {
        await provider.fetchNext(options);
      }

      const expected = new Error('fetch failed');
      const received = provider.fetchNext(options).then(
        () => expect.fail('Expected a fetch error'),
        (error: unknown) => error,
      );
      failed.reject(expected);
      expect(await received).to.equal(expected);
      await nextTurn();
      expect(fetchNextBatch.callCount).to.equal(prefetch ? 2 : 1);
    });

    it(`stops fetching after ${prefetch ? 'a prefetched' : 'an initial'} null`, async () => {
      const fetchNextBatch = sinon.stub<[], Promise<FetchResult>>().resolves(null);
      if (prefetch) {
        fetchNextBatch.onFirstCall().resolves(makeBatch([1]));
      }
      const provider = new KernelResultsProvider({ fetchNextBatch }, true);
      if (prefetch) {
        expect((await provider.fetchNext(options)).rowCount).to.equal(1);
      }

      expect(await provider.hasMore()).to.be.false;
      expect(await provider.fetchNext(options)).to.deep.equal({ batches: [], rowCount: 0 });
      expect(await provider.hasMore()).to.be.false;
      expect(await provider.fetchNext(options)).to.deep.equal({ batches: [], rowCount: 0 });
      expect(fetchNextBatch.callCount).to.equal(prefetch ? 2 : 1);
    });
  }

  it('handles a rejected prefetch after the consumer stops', async () => {
    const abandoned = deferred<FetchResult>();
    const fetchNextBatch = sinon.stub<[], Promise<FetchResult>>();
    fetchNextBatch.onFirstCall().resolves(makeBatch([1]));
    fetchNextBatch.onSecondCall().returns(abandoned.promise);
    const provider = new KernelResultsProvider({ fetchNextBatch }, true);
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => {
      unhandled.push(reason);
    };
    process.on('unhandledRejection', onUnhandled);
    try {
      expect((await provider.fetchNext(options)).rowCount).to.equal(1);
      abandoned.reject(new Error('abandoned prefetch failed'));
      await nextTurn();
      await nextTurn();
      expect(unhandled).to.be.empty;
      expect(fetchNextBatch.callCount).to.equal(2);
    } finally {
      process.removeListener('unhandledRejection', onUnhandled);
    }
  });

  it('skips empty batches before data and before exhaustion', async () => {
    const batch = makeBatch([1, 2, 3]);
    const batches = [makeBatch([]), makeBatch([]), batch, makeBatch([]), null];
    const fetchNextBatch = sinon.stub<[], Promise<FetchResult>>();
    batches.forEach((value, index) => fetchNextBatch.onCall(index).resolves(value));
    const provider = new KernelResultsProvider({ fetchNextBatch }, true);

    expect(await provider.hasMore()).to.be.true;
    expect(await provider.fetchNext(options)).to.deep.equal({ batches: [batch.ipcBytes], rowCount: 3 });
    expect(await provider.hasMore()).to.be.false;
    expect(await provider.fetchNext(options)).to.deep.equal({ batches: [], rowCount: 0 });
    expect(fetchNextBatch.callCount).to.equal(batches.length);
  });

  it('fetches on demand by default', async () => {
    const fetchNextBatch = sinon.stub<[], Promise<FetchResult>>();
    fetchNextBatch.onFirstCall().resolves(makeBatch([1]));
    fetchNextBatch.onSecondCall().resolves(makeBatch([2]));
    fetchNextBatch.onThirdCall().resolves(null);
    const provider = new KernelResultsProvider({ fetchNextBatch });

    expect(await provider.hasMore()).to.be.true;
    expect(fetchNextBatch.callCount).to.equal(1);
    expect((await provider.fetchNext(options)).rowCount).to.equal(1);
    await nextTurn();
    expect(fetchNextBatch.callCount).to.equal(1);

    expect((await provider.fetchNext(options)).rowCount).to.equal(1);
    expect(fetchNextBatch.callCount).to.equal(2);
    expect(await provider.hasMore()).to.be.false;
    expect(fetchNextBatch.callCount).to.equal(3);
  });
});
