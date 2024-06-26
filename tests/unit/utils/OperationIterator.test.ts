import { expect } from 'chai';
import { OperationChunksIterator, OperationRowsIterator } from '../../../lib/utils/OperationIterator';

import OperationStub from '../.stubs/OperationStub';

describe('OperationChunksIterator', () => {
  it('should iterate over all chunks', async () => {
    const chunks = [[1, 2, 3], [4, 5, 6, 7, 8], [9]];

    const operation = new OperationStub(chunks);

    expect(operation.closed).to.be.false;

    let index = 0;
    for await (const chunk of operation.iterateChunks()) {
      expect(chunk).to.deep.equal(chunks[index]);
      index += 1;
    }

    expect(index).to.equal(chunks.length);
    expect(operation.closed).to.be.false;
  });

  it('should iterate over all chunks and close operation', async () => {
    const chunks = [[1, 2, 3], [4, 5, 6, 7, 8], [9]];

    const operation = new OperationStub(chunks);

    expect(operation.closed).to.be.false;

    let index = 0;
    for await (const chunk of operation.iterateChunks({ autoClose: true })) {
      expect(chunk).to.deep.equal(chunks[index]);
      index += 1;
    }

    expect(index).to.equal(chunks.length);
    expect(operation.closed).to.be.true;
  });

  it('should iterate partially', async () => {
    const chunks = [[1, 2, 3], [4, 5, 6, 7, 8], [9]];

    const operation = new OperationStub(chunks);

    expect(operation.closed).to.be.false;

    for await (const chunk of operation.iterateChunks()) {
      expect(chunk).to.deep.equal(chunks[0]);
      break;
    }

    for await (const chunk of operation.iterateChunks()) {
      expect(chunk).to.deep.equal(chunks[1]);
      break;
    }

    expect(await operation.hasMoreRows()).to.be.true;
    expect(operation.closed).to.be.false;
  });

  it('should iterate partially and close operation', async () => {
    const chunks = [[1, 2, 3], [4, 5, 6, 7, 8], [9]];

    const operation = new OperationStub(chunks);

    expect(operation.closed).to.be.false;

    for await (const chunk of operation.iterateChunks({ autoClose: true })) {
      expect(chunk).to.deep.equal(chunks[0]);
      break;
    }

    expect(await operation.hasMoreRows()).to.be.false;
    expect(operation.closed).to.be.true;
  });
});

describe('OperationRowsIterator', () => {
  it('should iterate over all rows', async () => {
    const chunks = [[1, 2, 3], [4, 5, 6, 7, 8], [9]];
    const rows = chunks.flat();

    const operation = new OperationStub(chunks);

    expect(operation.closed).to.be.false;

    let index = 0;
    for await (const row of operation.iterateRows()) {
      expect(row).to.equal(rows[index]);
      index += 1;
    }

    expect(index).to.equal(rows.length);
    expect(operation.closed).to.be.false;
  });

  it('should iterate over all rows and close operation', async () => {
    const chunks = [[1, 2, 3], [4, 5, 6, 7, 8], [9]];
    const rows = chunks.flat();

    const operation = new OperationStub(chunks);

    expect(operation.closed).to.be.false;

    let index = 0;
    for await (const row of operation.iterateRows({ autoClose: true })) {
      expect(row).to.equal(rows[index]);
      index += 1;
    }

    expect(index).to.equal(rows.length);
    expect(operation.closed).to.be.true;
  });

  it('should iterate partially', async () => {
    const chunks = [[1, 2, 3], [4, 5, 6, 7, 8], [9]];

    const operation = new OperationStub(chunks);

    expect(operation.closed).to.be.false;

    for await (const row of operation.iterateRows()) {
      expect(row).to.equal(chunks[0][0]);
      break;
    }

    for await (const row of operation.iterateRows()) {
      // This is a limitation of rows iterator. Since operation can only
      // supply chunks of rows, when new iterator is created it will
      // start with the next available chunk. Generally this should not
      // be an issue, because using multiple iterators on the same operation
      // is not recommended
      expect(row).to.equal(chunks[1][0]);
      break;
    }

    expect(await operation.hasMoreRows()).to.be.true;
    expect(operation.closed).to.be.false;
  });

  it('should iterate partially and close operation', async () => {
    const chunks = [[1, 2, 3], [4, 5, 6, 7, 8], [9]];
    const rows = chunks.flat();

    const operation = new OperationStub(chunks);

    expect(operation.closed).to.be.false;

    for await (const row of operation.iterateRows({ autoClose: true })) {
      expect(row).to.equal(rows[0]);
      break;
    }

    expect(await operation.hasMoreRows()).to.be.false;
    expect(operation.closed).to.be.true;
  });
});
