const { expect } = require('chai');
const ArrowResultHandler = require('../../../lib/result/ArrowResultHandler').default;
const ArrowResultConverter = require('../../../lib/result/ArrowResultConverter').default;
const JsonResultHandler = require('../../../lib/result/JsonResultHandler').default;

const { fixArrowResult } = require('../../fixtures/compatibility');
const fixtureColumn = require('../../fixtures/compatibility/column');
const fixtureArrow = require('../../fixtures/compatibility/arrow');
const fixtureArrowNT = require('../../fixtures/compatibility/arrow_native_types');

const ResultsProviderMock = require('./fixtures/ResultsProviderMock');

describe('Result handlers compatibility tests', () => {
  it('colum-based data', async () => {
    const context = {};
    const rowSetProvider = new ResultsProviderMock(fixtureColumn.rowSets);
    const result = new JsonResultHandler(context, rowSetProvider, { schema: fixtureColumn.schema });
    const rows = await result.fetchNext({ limit: 10000 });
    expect(rows).to.deep.equal(fixtureColumn.expected);
  });

  it('arrow-based data without native types', async () => {
    const context = {};
    const rowSetProvider = new ResultsProviderMock(fixtureArrow.rowSets);
    const result = new ArrowResultConverter(
      context,
      new ArrowResultHandler(context, rowSetProvider, { arrowSchema: fixtureArrow.arrowSchema }),
      { schema: fixtureArrow.schema },
    );
    const rows = await result.fetchNext({ limit: 10000 });
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrow.expected);
  });

  it('arrow-based data with native types', async () => {
    const context = {};
    const rowSetProvider = new ResultsProviderMock(fixtureArrowNT.rowSets);
    const result = new ArrowResultConverter(
      context,
      new ArrowResultHandler(context, rowSetProvider, { arrowSchema: fixtureArrowNT.arrowSchema }),
      { schema: fixtureArrowNT.schema },
    );
    const rows = await result.fetchNext({ limit: 10000 });
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrowNT.expected);
  });

  it('should infer arrow schema from thrift schema', async () => {
    const context = {};
    const rowSetProvider = new ResultsProviderMock(fixtureArrow.rowSets);
    const result = new ArrowResultConverter(
      context,
      new ArrowResultHandler(context, rowSetProvider, { schema: fixtureArrow.schema }),
      { schema: fixtureArrow.schema },
    );
    const rows = await result.fetchNext({ limit: 10000 });
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrow.expected);
  });
});
