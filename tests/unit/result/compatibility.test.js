const { expect } = require('chai');
const ArrowResultHandler = require('../../../dist/result/ArrowResultHandler').default;
const ArrowResultConverter = require('../../../dist/result/ArrowResultConverter').default;
const JsonResultHandler = require('../../../dist/result/JsonResultHandler').default;

const { fixArrowResult } = require('../../fixtures/compatibility');
const fixtureColumn = require('../../fixtures/compatibility/column');
const fixtureArrow = require('../../fixtures/compatibility/arrow');
const fixtureArrowNT = require('../../fixtures/compatibility/arrow_native_types');

const ResultsProviderMock = require('./fixtures/ResultsProviderMock');

describe('Result handlers compatibility tests', () => {
  it('colum-based data', async () => {
    const context = {};
    const rowSetProvider = new ResultsProviderMock(fixtureColumn.rowSets);
    const result = new JsonResultHandler(context, rowSetProvider, fixtureColumn.schema);
    const rows = await result.fetchNext({ limit: 10000 });
    expect(rows).to.deep.equal(fixtureColumn.expected);
  });

  it('arrow-based data without native types', async () => {
    const context = {};
    const rowSetProvider = new ResultsProviderMock(fixtureArrow.rowSets);
    const result = new ArrowResultConverter(
      context,
      new ArrowResultHandler(context, rowSetProvider, fixtureArrow.arrowSchema),
      fixtureArrow.schema,
    );
    const rows = await result.fetchNext({ limit: 10000 });
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrow.expected);
  });

  it('arrow-based data with native types', async () => {
    const context = {};
    const rowSetProvider = new ResultsProviderMock(fixtureArrowNT.rowSets);
    const result = new ArrowResultConverter(
      context,
      new ArrowResultHandler(context, rowSetProvider, fixtureArrowNT.arrowSchema),
      fixtureArrowNT.schema,
    );
    const rows = await result.fetchNext({ limit: 10000 });
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrowNT.expected);
  });
});
