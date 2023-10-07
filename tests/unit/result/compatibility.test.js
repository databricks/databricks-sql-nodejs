const { expect } = require('chai');
const ArrowResultHandler = require('../../../dist/result/ArrowResultHandler').default;
const JsonResultHandler = require('../../../dist/result/JsonResultHandler').default;

const { fixArrowResult } = require('../../fixtures/compatibility');
const fixtureColumn = require('../../fixtures/compatibility/column');
const fixtureArrow = require('../../fixtures/compatibility/arrow');
const fixtureArrowNT = require('../../fixtures/compatibility/arrow_native_types');

const RowSetProviderMock = require('./fixtures/RowSetProviderMock');

describe('Result handlers compatibility tests', () => {
  it('colum-based data', async () => {
    const context = {};
    const rowSetProvider = new RowSetProviderMock();
    const result = new JsonResultHandler(context, rowSetProvider, fixtureColumn.schema);
    const rows = await result.getValue(fixtureColumn.rowSets);
    expect(rows).to.deep.equal(fixtureColumn.expected);
  });

  it('arrow-based data without native types', async () => {
    const context = {};
    const rowSetProvider = new RowSetProviderMock();
    const result = new ArrowResultHandler(context, rowSetProvider, fixtureArrow.schema, fixtureArrow.arrowSchema);
    const rows = await result.getValue(fixtureArrow.rowSets);
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrow.expected);
  });

  it('arrow-based data with native types', async () => {
    const context = {};
    const rowSetProvider = new RowSetProviderMock();
    const result = new ArrowResultHandler(context, rowSetProvider, fixtureArrowNT.schema, fixtureArrowNT.arrowSchema);
    const rows = await result.getValue(fixtureArrowNT.rowSets);
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrowNT.expected);
  });
});
