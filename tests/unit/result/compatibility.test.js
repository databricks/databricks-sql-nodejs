const { expect } = require('chai');
const ArrowResult = require('../../../dist/result/ArrowResult').default;
const JsonResult = require('../../../dist/result/JsonResult').default;

const { fixArrowResult } = require('../../fixtures/compatibility');
const fixtureColumn = require('../../fixtures/compatibility/column');
const fixtureArrow = require('../../fixtures/compatibility/arrow');
const fixtureArrowNT = require('../../fixtures/compatibility/arrow_native_types');

describe('Result handlers compatibility tests', () => {
  it('colum-based data', async () => {
    const context = {};
    const result = new JsonResult(context, fixtureColumn.schema);
    const rows = await result.getValue(fixtureColumn.rowSets);
    expect(rows).to.deep.equal(fixtureColumn.expected);
  });

  it('arrow-based data without native types', async () => {
    const context = {};
    const result = new ArrowResult(context, fixtureArrow.schema, fixtureArrow.arrowSchema);
    const rows = await result.getValue(fixtureArrow.rowSets);
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrow.expected);
  });

  it('arrow-based data with native types', async () => {
    const context = {};
    const result = new ArrowResult(context, fixtureArrowNT.schema, fixtureArrowNT.arrowSchema);
    const rows = await result.getValue(fixtureArrowNT.rowSets);
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrowNT.expected);
  });
});
