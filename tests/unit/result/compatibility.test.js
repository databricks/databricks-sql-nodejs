const { expect } = require('chai');
const ArrowResult = require('../../../dist/result/ArrowResult').default;
const JsonResult = require('../../../dist/result/JsonResult').default;

const fixtureColumn = require('../../fixtures/compatibility/column');
const fixtureArrow = require('../../fixtures/compatibility/arrow');
const fixtureArrowNT = require('../../fixtures/compatibility/arrow_native_types');

function fixArrowResult(rows) {
  return rows.map((row) => ({
    ...row,
    // This field is 32-bit floating point value, and since Arrow encodes it accurately,
    // it will have only approx. 7 significant decimal places. But in JS all floating
    // point numbers are double-precision, so after decoding we get, for example,
    // 1.414199948310852 instead of 1.4142. Therefore, we need to "fix" value
    // returned by Arrow, so it can be tested against our fixture
    flt: Number(row.flt.toFixed(4)),
  }));
}

describe('Result handlers compatibility tests', () => {
  it('colum-based data', () => {
    const result = new JsonResult(fixtureColumn.schema);
    const rows = result.getValue(fixtureColumn.rowSets);
    expect(rows).to.deep.equal(fixtureColumn.expected);
  });

  it('arrow-based data without native types', () => {
    const result = new ArrowResult(fixtureArrow.schema, fixtureArrow.arrowSchema);
    const rows = result.getValue(fixtureArrow.rowSets);
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrow.expected);
  });

  it('arrow-based data with native types', () => {
    const result = new ArrowResult(fixtureArrowNT.schema, fixtureArrowNT.arrowSchema);
    const rows = result.getValue(fixtureArrowNT.rowSets);
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrowNT.expected);
  });
});
