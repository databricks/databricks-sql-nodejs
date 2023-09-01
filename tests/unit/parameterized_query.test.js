const { expect } = require('chai');

const DBSQLClient = require('../../dist/DBSQLClient').default;
const convertToSparkParameters = require('../../dist/utils/convertToSparkParameters').default;
const { TSparkParameterValue, TSparkParameter } = require('../../thrift/TCLIService_types');
const { default: DBSQLParameter } = require('../../dist/DBSQLParameter');

describe('Test Inference', () => {
  it('should infer types correctly', () => {
    let params = convertToSparkParameters([null, 'value', 1, 1.1, true]);
    expect(params[0]).to.deep.eq(new TSparkParameter({ type: 'VOID', value: new TSparkParameterValue() }));
    expect(params[1]).to.deep.eq(
      new TSparkParameter({ type: 'STRING', value: new TSparkParameterValue({ stringValue: 'value' }) }),
    );
    expect(params[2]).to.deep.eq(
      new TSparkParameter({ type: 'INTEGER', value: new TSparkParameterValue({ stringValue: '1' }) }),
    );
    expect(params[3]).to.deep.eq(
      new TSparkParameter({ type: 'DOUBLE', value: new TSparkParameterValue({ stringValue: '1.1' }) }),
    );
    expect(params[4]).to.deep.eq(
      new TSparkParameter({ type: 'BOOLEAN', value: new TSparkParameterValue({ stringValue: 'true' }) }),
    );
  });
  it('should preserve name info', () => {
    let params = convertToSparkParameters([
      new DBSQLParameter({ name: '1', value: 26 }),
      new DBSQLParameter({ name: '2', value: 6.2, type: 'DECIMAL' }),
    ]);
    expect(params[0]).to.deep.eq(
      new TSparkParameter({ name: '1', type: 'INTEGER', value: new TSparkParameterValue({ stringValue: '26' }) }),
    );
    expect(params[1]).to.deep.eq(
      new TSparkParameter({ name: '2', type: 'DECIMAL', value: new TSparkParameterValue({ stringValue: '6.2' }) }),
    );
  });
});
