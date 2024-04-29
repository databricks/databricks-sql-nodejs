const { expect } = require('chai');
const InfoValue = require('../../../lib/dto/InfoValue').default;
const NodeInt64 = require('node-int64');

const createInfoValueMock = (value) =>
  Object.assign(
    {
      stringValue: null,
      smallIntValue: null,
      integerBitmask: null,
      integerFlag: null,
      lenValue: null,
    },
    value,
  );

describe('InfoValue', () => {
  it('should return string', () => {
    const value = new InfoValue(
      createInfoValueMock({
        stringValue: 'value',
      }),
    );

    expect(value.getValue()).to.be.eq('value');
  });

  it('should return number', () => {
    const smallInt = new InfoValue(
      createInfoValueMock({
        smallIntValue: 1,
      }),
    );

    expect(smallInt.getValue()).to.be.eq(1);

    const bitMask = new InfoValue(
      createInfoValueMock({
        integerBitmask: 0xaa55aa55,
      }),
    );

    expect(bitMask.getValue()).to.be.eq(0xaa55aa55);

    const integerFlag = new InfoValue(
      createInfoValueMock({
        integerFlag: 0x01,
      }),
    );

    expect(integerFlag.getValue()).to.be.eq(0x01);
  });

  it('should return int64', () => {
    const value = new InfoValue(
      createInfoValueMock({
        lenValue: new NodeInt64(Buffer.from([0x00, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10])),
      }),
    );

    expect(value.getValue()).to.be.instanceOf(NodeInt64);
    expect(value.getValue().toNumber()).to.be.eq(4521260802379792);
  });

  it('should return null for empty info value', () => {
    const value = new InfoValue(createInfoValueMock({}));

    expect(value.getValue()).to.be.null;
  });
});
