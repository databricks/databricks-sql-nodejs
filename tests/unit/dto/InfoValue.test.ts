import { expect } from 'chai';
import Int64 from 'node-int64';
import InfoValue from '../../../lib/dto/InfoValue';

describe('InfoValue', () => {
  it('should return string', () => {
    const value = new InfoValue({
      stringValue: 'value',
    });

    expect(value.getValue()).to.be.eq('value');
  });

  it('should return number', () => {
    const smallInt = new InfoValue({
      smallIntValue: 1,
    });

    expect(smallInt.getValue()).to.be.eq(1);

    const bitMask = new InfoValue({
      integerBitmask: 0xaa55aa55,
    });

    expect(bitMask.getValue()).to.be.eq(0xaa55aa55);

    const integerFlag = new InfoValue({
      integerFlag: 0x01,
    });

    expect(integerFlag.getValue()).to.be.eq(0x01);
  });

  it('should return int64', () => {
    const value = new InfoValue({
      lenValue: new Int64(Buffer.from([0x00, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10])),
    });

    expect(value.getValue()).to.be.instanceOf(Int64);
    expect(value.getValue()?.toString()).to.be.eq('4521260802379792');
  });

  it('should return null for empty info value', () => {
    const value = new InfoValue({});

    expect(value.getValue()).to.be.null;
  });
});
