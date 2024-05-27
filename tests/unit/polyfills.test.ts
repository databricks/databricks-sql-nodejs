import { expect } from 'chai';
import { at } from '../../lib/polyfills';

const arrayLikeStub = {
  0: 'a',
  1: 'b',
  2: 'c',
  3: 'd',
  length: 4,
  at,
};

describe('Array.at', () => {
  it('should handle zero index', () => {
    const obj = { ...arrayLikeStub };
    expect(obj.at(0)).to.eq('a');
    expect(obj.at(Number('+0'))).to.eq('a');
    expect(obj.at(Number('-0'))).to.eq('a');
  });

  it('should handle positive index', () => {
    const obj = { ...arrayLikeStub };
    expect(obj.at(2)).to.eq('c');
    expect(obj.at(2.2)).to.eq('c');
    expect(obj.at(2.8)).to.eq('c');
  });

  it('should handle negative index', () => {
    const obj = { ...arrayLikeStub };
    expect(obj.at(-2)).to.eq('c');
    expect(obj.at(-2.2)).to.eq('c');
    expect(obj.at(-2.8)).to.eq('c');
  });

  it('should handle positive infinity index', () => {
    const obj = { ...arrayLikeStub };
    expect(obj.at(Number.POSITIVE_INFINITY)).to.be.undefined;
  });

  it('should handle negative infinity index', () => {
    const obj = { ...arrayLikeStub };
    expect(obj.at(Number.NEGATIVE_INFINITY)).to.be.undefined;
  });

  it('should handle non-numeric index', () => {
    const obj = { ...arrayLikeStub };
    expect(obj.at('2')).to.eq('c');
  });

  it('should handle NaN index', () => {
    const obj = { ...arrayLikeStub };
    expect(obj.at(Number.NaN)).to.eq('a');
    expect(obj.at('invalid')).to.eq('a');
  });

  it('should handle index out of bounds', () => {
    const obj = { ...arrayLikeStub };
    expect(obj.at(10)).to.be.undefined;
    expect(obj.at(-10)).to.be.undefined;
  });

  it('should handle zero length', () => {
    const obj = { ...arrayLikeStub, length: 0 };
    expect(obj.at(2)).to.be.undefined;
  });

  it('should handle negative length', () => {
    const obj = { ...arrayLikeStub, length: -4 };
    expect(obj.at(2)).to.be.undefined;
  });

  it('should handle non-numeric length', () => {
    const obj = { ...arrayLikeStub, length: 'invalid' as any };
    expect(obj.at(2)).to.be.undefined;
  });
});
