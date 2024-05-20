/* eslint-disable import/prefer-default-export */

// `Array.at` / `TypedArray.at` is supported only since Nodejs@16.6.0
// These methods are massively used by `apache-arrow@13`, but we have
// to use this version because older ones contain some other nasty bugs

// https://tc39.es/ecma262/multipage/abstract-operations.html#sec-tointegerorinfinity
function toIntegerOrInfinity(value: unknown): number {
  const result = Number(value);

  // Return `0` for NaN; return `+Infinity` / `-Infinity` as is
  if (!Number.isFinite(result)) {
    return Number.isNaN(result) ? 0 : result;
  }

  return Math.trunc(result);
}

// https://tc39.es/ecma262/multipage/abstract-operations.html#sec-tolength
function toLength(value: unknown): number {
  const result = toIntegerOrInfinity(value);
  return result > 0 ? Math.min(result, Number.MAX_SAFE_INTEGER) : 0;
}

// https://tc39.es/ecma262/multipage/indexed-collections.html#sec-array.prototype.at
export function at<T>(this: ArrayLike<T>, index: unknown): T | undefined {
  const length = toLength(this.length);
  const relativeIndex = toIntegerOrInfinity(index);
  const absoluteIndex = relativeIndex >= 0 ? relativeIndex : length + relativeIndex;
  return absoluteIndex >= 0 && absoluteIndex < length ? this[absoluteIndex] : undefined;
}

const ArrayConstructors = [
  global.Array,
  global.Int8Array,
  global.Uint8Array,
  global.Uint8ClampedArray,
  global.Int16Array,
  global.Uint16Array,
  global.Int32Array,
  global.Uint32Array,
  global.Float32Array,
  global.Float64Array,
  global.BigInt64Array,
  global.BigUint64Array,
];

ArrayConstructors.forEach((ArrayConstructor) => {
  if (typeof ArrayConstructor.prototype.at !== 'function') {
    ArrayConstructor.prototype.at = at;
  }
});
