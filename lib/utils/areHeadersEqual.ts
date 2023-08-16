import { HttpHeaders } from 'thrift';

function areArraysEqual<T>(a: Array<T>, b: Array<T>): boolean {
  // If they're the same object - they're equal
  if (a === b) {
    return true;
  }

  // If they have a different size - they're definitely not equal
  if (a.length !== b.length) {
    return false;
  }

  // Here we have arrays of same size. Compare elements - if any pair is different
  // then arrays are different
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) {
      return false;
    }
  }

  // If all corresponding elements in both arrays are equal - arrays are equal too
  return true;
}

export default function areHeadersEqual(a: HttpHeaders, b: HttpHeaders): boolean {
  // If they're the same object - they're equal
  if (a === b) {
    return true;
  }

  // If both objects have different keys - they're not equal
  const keysOfA = Object.keys(a);
  const keysOfB = Object.keys(b);
  if (!areArraysEqual(keysOfA, keysOfB)) {
    return false;
  }

  // Compare corresponding properties of both objects. If any pair is different - objects are different
  for (const key of keysOfA) {
    const propA = a[key];
    const propB = b[key];

    if (Array.isArray(propA) && Array.isArray(propB)) {
      if (!areArraysEqual(propA, propB)) {
        return false;
      }
    } else if (propA !== propB) {
      return false;
    }
  }

  return true;
}
