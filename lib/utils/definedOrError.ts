export default function definedOrError<T>(value: T | undefined): T {
  if (value === undefined) {
    throw new TypeError('Value is undefined');
  }
  return value;
}
