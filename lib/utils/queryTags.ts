/**
 * Serializes a query tags dictionary into a string for use in confOverlay.
 *
 * Format: comma-separated key:value pairs, e.g. "key1:value1,key2:value2"
 * - If a value is null or undefined, the key is included without a colon or value
 * - Backslashes in keys are escaped; other special characters in keys are not escaped
 * - Special characters (backslash, colon, comma) in values are backslash-escaped
 *
 * @param queryTags - dictionary of query tag key-value pairs
 * @returns serialized string, or undefined if input is empty/null/undefined
 */
export default function serializeQueryTags(
  queryTags: Record<string, string | null | undefined> | null | undefined,
): string | undefined {
  if (queryTags == null) {
    return undefined;
  }

  const keys = Object.keys(queryTags);
  if (keys.length === 0) {
    return undefined;
  }

  return keys
    .map((key) => {
      const escapedKey = key.replace(/\\/g, '\\\\');
      const value = queryTags[key];
      if (value == null) {
        return escapedKey;
      }
      const escapedValue = value.replace(/[\\:,]/g, (c) => `\\${c}`);
      return `${escapedKey}:${escapedValue}`;
    })
    .join(',');
}
