import * as fs from 'fs';
import * as path from 'path';
import { expect } from 'chai';

/**
 * Validates that all Thrift-generated classes comply with field ID constraints.
 *
 * Field IDs in Thrift must stay below 3329 to avoid conflicts with reserved ranges and ensure
 * compatibility with various Thrift implementations and protocols.
 */
describe('Thrift Field ID Validation', () => {
  const MAX_ALLOWED_FIELD_ID = 3329;
  const THRIFT_DIR = path.join(__dirname, '../../thrift');

  it('should ensure all Thrift field IDs are within allowed range', () => {
    const violations: string[] = [];

    // Get all JavaScript files in the thrift directory
    const thriftFiles = fs
      .readdirSync(THRIFT_DIR)
      .filter((file) => file.endsWith('.js'))
      .map((file) => path.join(THRIFT_DIR, file));

    expect(thriftFiles.length).to.be.greaterThan(0, 'No Thrift JavaScript files found');

    for (const filePath of thriftFiles) {
      const fileName = path.basename(filePath);
      const fileContent = fs.readFileSync(filePath, 'utf8');

      // Extract field IDs from both read and write functions
      const fieldIds = extractFieldIds(fileContent);

      for (const fieldId of fieldIds) {
        if (fieldId >= MAX_ALLOWED_FIELD_ID) {
          violations.push(
            `${fileName}: Field ID ${fieldId} exceeds maximum allowed value of ${MAX_ALLOWED_FIELD_ID - 1}`,
          );
        }
      }
    }

    if (violations.length > 0) {
      const errorMessage = [
        `Found Thrift field IDs that exceed the maximum allowed value of ${MAX_ALLOWED_FIELD_ID - 1}.`,
        'This can cause compatibility issues and conflicts with reserved ID ranges.',
        'Violations found:',
        ...violations.map((v) => `  - ${v}`),
      ].join('\n');

      throw new Error(errorMessage);
    }
  });
});

/**
 * Extracts all field IDs from the given Thrift JavaScript file content.
 * Looks for field IDs in both read functions (case statements) and write functions (writeFieldBegin calls).
 */
function extractFieldIds(fileContent: string): number[] {
  const fieldIds = new Set<number>();

  // Pattern 1: Extract field IDs from case statements in read functions
  // Example: case 1281:
  const casePattern = /case\s+(\d+):/g;
  let match;

  while ((match = casePattern.exec(fileContent)) !== null) {
    const fieldId = parseInt(match[1], 10);
    if (!isNaN(fieldId)) {
      fieldIds.add(fieldId);
    }
  }

  // Pattern 2: Extract field IDs from writeFieldBegin calls in write functions
  // Example: output.writeFieldBegin('errorDetailsJson', Thrift.Type.STRING, 1281);
  const writeFieldPattern = /writeFieldBegin\([^,]+,\s*[^,]+,\s*(\d+)\)/g;

  while ((match = writeFieldPattern.exec(fileContent)) !== null) {
    const fieldId = parseInt(match[1], 10);
    if (!isNaN(fieldId)) {
      fieldIds.add(fieldId);
    }
  }

  return Array.from(fieldIds).sort((a, b) => a - b);
}
