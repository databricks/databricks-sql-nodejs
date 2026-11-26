import { expect } from 'chai';
import { TGetResultSetMetadataResp, TStatusCode, TTableSchema, TTypeId } from '../../../thrift/TCLIService_types';
import getResultSchema from '../../../lib/thrift-backend/resultSchema';
import { hiveSchemaToArrowSchema } from '../../../lib/result/utils';

function stringColumn(columnName: string, position: number) {
  return { columnName, position, typeDesc: { types: [{ primitiveEntry: { type: TTypeId.STRING_TYPE } }] } };
}

// What the server sends for DDL: a placeholder column alongside an Arrow schema with no fields.
const placeholder: TTableSchema = { columns: [stringColumn('Result', 1)] };

function metadata(schema: TTableSchema, arrowSchema?: Buffer): TGetResultSetMetadataResp {
  return { status: { statusCode: TStatusCode.SUCCESS_STATUS }, schema, arrowSchema };
}

describe('getResultSchema', () => {
  it('reports no columns when the Arrow schema has no fields', () => {
    expect(getResultSchema(metadata(placeholder, hiveSchemaToArrowSchema({ columns: [] })))).to.deep.equal({
      columns: [],
    });
  });

  it('keeps a real `Result` column', () => {
    const md = metadata(placeholder, hiveSchemaToArrowSchema(placeholder));
    expect(getResultSchema(md)).to.equal(placeholder);
  });

  it('keeps the placeholder without an Arrow schema', () => {
    expect(getResultSchema(metadata(placeholder))).to.equal(placeholder);
  });

  it('keeps the placeholder when the Arrow schema cannot be decoded', () => {
    expect(getResultSchema(metadata(placeholder, Buffer.from('not arrow')))).to.equal(placeholder);
  });

  it('returns other schemas unchanged', () => {
    const schema: TTableSchema = { columns: [stringColumn('Result', 1), stringColumn('other', 2)] };
    expect(getResultSchema(metadata(schema, hiveSchemaToArrowSchema({ columns: [] })))).to.equal(schema);
  });
});
