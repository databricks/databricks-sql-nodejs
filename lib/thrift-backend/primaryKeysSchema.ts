import { TTableSchema, TTypeId } from '../../thrift/TCLIService_types';

export default function createPrimaryKeysSchema(): TTableSchema {
  return {
    // Preserve the Thrift server's KEQ_SEQ spelling.
    columns: ['TABLE_CAT', 'TABLE_SCHEM', 'TABLE_NAME', 'COLUMN_NAME', 'KEQ_SEQ', 'PK_NAME'].map(
      (columnName, index) => ({
        columnName,
        typeDesc: {
          types: [{ primitiveEntry: { type: columnName === 'KEQ_SEQ' ? TTypeId.INT_TYPE : TTypeId.STRING_TYPE } }],
        },
        position: index + 1,
        comment: '',
      }),
    ),
  };
}
