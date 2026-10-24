import { TTableSchema, TTypeId } from '../../thrift/TCLIService_types';

const primaryKeysSchema: TTableSchema = {
  columns: ['TABLE_CAT', 'TABLE_SCHEM', 'TABLE_NAME', 'COLUMN_NAME', 'KEY_SEQ', 'PK_NAME'].map((columnName, index) => ({
    columnName,
    typeDesc: {
      types: [{ primitiveEntry: { type: columnName === 'KEY_SEQ' ? TTypeId.INT_TYPE : TTypeId.STRING_TYPE } }],
    },
    position: index + 1,
    comment: '',
  })),
};

export default primaryKeysSchema;
