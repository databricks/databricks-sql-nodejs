import { TTableSchema } from '../../../../thrift/TCLIService_types';

const thriftSchema: TTableSchema = {
  columns: [
    {
      columnName: 'boolean_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 0 },
          },
        ],
      },
      position: 1,
      comment: '',
    },
    {
      columnName: 'tinyint_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 1 },
          },
        ],
      },
      position: 2,
      comment: '',
    },
    {
      columnName: 'smallint_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 2 },
          },
        ],
      },
      position: 3,
      comment: '',
    },
    {
      columnName: 'int_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 3 },
          },
        ],
      },
      position: 4,
      comment: '',
    },
    {
      columnName: 'bigint_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 4 },
          },
        ],
      },
      position: 5,
      comment: '',
    },
    {
      columnName: 'float_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 5 },
          },
        ],
      },
      position: 6,
      comment: '',
    },
    {
      columnName: 'double_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 6 },
          },
        ],
      },
      position: 7,
      comment: '',
    },
    {
      columnName: 'decimal_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 15,
              typeQualifiers: {
                qualifiers: {
                  scale: { i32Value: 2 },
                  precision: { i32Value: 6 },
                },
              },
            },
          },
        ],
      },
      position: 8,
      comment: '',
    },
    {
      columnName: 'string_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 7 },
          },
        ],
      },
      position: 9,
      comment: '',
    },
    {
      columnName: 'char_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 7 },
          },
        ],
      },
      position: 10,
      comment: '',
    },
    {
      columnName: 'varchar_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 7 },
          },
        ],
      },
      position: 11,
      comment: '',
    },
    {
      columnName: 'timestamp_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 8 },
          },
        ],
      },
      position: 12,
      comment: '',
    },
    {
      columnName: 'date_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 17 },
          },
        ],
      },
      position: 13,
      comment: '',
    },
    {
      columnName: 'day_interval_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 7 },
          },
        ],
      },
      position: 14,
      comment: '',
    },
    {
      columnName: 'month_interval_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 7 },
          },
        ],
      },
      position: 15,
      comment: '',
    },
    {
      columnName: 'binary_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 9 },
          },
        ],
      },
      position: 16,
      comment: '',
    },
    {
      columnName: 'struct_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 12 },
          },
        ],
      },
      position: 17,
      comment: '',
    },
    {
      columnName: 'array_field',
      typeDesc: {
        types: [
          {
            primitiveEntry: { type: 10 },
          },
        ],
      },
      position: 18,
      comment: '',
    },
  ],
};

export default thriftSchema;
