import { TTableSchema } from '../../../thrift/TCLIService_types';

const thriftSchema: TTableSchema = {
  columns: [
    {
      columnName: 'bool',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 0,
            },
          },
        ],
      },
      position: 1,
    },
    {
      columnName: 'tiny_int',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 1,
            },
          },
        ],
      },
      position: 2,
    },
    {
      columnName: 'small_int',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 2,
            },
          },
        ],
      },
      position: 3,
    },
    {
      columnName: 'int_type',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 3,
            },
          },
        ],
      },
      position: 4,
    },
    {
      columnName: 'big_int',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 4,
            },
          },
        ],
      },
      position: 5,
    },
    {
      columnName: 'flt',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 5,
            },
          },
        ],
      },
      position: 6,
    },
    {
      columnName: 'dbl',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 6,
            },
          },
        ],
      },
      position: 7,
    },
    {
      columnName: 'dec',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 15,
              typeQualifiers: {
                qualifiers: {
                  scale: { i32Value: 2 },
                  precision: { i32Value: 3 },
                },
              },
            },
          },
        ],
      },
      position: 8,
    },
    {
      columnName: 'str',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 7,
            },
          },
        ],
      },
      position: 9,
    },
    {
      columnName: 'chr',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 7,
            },
          },
        ],
      },
      position: 10,
    },
    {
      columnName: 'vchr',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 7,
            },
          },
        ],
      },
      position: 11,
    },
    {
      columnName: 'ts',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 8,
            },
          },
        ],
      },
      position: 12,
    },
    {
      columnName: 'dat',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 17,
            },
          },
        ],
      },
      position: 13,
    },
    {
      columnName: 'day_interval',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 7,
            },
          },
        ],
      },
      position: 14,
    },
    {
      columnName: 'month_interval',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 7,
            },
          },
        ],
      },
      position: 15,
    },
    {
      columnName: 'bin',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 9,
            },
          },
        ],
      },
      position: 16,
    },
    {
      columnName: 'struct1',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 12,
            },
          },
        ],
      },
      position: 17,
    },
    {
      columnName: 'arr1',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 10,
            },
          },
        ],
      },
      position: 18,
    },
    {
      columnName: 'arr2',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 10,
            },
          },
        ],
      },
      position: 19,
    },
    {
      columnName: 'arr3',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 10,
            },
          },
        ],
      },
      position: 20,
    },
    {
      columnName: 'arr4',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 10,
            },
          },
        ],
      },
      position: 21,
    },
    {
      columnName: 'map1',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 11,
            },
          },
        ],
      },
      position: 22,
    },
    {
      columnName: 'map2',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 11,
            },
          },
        ],
      },
      position: 23,
    },
    {
      columnName: 'map3',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 11,
            },
          },
        ],
      },
      position: 24,
    },
    {
      columnName: 'map4',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 11,
            },
          },
        ],
      },
      position: 25,
    },
  ],
};

export default thriftSchema;
