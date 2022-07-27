import Int64 from 'node-int64';
import {
  TBoolColumn,
  TByteColumn,
  TI16Column,
  TI32Column,
  TI64Column,
  TDoubleColumn,
  TStringColumn,
  TBinaryColumn,
} from '../../../thrift/TCLIService_types';

export { Int64 };

export enum ColumnCode {
  boolVal = 'boolVal',
  byteVal = 'byteVal',
  i16Val = 'i16Val',
  i32Val = 'i32Val',
  i64Val = 'i64Val',
  doubleVal = 'doubleVal',
  stringVal = 'stringVal',
  binaryVal = 'binaryVal',
}

export type ColumnType =
  | TBoolColumn
  | TByteColumn
  | TI16Column
  | TI32Column
  | TI64Column
  | TDoubleColumn
  | TStringColumn
  | TBinaryColumn;
