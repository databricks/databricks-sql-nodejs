import { TColumn, TFetchResultsResp } from '../../thrift/TCLIService_types';
import { ColumnCode } from '../hive/Types';

export default function checkIfOperationHasMoreRows(response: TFetchResultsResp): boolean {
  if (response.hasMoreRows) {
    return true;
  }

  const columns = response.results?.columns || [];

  if (!columns.length) {
    return false;
  }

  const column: TColumn = columns[0];

  const columnValue = column[ColumnCode.binaryVal]
    || column[ColumnCode.boolVal]
    || column[ColumnCode.byteVal]
    || column[ColumnCode.doubleVal]
    || column[ColumnCode.i16Val]
    || column[ColumnCode.i32Val]
    || column[ColumnCode.i64Val]
    || column[ColumnCode.stringVal];

  return (columnValue?.values?.length || 0) > 0;
}
