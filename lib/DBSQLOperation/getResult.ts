import { TRowSet, TTableSchema } from '../../thrift/TCLIService_types';
import IOperationResult from '../result/IOperationResult';
import NullResult from '../result/NullResult';
import JsonResult from '../result/JsonResult';

function getHandler(schema: TTableSchema | null, data: Array<TRowSet>): IOperationResult {
  if (schema === null) {
    return new NullResult();
  }
  return new JsonResult(schema, data);
}

export default function getResult(schema: TTableSchema | null, data: Array<TRowSet>) {
  const handler = getHandler(schema, data);
  return handler.getValue();
}
