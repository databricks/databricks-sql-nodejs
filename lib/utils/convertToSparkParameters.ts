import { TSparkParameter, TSparkParameterValue } from '../../thrift/TCLIService_types';
import DBSQLParameter from '../DBSQLParameter';
import HiveDriverError from '../errors/HiveDriverError';

function convertToDBSQLParameters(values: any[]): DBSQLParameter[] {
  const params: DBSQLParameter[] = [];
  for (const value of values) {
    switch (typeof value) {
      case 'object':
        if (value === null) {
          params.push(new DBSQLParameter({}));
          break;
        }
        if (value instanceof DBSQLParameter) {
          params.push(value);
          break;
        }
        throw new HiveDriverError('Unsupported object type used for parameterized query.');
      default:
        params.push(new DBSQLParameter({ value }));
    }
  }
  return params;
}
// Possible inputs to the params array:
// Naked args (will be converted to DBParameter)
function inferTypes(params: DBSQLParameter[]): void {
  for (const param of params) {
    if (!param.type) {
      switch (typeof param.value) {
        case 'undefined':
          param.type = 'VOID';
          break;
        case 'boolean':
          param.type = 'BOOLEAN';
          param.value = param.value.toString();
          break;
        case 'number':
          if (Number.isInteger(param.value)) {
            param.type = 'INTEGER';
          } else {
            param.type = 'DOUBLE';
          }
          param.value = param.value.toString();
          break;
        case 'string':
          param.type = 'STRING';
          break;
        default:
          throw new HiveDriverError('Unsupported object type used for parameterized query.');
      }
    }
  }
}
export default function convertToSparkParameters(values: any[]): TSparkParameter[] {
  const params = convertToDBSQLParameters(values);
  const retVal: TSparkParameter[] = [];
  inferTypes(params);
  for (const param of params) {
    switch (typeof param.value) {
      case 'string':
        retVal.push(
          new TSparkParameter({
            name: param.name,
            value: new TSparkParameterValue({ stringValue: param.value }),
            type: param.type,
          }),
        );
        break;
      case 'undefined':
        retVal.push(new TSparkParameter({ name: param.name, value: new TSparkParameterValue({}), type: param.type }));
        break;
      default:
        // Cast to a string and then return param
        retVal.push(
          new TSparkParameter({
            name: param.name,
            value: new TSparkParameterValue({ stringValue: param.value.toString() }),
            type: param.type,
          }),
        );
    }
  }
  return retVal;
}
