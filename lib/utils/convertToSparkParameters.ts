import { TSparkParameter, TSparkParameterValue } from '../../thrift/TCLIService_types';
import HiveDriverError from '../errors/HiveDriverError';

function getTypeAndValue(value: any): [string, TSparkParameterValue] {
  switch (typeof value) {
    case 'object':
      if (value === null) {
        return ['VOID', new TSparkParameterValue()];
      }

      throw new HiveDriverError('Unsupported object type used for parameterized query.');

    case 'boolean':
      return ['BOOLEAN', new TSparkParameterValue({ booleanValue: value })];
    case 'number':
      if (Number.isInteger(value)) {
        return ['INT', new TSparkParameterValue({ doubleValue: value })];
      }

      return ['DOUBLE', new TSparkParameterValue({ doubleValue: value })];

    case 'string':
      return ['STRING', new TSparkParameterValue({ stringValue: value })];
    default:
      throw new HiveDriverError('Unsupported object type used for parameterized query.');
  }
}

export default function convertToSparkParameters(params: object): TSparkParameter[] {
  const sparkValueParams = [];
  for (const e of Object.entries(params)) {
    const key = e[0];
    const value = e[1];
    const typeValueTuple = getTypeAndValue(value);
    sparkValueParams.push(new TSparkParameter({ name: key, type: typeValueTuple[0], value: typeValueTuple[1] }));
  }
  return sparkValueParams;
}
