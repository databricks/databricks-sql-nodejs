import { TGetInfoValue } from '../../thrift/TCLIService_types';
import { Int64 } from '../hive/Types';

type InfoResultType = string | number | Buffer | Int64 | null;

export default class InfoValue {
  private value: TGetInfoValue;

  constructor(value: TGetInfoValue) {
    this.value = value;
  }

  getValue(): InfoResultType {
    const infoValue = this.value;

    if (infoValue.stringValue) {
      return infoValue.stringValue;
    } if (infoValue.smallIntValue) {
      return infoValue.smallIntValue;
    } if (infoValue.integerBitmask) {
      return infoValue.integerBitmask;
    } if (infoValue.integerFlag) {
      return infoValue.integerFlag;
    } if (infoValue.lenValue) {
      return infoValue.lenValue;
    }
    return null;
  }
}
