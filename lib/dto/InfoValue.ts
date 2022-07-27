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
    } else if (infoValue.smallIntValue) {
      return infoValue.smallIntValue;
    } else if (infoValue.integerBitmask) {
      return infoValue.integerBitmask;
    } else if (infoValue.integerFlag) {
      return infoValue.integerFlag;
    } else if (infoValue.lenValue) {
      return infoValue.lenValue;
    } else {
      return null;
    }
  }
}
