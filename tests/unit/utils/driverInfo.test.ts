import { expect } from 'chai';
import { driverInfoValue, SQL_TXN_SERIALIZABLE } from '../../../lib/utils/driverInfo';
import InfoValue from '../../../lib/dto/InfoValue';
import { TGetInfoType } from '../../../thrift/TCLIService_types';

describe('driverInfoValue', () => {
  it('reports SQL_TXN_SERIALIZABLE as the default isolation level', () => {
    const value = driverInfoValue(TGetInfoType.CLI_DEFAULT_TXN_ISOLATION);
    expect(value?.integerFlag).to.equal(SQL_TXN_SERIALIZABLE);
    expect(new InfoValue(value!).getValue()).to.equal(8);
  });

  it('reports SQL_TXN_SERIALIZABLE as the only supported isolation level', () => {
    const value = driverInfoValue(TGetInfoType.CLI_TXN_ISOLATION_OPTION);
    expect(value?.integerBitmask).to.equal(SQL_TXN_SERIALIZABLE);
    expect(new InfoValue(value!).getValue()).to.equal(8);
  });

  it('returns undefined for every other info type', () => {
    expect(driverInfoValue(TGetInfoType.CLI_SERVER_NAME)).to.equal(undefined);
    expect(driverInfoValue(TGetInfoType.CLI_DBMS_NAME)).to.equal(undefined);
    expect(driverInfoValue(TGetInfoType.CLI_DBMS_VER)).to.equal(undefined);
    expect(driverInfoValue(TGetInfoType.CLI_TXN_CAPABLE)).to.equal(undefined);
    expect(driverInfoValue(99999)).to.equal(undefined);
  });
});
