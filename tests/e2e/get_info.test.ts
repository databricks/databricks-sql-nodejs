import { expect } from 'chai';
import { DBSQLClient } from '../../lib';
import IDBSQLSession from '../../lib/contracts/IDBSQLSession';
import { InternalConnectionOptions } from '../../lib/contracts/InternalConnectionOptions';
import { TGetInfoType } from '../../thrift/TCLIService_types';
import config from './utils/config';

// ODBC SQL_TXN_SERIALIZABLE
const SQL_TXN_SERIALIZABLE = 8;

[false, true].forEach((useKernel) => {
  describe(`getInfo (${useKernel ? 'kernel' : 'thrift'})`, () => {
    let client: DBSQLClient;
    let session: IDBSQLSession;

    before(async () => {
      client = new DBSQLClient();
      await client.connect({
        host: config.host,
        path: config.path,
        token: config.token,
        ...({ useKernel } as InternalConnectionOptions),
      });
      session = await client.openSession();
    });

    after(async () => {
      try {
        await session?.close();
      } finally {
        await client?.close();
      }
    });

    it('reports SQL_TXN_SERIALIZABLE as the default and only supported isolation level', async () => {
      const defaultLevel = await session.getInfo(TGetInfoType.CLI_DEFAULT_TXN_ISOLATION);
      const supportedLevels = await session.getInfo(TGetInfoType.CLI_TXN_ISOLATION_OPTION);
      expect(defaultLevel.getValue()).to.equal(SQL_TXN_SERIALIZABLE);
      expect(supportedLevels.getValue()).to.equal(SQL_TXN_SERIALIZABLE);
    });

    it('still returns the server-answered DBMS version', async () => {
      const version = await session.getInfo(TGetInfoType.CLI_DBMS_VER);
      expect(version.getValue()).to.equal('3.1.1');
    });
  });
});
