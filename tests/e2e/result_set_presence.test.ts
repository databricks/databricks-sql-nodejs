import { expect } from 'chai';
import sinon from 'sinon';
import { DBSQLClient } from '../../lib';
import { ConnectionOptions } from '../../lib/contracts/IDBSQLClient';
import { ClientConfig } from '../../lib/contracts/IClientContext';
import { InternalConnectionOptions } from '../../lib/contracts/InternalConnectionOptions';
import IDBSQLSession from '../../lib/contracts/IDBSQLSession';
import { tryGetKernelNative } from '../../lib/kernel/KernelNativeLoader';
import config from './utils/config';

async function openSession(useKernel: boolean, customConfig: Partial<ClientConfig> = {}) {
  const client = new DBSQLClient();
  if (Object.keys(customConfig).length > 0) {
    const clientConfig = client.getConfig();
    sinon.stub(client, 'getConfig').returns({ ...clientConfig, ...customConfig });
  }
  await client.connect({
    host: config.host,
    path: config.path,
    token: config.token,
    useKernel,
  } as ConnectionOptions & InternalConnectionOptions);
  const session = await client.openSession({ initialCatalog: config.catalog, initialSchema: config.schema });
  return { client, session };
}

async function hasResultSet(session: IDBSQLSession, statement: string) {
  const operation = await session.executeStatement(statement);
  try {
    const { hasResultSet: value } = await operation.getResultMetadata();
    await operation.fetchAll();
    return value;
  } finally {
    await operation.close();
  }
}

const backends = [
  { name: 'Thrift', useKernel: false },
  { name: 'kernel', useKernel: true, skip: tryGetKernelNative() === undefined },
];

backends.forEach(({ name, useKernel, skip }) => {
  describe(`getResultMetadata().hasResultSet (${name})`, () => {
    const table = `${config.catalog}.${config.schema}.result_set_presence_${config.tableSuffix}_${name}`;
    let client: DBSQLClient;
    let session: IDBSQLSession;

    before(async function setup() {
      if (skip) {
        // eslint-disable-next-line no-invalid-this
        this.skip();
      }
      ({ client, session } = await openSession(useKernel));
    });

    after(async () => {
      try {
        if (session) {
          await hasResultSet(session, `DROP TABLE IF EXISTS ${table}`);
          await session.close();
        }
      } finally {
        await client?.close();
      }
    });

    it('is false for DDL and true for an empty SELECT', async () => {
      expect(await hasResultSet(session, `CREATE TABLE IF NOT EXISTS ${table} (col1 INT)`)).to.be.false;
      expect(await hasResultSet(session, `SELECT 'sample' AS Result WHERE 1 = 0`)).to.be.true;
      expect(await hasResultSet(session, 'SELECT 1 AS one')).to.be.true;
      expect(await hasResultSet(session, `INSERT INTO ${table} VALUES (1)`)).to.be.true;
      expect(await hasResultSet(session, `USE SCHEMA ${config.schema}`)).to.be.false;
    });

    it('is true for catalog operations', async () => {
      const operation = await session.getTables({ catalogName: config.catalog, schemaName: config.schema });
      try {
        expect((await operation.getResultMetadata()).hasResultSet).to.be.true;
      } finally {
        await operation.close();
      }
    });

    if (!useKernel) {
      it('is undefined when Arrow results are disabled', async () => {
        const columnar = await openSession(false, { arrowEnabled: false });
        try {
          expect(await hasResultSet(columnar.session, `SELECT 'sample' AS Result WHERE 1 = 0`)).to.be.undefined;
        } finally {
          await columnar.session.close();
          await columnar.client.close();
          sinon.restore();
        }
      });
    }
  });
});
