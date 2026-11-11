import { expect } from 'chai';
import sinon from 'sinon';
import { DBSQLClient } from '../../lib';
import { ClientConfig } from '../../lib/contracts/IClientContext';
import IDBSQLSession from '../../lib/contracts/IDBSQLSession';

import config from './utils/config';

async function openSession(customConfig: Partial<ClientConfig> = {}) {
  const client = new DBSQLClient();

  const clientConfig = client.getConfig();
  sinon.stub(client, 'getConfig').returns({
    ...clientConfig,
    ...customConfig,
  });

  const connection = await client.connect({
    host: config.host,
    path: config.path,
    token: config.token,
  });

  return connection.openSession({
    initialCatalog: config.catalog,
    initialSchema: config.schema,
  });
}

async function execute(session: IDBSQLSession, statement: string) {
  const operation = await session.executeStatement(statement);
  const result = await operation.fetchAll();
  await operation.close();
  return result as Array<any>;
}

const structQuery = `
  SELECT
    named_struct('id', 1, 'ts', TIMESTAMP'2021-12-21 21:33:59.339', 'd', DATE'2021-12-21', 's', 'x') AS st,
    CAST(TIMESTAMP'2021-12-21 21:33:59.339' AS STRING) AS ts_str,
    CAST(DATE'2021-12-21' AS STRING) AS d_str
`;

const collectionsQuery = `
  SELECT
    array(TIMESTAMP'2021-12-21 21:33:59.339', TIMESTAMP'2022-01-01 00:00:00') AS arr,
    map('a', TIMESTAMP'2021-12-21 21:33:59.339') AS m,
    CAST(TIMESTAMP'2021-12-21 21:33:59.339' AS STRING) AS ts1_str,
    CAST(TIMESTAMP'2022-01-01 00:00:00' AS STRING) AS ts2_str
`;

describe('Complex types containing dates/timestamps (issue #103)', () => {
  it('struct with nested timestamp/date is parsed (JSON results)', async () => {
    const session = await openSession({ arrowEnabled: false });
    try {
      const [row] = await execute(session, structQuery);
      expect(row.st).to.deep.equal({ id: 1, ts: row.ts_str, d: row.d_str, s: 'x' });
    } finally {
      await session.close();
    }
  });

  it('array and map with nested timestamps are parsed (JSON results)', async () => {
    const session = await openSession({ arrowEnabled: false });
    try {
      const [row] = await execute(session, collectionsQuery);
      expect(row.arr).to.deep.equal([row.ts1_str, row.ts2_str]);
      expect(row.m).to.deep.equal({ a: row.ts1_str });
    } finally {
      await session.close();
    }
  });

  it('struct with nested timestamp/date is parsed (Arrow results without native types)', async () => {
    const session = await openSession({ arrowEnabled: true, useArrowNativeTypes: false });
    try {
      const [row] = await execute(session, structQuery);
      expect(row.st).to.deep.equal({ id: 1, ts: row.ts_str, d: row.d_str, s: 'x' });
    } finally {
      await session.close();
    }
  });
});
