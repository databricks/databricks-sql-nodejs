import { expect } from 'chai';
import { DBSQLClient } from '../../lib';
import { ConnectionOptions } from '../../lib/contracts/IDBSQLClient';
import { InternalConnectionOptions } from '../../lib/contracts/InternalConnectionOptions';

import config from './utils/config';

// From #309: DDL and an empty SELECT both return no rows; only the schema tells them apart.
describe('schema of statements without output', () => {
  for (const [name, useKernel] of [
    ['Thrift', false],
    ['kernel', true],
  ] as const) {
    it(`reports no columns for DDL with ${name}`, async () => {
      const client = new DBSQLClient();
      await client.connect({
        host: config.host,
        path: config.path,
        token: config.token,
        useKernel,
      } as ConnectionOptions & InternalConnectionOptions);
      const session = await client.openSession({ initialCatalog: config.catalog, initialSchema: config.schema });
      const table = `dbsql_nodejs_sdk_e2e_ddl_schema_${name.toLowerCase()}_${config.tableSuffix}`;

      const columnsOf = async (statement: string) => {
        const operation = await session.executeStatement(statement);
        expect(await operation.fetchAll()).to.deep.equal([]);
        const schema = await operation.getSchema();
        await operation.close();
        return schema?.columns.map((column) => column.columnName);
      };

      try {
        expect(await columnsOf(`SELECT 'sample' AS Result WHERE 1 = 0`)).to.deep.equal(['Result']);
        expect(await columnsOf(`CREATE TABLE ${table} (col1 INT)`)).to.deep.equal([]);
        expect(await columnsOf(`DROP TABLE ${table}`)).to.deep.equal([]);
      } finally {
        const cleanup = await session.executeStatement(`DROP TABLE IF EXISTS ${table}`);
        await cleanup.close();
        await session.close();
        await client.close();
      }
    });
  }
});
