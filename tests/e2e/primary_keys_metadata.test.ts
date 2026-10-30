import { expect } from 'chai';
import { v4 } from 'uuid';
import { DBSQLClient } from '../../lib';
import IDBSQLSession from '../../lib/contracts/IDBSQLSession';
import { TOperationState, TStatusCode, TTypeId } from '../../thrift/TCLIService_types';
import config from './utils/config';

describe('Thrift primary-key metadata', () => {
  const missingName = `missing_primary_keys_${v4().replace(/-/g, '')}`;
  let client: DBSQLClient;
  let session: IDBSQLSession;

  before(async () => {
    client = new DBSQLClient();
    await client.connect({ host: config.host, path: config.path, token: config.token });
    session = await client.openSession({ initialCatalog: config.catalog, initialSchema: config.schema });
  });

  after(async () => {
    try {
      await session?.close();
    } finally {
      await client?.close();
    }
  });

  [
    { missingObject: 'schema', catalogName: config.catalog, schemaName: missingName },
    { missingObject: 'catalog', catalogName: missingName, schemaName: config.schema },
    { missingObject: 'table', catalogName: config.catalog, schemaName: config.schema },
  ].forEach(({ missingObject, catalogName, schemaName }) => {
    [1000, null].forEach((maxRows) => {
      it(`returns typed empty primary keys for a missing ${missingObject} with direct results ${
        maxRows === null ? 'off' : 'on'
      }`, async () => {
        const operation = await session.getPrimaryKeys({
          catalogName,
          schemaName,
          tableName: missingName,
          maxRows,
        });
        try {
          const schema = await operation.getSchema();
          expect(
            schema?.columns.map((column) => [
              column.columnName,
              column.typeDesc.types[0].primitiveEntry?.type,
              column.position,
            ]),
          ).to.deep.equal([
            ['TABLE_CAT', TTypeId.STRING_TYPE, 1],
            ['TABLE_SCHEM', TTypeId.STRING_TYPE, 2],
            ['TABLE_NAME', TTypeId.STRING_TYPE, 3],
            ['COLUMN_NAME', TTypeId.STRING_TYPE, 4],
            ['KEQ_SEQ', TTypeId.INT_TYPE, 5],
            ['PK_NAME', TTypeId.STRING_TYPE, 6],
          ]);
          expect(await operation.fetchAll()).to.deep.equal([]);
          expect(await operation.hasMoreRows()).to.be.false;
          const status = await operation.status();
          expect(status.status.statusCode).to.equal(TStatusCode.SUCCESS_STATUS);
          expect(status.operationState).to.equal(TOperationState.FINISHED_STATE);
          expect(status.hasResultSet).to.be.true;
        } finally {
          await operation.close();
        }
      });
    });
  });
});
