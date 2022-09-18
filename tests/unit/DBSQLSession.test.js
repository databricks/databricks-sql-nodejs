const { expect } = require('chai');
const DBSQLSession = require('../../dist/DBSQLSession').default;
const InfoValue = require('../../dist/dto/InfoValue').default;
const Status = require('../../dist/dto/Status').default;
const DBSQLOperation = require('../../dist/DBSQLOperation').default;

const testMethod = (methodName, parameters, delegationToken) => {
  const driver = {
    [methodName]: () =>
      Promise.resolve({
        status: {
          statusCode: 0,
        },
        operationHandle: 'operationHandle',
        infoValue: {},
        delegationToken,
      }),
  };
  const session = new DBSQLSession(driver, { sessionId: 'id' });

  return session[methodName].apply(session, parameters);
};

describe('DBSQLSession', () => {
  describe('getInfo', () => {
    it('should run operation', async () => {
      const result = await testMethod('getInfo', [1]);
      expect(result).instanceOf(InfoValue);
    });
  });

  describe('executeStatement', () => {
    it('should execute statement', async () => {
      const result = await testMethod('executeStatement', ['SELECT * FROM table']);
      expect(result).instanceOf(DBSQLOperation);
    });
    it('should execute statement asynchronously', async () => {
      const result = await testMethod('executeStatement', ['SELECT * FROM table', { runAsync: true }]);
      expect(result).instanceOf(DBSQLOperation);
    });
    it('should use direct results', async () => {
      const result = await testMethod('executeStatement', ['SELECT * FROM table', { maxRows: 10 }]);
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getTypeInfo', () => {
    it('should run operation', async () => {
      const result = await testMethod('getTypeInfo', []);
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getCatalogs', () => {
    it('should run operation', async () => {
      const result = await testMethod('getCatalogs', []);
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getSchemas', () => {
    it('should run operation', async () => {
      const result = await testMethod('getSchemas', [
        {
          catalogName: 'catalog',
          schemaName: 'schema',
        },
      ]);
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getTables', () => {
    it('should run operation', async () => {
      const result = await testMethod('getTables', [
        {
          catalogName: 'catalog',
          schemaName: 'default',
          tableName: 't1',
          tableTypes: ['external'],
        },
      ]);
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getTableTypes', () => {
    it('should run operation', async () => {
      const result = await testMethod('getTableTypes', []);
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getColumns', () => {
    it('should run operation', async () => {
      const result = await testMethod('getColumns', [
        {
          catalogName: 'catalog',
          schemaName: 'schema',
          tableName: 'table',
          columnName: 'column',
        },
      ]);
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getFunctions', () => {
    it('should run operation', async () => {
      const result = await testMethod('getFunctions', [
        {
          catalogName: 'catalog',
          schemaName: 'schema',
          functionName: 'avg',
        },
      ]);
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getPrimaryKeys', () => {
    it('should run operation', async () => {
      const result = await testMethod('getPrimaryKeys', [
        {
          catalogName: 'catalog',
          schemaName: 'schema',
          tableName: 't1',
        },
      ]);
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getCrossReference', () => {
    it('should run operation', async () => {
      const result = await testMethod('getCrossReference', [
        {
          parentCatalogName: 'parentCatalogName',
          parentSchemaName: 'parentSchemaName',
          parentTableName: 'parentTableName',
          foreignCatalogName: 'foreignCatalogName',
          foreignSchemaName: 'foreignSchemaName',
          foreignTableName: 'foreignTableName',
        },
      ]);
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getDelegationToken', () => {
    it('should run operation', async () => {
      const result1 = await testMethod('getDelegationToken', ['owner', 'renewer'], 'token');
      expect(result1).to.be.eq('token');

      const result2 = await testMethod('getDelegationToken', ['owner', 'renewer']);
      expect(result2).to.be.eq('');
    });
  });

  describe('renewDelegationToken', () => {
    it('should run operation', async () => {
      const result = await testMethod('renewDelegationToken', ['token']);
      expect(result).instanceOf(Status);
    });
  });

  describe('cancelDelegationToken', () => {
    it('should run operation', async () => {
      const result = await testMethod('cancelDelegationToken', ['token']);
      expect(result).instanceOf(Status);
    });
  });

  describe('close', () => {
    it('should run operation', async () => {
      const driver = {
        closeSession: () =>
          Promise.resolve({
            status: {
              statusCode: 0,
            },
          }),
      };
      const session = new DBSQLSession(driver, { sessionId: 'id' });

      const result = await session.close();
      expect(result).instanceOf(Status);
    });
  });
});
