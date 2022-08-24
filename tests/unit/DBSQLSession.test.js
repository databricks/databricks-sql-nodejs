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
  it('getInfo', () => {
    return testMethod('getInfo', [1]).then((result) => {
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
      const result = await testMethod('executeStatement', ['SELECT * FROM table', { prefetchRows: 10 }]);
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  it('getTypeInfo', () => {
    return testMethod('getTypeInfo', []).then((result) => {
      expect(result).instanceOf(DBSQLOperation);
    });
  });
  it('getCatalogs', () => {
    return testMethod('getCatalogs', []).then((result) => {
      expect(result).instanceOf(DBSQLOperation);
    });
  });
  it('getSchemas', () => {
    return testMethod('getSchemas', [
      {
        catalogName: 'catalog',
        schemaName: 'schema',
      },
    ]).then((result) => {
      expect(result).instanceOf(DBSQLOperation);
    });
  });
  it('getTables', () => {
    return testMethod('getTables', [
      {
        catalogName: 'catalog',
        schemaName: 'default',
        tableName: 't1',
        tableTypes: ['external'],
      },
    ]).then((result) => {
      expect(result).instanceOf(DBSQLOperation);
    });
  });
  it('getTableTypes', () => {
    return testMethod('getTableTypes', []).then((result) => {
      expect(result).instanceOf(DBSQLOperation);
    });
  });
  it('getColumns', () => {
    return testMethod('getColumns', [
      {
        catalogName: 'catalog',
        schemaName: 'schema',
        tableName: 'table',
        columnName: 'column',
      },
    ]).then((result) => {
      expect(result).instanceOf(DBSQLOperation);
    });
  });
  it('getFunctions', () => {
    return testMethod('getFunctions', [
      {
        catalogName: 'catalog',
        schemaName: 'schema',
        functionName: 'avg',
      },
    ]).then((result) => {
      expect(result).instanceOf(DBSQLOperation);
    });
  });
  it('getPrimaryKeys', () => {
    return testMethod('getPrimaryKeys', [
      {
        catalogName: 'catalog',
        schemaName: 'schema',
        tableName: 't1',
      },
    ]).then((result) => {
      expect(result).instanceOf(DBSQLOperation);
    });
  });
  it('getCrossReference', () => {
    return testMethod('getCrossReference', [
      {
        parentCatalogName: 'parentCatalogName',
        parentSchemaName: 'parentSchemaName',
        parentTableName: 'parentTableName',
        foreignCatalogName: 'foreignCatalogName',
        foreignSchemaName: 'foreignSchemaName',
        foreignTableName: 'foreignTableName',
      },
    ]).then((result) => {
      expect(result).instanceOf(DBSQLOperation);
    });
  });
  it('getDelegationToken', () => {
    return testMethod('getDelegationToken', ['owner', 'renewer'], 'token')
      .then((result) => {
        expect(result).to.be.eq('token');
      })
      .then(() => {
        return testMethod('getDelegationToken', ['owner', 'renewer']);
      })
      .then((result) => {
        expect(result).to.be.eq('');
      });
  });
  it('renewDelegationToken', () => {
    return testMethod('renewDelegationToken', ['token']).then((result) => {
      expect(result).instanceOf(Status);
    });
  });
  it('cancelDelegationToken', () => {
    return testMethod('cancelDelegationToken', ['token']).then((result) => {
      expect(result).instanceOf(Status);
    });
  });

  it('close', () => {
    const driver = {
      closeSession: () =>
        Promise.resolve({
          status: {
            statusCode: 0,
          },
        }),
    };
    const session = new DBSQLSession(driver, { sessionId: 'id' });

    return session.close().then((result) => {
      expect(result).instanceOf(Status);
    });
  });
});
