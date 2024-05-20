import { AssertionError, expect } from 'chai';
import sinon, { SinonSpy } from 'sinon';
import Int64 from 'node-int64';
import DBSQLSession, { numberToInt64 } from '../../lib/DBSQLSession';
import InfoValue from '../../lib/dto/InfoValue';
import Status from '../../lib/dto/Status';
import DBSQLOperation from '../../lib/DBSQLOperation';
import { TSessionHandle } from '../../thrift/TCLIService_types';
import ClientContextStub from './.stubs/ClientContextStub';

const sessionHandleStub: TSessionHandle = {
  sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
};

class DBSQLSessionTest extends DBSQLSession {
  public inspectInternals() {
    return {
      isOpen: this.isOpen,
      operations: this.operations,
    };
  }
}

async function expectFailure(fn: () => Promise<unknown>) {
  try {
    await fn();
    expect.fail('It should throw an error');
  } catch (error) {
    if (error instanceof AssertionError) {
      throw error;
    }
  }
}

describe('DBSQLSession', () => {
  describe('numberToInt64', () => {
    it('should convert regular number to Int64', () => {
      const num = Math.random() * 1000000;
      const value = numberToInt64(num);
      expect(value.equals(new Int64(num))).to.be.true;
    });

    it('should return Int64 values as is', () => {
      const num = new Int64(Math.random() * 1000000);
      const value = numberToInt64(num);
      expect(value).to.equal(num);
    });

    it('should convert BigInt to Int64', () => {
      // This case is especially important, because Int64 has no native methods to convert
      // between Int64 and BigInt. This conversion involves some byte operations, and it's
      // important to make sure we don't mess up with things like byte order

      const num = BigInt(Math.round(Math.random() * 10000)) * BigInt(Math.round(Math.random() * 10000));
      const value = numberToInt64(num);
      expect(value.toString()).equal(num.toString());
    });
  });

  describe('getInfo', () => {
    it('should run operation', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getInfo(1);
      expect(result).instanceOf(InfoValue);
    });
  });

  describe('executeStatement', () => {
    it('should execute statement', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.executeStatement('SELECT * FROM table');
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.executeStatement('SELECT * FROM table', { maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.executeStatement('SELECT * FROM table', { maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });

    describe('Arrow support', () => {
      it('should not use Arrow if disabled in options', async () => {
        const session = new DBSQLSessionTest({
          handle: sessionHandleStub,
          context: new ClientContextStub({ arrowEnabled: false }),
        });
        const result = await session.executeStatement('SELECT * FROM table');
        expect(result).instanceOf(DBSQLOperation);
      });

      it('should apply defaults for Arrow options', async () => {
        case1: {
          const session = new DBSQLSessionTest({
            handle: sessionHandleStub,
            context: new ClientContextStub({ arrowEnabled: true }),
          });
          const result = await session.executeStatement('SELECT * FROM table');
          expect(result).instanceOf(DBSQLOperation);
        }

        case2: {
          const session = new DBSQLSessionTest({
            handle: sessionHandleStub,
            context: new ClientContextStub({ arrowEnabled: true, useArrowNativeTypes: false }),
          });
          const result = await session.executeStatement('SELECT * FROM table');
          expect(result).instanceOf(DBSQLOperation);
        }
      });
    });
  });

  describe('getTypeInfo', () => {
    it('should run operation', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTypeInfo();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTypeInfo({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTypeInfo({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getCatalogs', () => {
    it('should run operation', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getCatalogs();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getCatalogs({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getCatalogs({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getSchemas', () => {
    it('should run operation', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getSchemas();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use filters', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getSchemas({
        catalogName: 'catalog',
        schemaName: 'schema',
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getSchemas({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getSchemas({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getTables', () => {
    it('should run operation', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTables();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use filters', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTables({
        catalogName: 'catalog',
        schemaName: 'default',
        tableName: 't1',
        tableTypes: ['external'],
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTables({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTables({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getTableTypes', () => {
    it('should run operation', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTableTypes();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTableTypes({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTableTypes({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getColumns', () => {
    it('should run operation', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getColumns();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use filters', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getColumns({
        catalogName: 'catalog',
        schemaName: 'schema',
        tableName: 'table',
        columnName: 'column',
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getColumns({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getColumns({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getFunctions', () => {
    it('should run operation', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getFunctions({
        catalogName: 'catalog',
        schemaName: 'schema',
        functionName: 'avg',
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getFunctions({
        catalogName: 'catalog',
        schemaName: 'schema',
        functionName: 'avg',
        maxRows: 10,
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getFunctions({
        catalogName: 'catalog',
        schemaName: 'schema',
        functionName: 'avg',
        maxRows: null,
      });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getPrimaryKeys', () => {
    it('should run operation', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getPrimaryKeys({
        catalogName: 'catalog',
        schemaName: 'schema',
        tableName: 't1',
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getPrimaryKeys({
        catalogName: 'catalog',
        schemaName: 'schema',
        tableName: 't1',
        maxRows: 10,
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getPrimaryKeys({
        catalogName: 'catalog',
        schemaName: 'schema',
        tableName: 't1',
        maxRows: null,
      });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getCrossReference', () => {
    it('should run operation', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getCrossReference({
        parentCatalogName: 'parentCatalogName',
        parentSchemaName: 'parentSchemaName',
        parentTableName: 'parentTableName',
        foreignCatalogName: 'foreignCatalogName',
        foreignSchemaName: 'foreignSchemaName',
        foreignTableName: 'foreignTableName',
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getCrossReference({
        parentCatalogName: 'parentCatalogName',
        parentSchemaName: 'parentSchemaName',
        parentTableName: 'parentTableName',
        foreignCatalogName: 'foreignCatalogName',
        foreignSchemaName: 'foreignSchemaName',
        foreignTableName: 'foreignTableName',
        maxRows: 10,
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getCrossReference({
        parentCatalogName: 'parentCatalogName',
        parentSchemaName: 'parentSchemaName',
        parentTableName: 'parentTableName',
        foreignCatalogName: 'foreignCatalogName',
        foreignSchemaName: 'foreignSchemaName',
        foreignTableName: 'foreignTableName',
        maxRows: null,
      });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('close', () => {
    it('should run operation', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);

      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context });
      expect(session.inspectInternals().isOpen).to.be.true;

      const result = await session.close();
      expect(result).instanceOf(Status);
      expect(session.inspectInternals().isOpen).to.be.false;
      expect(driver.closeSession.callCount).to.eq(1);
    });

    it('should not run operation twice', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);

      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context });
      expect(session.inspectInternals().isOpen).to.be.true;

      const result = await session.close();
      expect(result).instanceOf(Status);
      expect(session.inspectInternals().isOpen).to.be.false;
      expect(driver.closeSession.callCount).to.eq(1);

      const result2 = await session.close();
      expect(result2).instanceOf(Status);
      expect(session.inspectInternals().isOpen).to.be.false;
      expect(driver.closeSession.callCount).to.eq(1); // second time it should not be called
    });

    it('should close operations that belong to it', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const operation = await session.executeStatement('SELECT * FROM table');
      if (!(operation instanceof DBSQLOperation)) {
        expect.fail('Assertion error: operation is not a DBSQLOperation');
      }

      expect(operation.onClose).to.be.not.undefined;
      // @ts-expect-error TS2445: Property closed is protected and only accessible within class DBSQLOperation and its subclasses
      expect(operation.closed).to.be.false;
      // @ts-expect-error TS2445: Property items is protected and only accessible within class CloseableCollection and its subclasses
      expect(session.inspectInternals().operations.items.size).to.eq(1);

      sinon.spy(session.inspectInternals().operations, 'closeAll');
      sinon.spy(operation, 'close');

      await session.close();
      expect((operation.close as SinonSpy).called).to.be.true;
      expect((session.inspectInternals().operations.closeAll as SinonSpy).called).to.be.true;
      expect(operation.onClose).to.be.undefined;
      // @ts-expect-error TS2445: Property closed is protected and only accessible within class DBSQLOperation and its subclasses
      expect(operation.closed).to.be.true;
      // @ts-expect-error TS2445: Property items is protected and only accessible within class CloseableCollection and its subclasses
      expect(session.inspectInternals().operations.items.size).to.eq(0);
    });

    it('should reject all methods once closed', async () => {
      const session = new DBSQLSessionTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      await session.close();
      expect(session.inspectInternals().isOpen).to.be.false;

      await expectFailure(() => session.getInfo(1));
      await expectFailure(() => session.executeStatement('SELECT * FROM table'));
      await expectFailure(() => session.getTypeInfo());
      await expectFailure(() => session.getCatalogs());
      await expectFailure(() => session.getSchemas());
      await expectFailure(() => session.getTables());
      await expectFailure(() => session.getTableTypes());
      await expectFailure(() => session.getColumns());
      await expectFailure(() =>
        session.getFunctions({
          functionName: 'func',
        }),
      );
      await expectFailure(() =>
        session.getPrimaryKeys({
          schemaName: 'schema',
          tableName: 'table',
        }),
      );
      await expectFailure(() =>
        session.getCrossReference({
          parentCatalogName: 'parent_catalog',
          parentSchemaName: 'parent_schema',
          parentTableName: 'parent_table',
          foreignCatalogName: 'foreign_catalog',
          foreignSchemaName: 'foreign_schema',
          foreignTableName: 'foreign_table',
        }),
      );
    });
  });
});
