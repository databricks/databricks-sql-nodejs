import { AssertionError, expect } from 'chai';
import sinon, { SinonSpy } from 'sinon';
import Int64 from 'node-int64';
import DBSQLSession, { numberToInt64 } from '../../lib/DBSQLSession';
import InfoValue from '../../lib/dto/InfoValue';
import Status from '../../lib/dto/Status';
import DBSQLOperation from '../../lib/DBSQLOperation';
import { TSessionHandle, TProtocolVersion } from '../../thrift/TCLIService_types';
import ClientContextStub from './.stubs/ClientContextStub';

const sessionHandleStub: TSessionHandle = {
  sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
};

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
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getInfo(1);
      expect(result).instanceOf(InfoValue);
    });
  });

  describe('executeStatement', () => {
    it('should execute statement', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.executeStatement('SELECT * FROM table');
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.executeStatement('SELECT * FROM table', { maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.executeStatement('SELECT * FROM table', { maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });

    describe('Arrow support', () => {
      it('should not use Arrow if disabled in options', async () => {
        const session = new DBSQLSession({
          handle: sessionHandleStub,
          context: new ClientContextStub({ arrowEnabled: false }),
        });
        const result = await session.executeStatement('SELECT * FROM table');
        expect(result).instanceOf(DBSQLOperation);
      });

      it('should apply defaults for Arrow options', async () => {
        // case 1
        {
          const session = new DBSQLSession({
            handle: sessionHandleStub,
            context: new ClientContextStub({ arrowEnabled: true }),
          });
          const result = await session.executeStatement('SELECT * FROM table');
          expect(result).instanceOf(DBSQLOperation);
        }

        // case 2
        {
          const session = new DBSQLSession({
            handle: sessionHandleStub,
            context: new ClientContextStub({ arrowEnabled: true, useArrowNativeTypes: false }),
          });
          const result = await session.executeStatement('SELECT * FROM table');
          expect(result).instanceOf(DBSQLOperation);
        }
      });
    });

    describe('executeStatement with different protocol versions', () => {
      const protocolVersions = [
        { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1, desc: 'V1: no special features' },
        { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V2, desc: 'V2: no special features' },
        { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V3, desc: 'V3: cloud fetch' },
        { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4, desc: 'V4: multiple catalogs' },
        { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5, desc: 'V5: arrow metadata' },
        { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6, desc: 'V6: async metadata, arrow compression' },
        { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7, desc: 'V7: result persistence mode' },
        { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8, desc: 'V8: parameterized queries' },
      ];

      for (const { version, desc } of protocolVersions) {
        it(`should properly format request with protocol version ${desc}`, async () => {
          const context = new ClientContextStub();
          const driver = sinon.spy(context.driver);
          const statement = 'SELECT * FROM table';
          const options = {
            maxRows: 10,
            queryTimeout: 100,
            namedParameters: { param1: 'value1' },
            useCloudFetch: true,
            useLZ4Compression: true,
          };

          const session = new DBSQLSession({
            handle: sessionHandleStub,
            context,
            serverProtocolVersion: version,
          });

          await session.executeStatement(statement, options);

          expect(driver.executeStatement.callCount).to.eq(1);
          const req = driver.executeStatement.firstCall.args[0];

          // Basic fields that should always be present
          expect(req.sessionHandle.sessionId.guid).to.deep.equal(sessionHandleStub.sessionId.guid);
          expect(req.sessionHandle.sessionId.secret).to.deep.equal(sessionHandleStub.sessionId.secret);
          expect(req.statement).to.equal(statement);
          expect(req.runAsync).to.be.true;
          expect(req.queryTimeout).to.deep.equal(numberToInt64(options.queryTimeout));

          // Fields that depend on protocol version
          if (version >= TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8) {
            expect(req.parameters).to.exist;
            expect(req.parameters?.length).to.equal(1);
          } else {
            expect(req.parameters).to.not.exist;
          }

          if (version >= TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6) {
            // Since cloud fetch is enabled, canDecompressLZ4Result should not be set
            if (req.canDownloadResult === true) {
              expect(req.canDecompressLZ4Result).to.not.be.true;
            } else {
              expect(req.canDecompressLZ4Result).to.be.true;
            }
          } else {
            expect(req.canDecompressLZ4Result).to.not.be.true;
          }

          if (version >= TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5) {
            expect(req.canReadArrowResult).to.be.true;
            expect(req.useArrowNativeTypes).to.not.be.undefined;
          } else if (version >= TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V3) {
            // V3 and V4 have canDownloadResult but not arrow-related fields
            expect(req.canReadArrowResult).to.be.false;
            expect(req.useArrowNativeTypes).to.not.exist;
            expect(req.canDownloadResult).to.be.true;
          } else {
            // V1 and V2 don't have arrow or download features
            expect(req.canReadArrowResult).to.be.false;
            expect(req.useArrowNativeTypes).to.not.exist;
            expect(req.canDownloadResult).to.not.exist;
          }
        });
      }
    });

    describe('LZ4 compression with cloud fetch', () => {
      it('should not set canDecompressLZ4Result when cloud fetch is enabled (canDownloadResult=true)', async () => {
        const context = new ClientContextStub({ useLZ4Compression: true });
        const driver = sinon.spy(context.driver);
        const statement = 'SELECT * FROM table';

        // Use V6+ which supports arrow compression
        const session = new DBSQLSession({
          handle: sessionHandleStub,
          context,
          serverProtocolVersion: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6,
        });

        // Execute with cloud fetch enabled
        await session.executeStatement(statement, { useCloudFetch: true });

        expect(driver.executeStatement.callCount).to.eq(1);
        const req = driver.executeStatement.firstCall.args[0];

        // canDownloadResult should be true and canDecompressLZ4Result should NOT be set
        expect(req.canDownloadResult).to.be.true;
        expect(req.canDecompressLZ4Result).to.not.be.true;
      });

      it('should set canDecompressLZ4Result when cloud fetch is disabled (canDownloadResult=false)', async () => {
        const context = new ClientContextStub({ useLZ4Compression: true });
        const driver = sinon.spy(context.driver);
        const statement = 'SELECT * FROM table';

        // Use V6+ which supports arrow compression
        const session = new DBSQLSession({
          handle: sessionHandleStub,
          context,
          serverProtocolVersion: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6,
        });

        // Execute with cloud fetch disabled
        await session.executeStatement(statement, { useCloudFetch: false });

        expect(driver.executeStatement.callCount).to.eq(1);
        const req = driver.executeStatement.firstCall.args[0];

        // canDownloadResult should be false and canDecompressLZ4Result should be set
        expect(req.canDownloadResult).to.be.false;
        expect(req.canDecompressLZ4Result).to.be.true;
      });

      it('should not set canDecompressLZ4Result when server protocol does not support Arrow compression', async () => {
        const context = new ClientContextStub({ useLZ4Compression: true });
        const driver = sinon.spy(context.driver);
        const statement = 'SELECT * FROM table';

        // Use V5 which does not support arrow compression
        const session = new DBSQLSession({
          handle: sessionHandleStub,
          context,
          serverProtocolVersion: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5,
        });

        // Execute with cloud fetch disabled
        await session.executeStatement(statement, { useCloudFetch: false });

        expect(driver.executeStatement.callCount).to.eq(1);
        const req = driver.executeStatement.firstCall.args[0];

        // canDecompressLZ4Result should NOT be set regardless of cloud fetch setting
        expect(req.canDecompressLZ4Result).to.not.be.true;
      });
    });
  });

  describe('getTypeInfo', () => {
    it('should run operation', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTypeInfo();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTypeInfo({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTypeInfo({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getCatalogs', () => {
    it('should run operation', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getCatalogs();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getCatalogs({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getCatalogs({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getSchemas', () => {
    it('should run operation', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getSchemas();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use filters', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getSchemas({
        catalogName: 'catalog',
        schemaName: 'schema',
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getSchemas({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getSchemas({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getTables', () => {
    it('should run operation', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTables();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use filters', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTables({
        catalogName: 'catalog',
        schemaName: 'default',
        tableName: 't1',
        tableTypes: ['external'],
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTables({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTables({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getTableTypes', () => {
    it('should run operation', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTableTypes();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTableTypes({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTableTypes({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getColumns', () => {
    it('should run operation', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getColumns();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use filters', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getColumns({
        catalogName: 'catalog',
        schemaName: 'schema',
        tableName: 'table',
        columnName: 'column',
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getColumns({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getColumns({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getFunctions', () => {
    it('should run operation', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getFunctions({
        catalogName: 'catalog',
        schemaName: 'schema',
        functionName: 'avg',
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getFunctions({
        catalogName: 'catalog',
        schemaName: 'schema',
        functionName: 'avg',
        maxRows: 10,
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
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
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getPrimaryKeys({
        catalogName: 'catalog',
        schemaName: 'schema',
        tableName: 't1',
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getPrimaryKeys({
        catalogName: 'catalog',
        schemaName: 'schema',
        tableName: 't1',
        maxRows: 10,
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
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
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
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
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
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
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
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

      const session = new DBSQLSession({ handle: sessionHandleStub, context });
      expect(session['isOpen']).to.be.true;

      const result = await session.close();
      expect(result).instanceOf(Status);
      expect(session['isOpen']).to.be.false;
      expect(driver.closeSession.callCount).to.eq(1);
    });

    it('should not run operation twice', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);

      const session = new DBSQLSession({ handle: sessionHandleStub, context });
      expect(session['isOpen']).to.be.true;

      const result = await session.close();
      expect(result).instanceOf(Status);
      expect(session['isOpen']).to.be.false;
      expect(driver.closeSession.callCount).to.eq(1);

      const result2 = await session.close();
      expect(result2).instanceOf(Status);
      expect(session['isOpen']).to.be.false;
      expect(driver.closeSession.callCount).to.eq(1); // second time it should not be called
    });

    it('should close operations that belong to it', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      const operation = await session.executeStatement('SELECT * FROM table');
      if (!(operation instanceof DBSQLOperation)) {
        expect.fail('Assertion error: operation is not a DBSQLOperation');
      }

      expect(operation.onClose).to.be.not.undefined;
      expect(operation['closed']).to.be.false;
      expect(session['operations']['items'].size).to.eq(1);

      sinon.spy(session['operations'], 'closeAll');
      sinon.spy(operation, 'close');

      await session.close();
      expect((operation.close as SinonSpy).called).to.be.true;
      expect((session['operations'].closeAll as SinonSpy).called).to.be.true;
      expect(operation.onClose).to.be.undefined;
      expect(operation['closed']).to.be.true;
      expect(session['operations']['items'].size).to.eq(0);
    });

    it('should reject all methods once closed', async () => {
      const session = new DBSQLSession({ handle: sessionHandleStub, context: new ClientContextStub() });
      await session.close();
      expect(session['isOpen']).to.be.false;

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
