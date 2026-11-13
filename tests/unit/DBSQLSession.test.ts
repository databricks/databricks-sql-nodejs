import { AssertionError, expect } from 'chai';
import sinon, { SinonSpy } from 'sinon';
import Int64 from 'node-int64';
import DBSQLSession, { numberToInt64 } from '../../lib/DBSQLSession';
import InfoValue from '../../lib/dto/InfoValue';
import Status from '../../lib/dto/Status';
import DBSQLOperation from '../../lib/DBSQLOperation';
import { PrimaryKeysRequest } from '../../lib/contracts/IDBSQLSession';
import ISessionBackend from '../../lib/contracts/ISessionBackend';
import ParameterError from '../../lib/errors/ParameterError';
import { DBSQLParameter } from '../../lib/DBSQLParameter';
import { TSessionHandle, TProtocolVersion, TOperationState, TStatusCode } from '../../thrift/TCLIService_types';
import ClientContextStub from './.stubs/ClientContextStub';
import { createSessionForTest } from './.stubs/createSessionForTest';

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
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getInfo(1);
      expect(result).instanceOf(InfoValue);
    });
  });

  describe('executeStatement', () => {
    it('should execute statement', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.executeStatement('SELECT * FROM table');
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.executeStatement('SELECT * FROM table', { maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.executeStatement('SELECT * FROM table', { maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });

    describe('Arrow support', () => {
      it('should not use Arrow if disabled in options', async () => {
        const session = createSessionForTest({
          handle: sessionHandleStub,
          context: new ClientContextStub({ arrowEnabled: false }),
        });
        const result = await session.executeStatement('SELECT * FROM table');
        expect(result).instanceOf(DBSQLOperation);
      });

      it('should apply defaults for Arrow options', async () => {
        // case 1
        {
          const session = createSessionForTest({
            handle: sessionHandleStub,
            context: new ClientContextStub({ arrowEnabled: true }),
          });
          const result = await session.executeStatement('SELECT * FROM table');
          expect(result).instanceOf(DBSQLOperation);
        }

        // case 2
        {
          const session = createSessionForTest({
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

          const session = createSessionForTest({
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
        const session = createSessionForTest({
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
        const session = createSessionForTest({
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
        const session = createSessionForTest({
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

  // https://github.com/databricks/databricks-sql-nodejs/issues/261
  describe('executeStatement with non-scalar parameter values', () => {
    const invalidCases: Array<{ desc: string; options: object; pattern: RegExp }> = [
      { desc: 'ordinal array-of-arrays', options: { ordinalParameters: [[1], [2], [3]] }, pattern: /array/ },
      { desc: 'named array value', options: { namedParameters: { p: [1, 2] } }, pattern: /array/ },
      { desc: 'ordinal plain object', options: { ordinalParameters: [{ a: 1 }] }, pattern: /object/ },
      { desc: 'named plain object', options: { namedParameters: { p: { a: 1 } } }, pattern: /object/ },
      {
        desc: 'ordinal array wrapped in DBSQLParameter',
        options: { ordinalParameters: [new DBSQLParameter({ value: [1, 2] as never })] },
        pattern: /array/,
      },
      // eslint-disable-next-line no-new-wrappers
      { desc: 'ordinal boxed String', options: { ordinalParameters: [new String('x')] }, pattern: /object/ },
      // eslint-disable-next-line no-new-wrappers
      { desc: 'named boxed Number', options: { namedParameters: { p: new Number(1) } }, pattern: /object/ },
    ];

    for (const { desc, options, pattern } of invalidCases) {
      it(`should throw ParameterError and not call the driver for ${desc}`, async () => {
        const context = new ClientContextStub();
        const driver = sinon.spy(context.driver);
        const session = createSessionForTest({
          handle: sessionHandleStub,
          context,
          serverProtocolVersion: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8,
        });

        try {
          await session.executeStatement('SELECT ?', options as never);
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(error).instanceOf(ParameterError);
          expect((error as Error).message).to.match(pattern);
        }
        expect(driver.executeStatement.called).to.be.false;
      });
    }

    it('should keep accepting valid scalar values', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const session = createSessionForTest({
        handle: sessionHandleStub,
        context,
        serverProtocolVersion: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8,
      });

      await session.executeStatement('SELECT ?, ?, ?, ?, ?', {
        ordinalParameters: [new Date('2023-09-06T03:14:27.843Z'), new Int64(1234), BigInt(1234), null, 'x'],
      });
      expect(driver.executeStatement.callCount).to.eq(1);
      expect(driver.executeStatement.firstCall.args[0].parameters?.length).to.eq(5);
    });
  });

  describe('executeStatement with queryTags', () => {
    it('should set confOverlay with query_tags when queryTags are provided', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const session = createSessionForTest({ handle: sessionHandleStub, context });

      await session.executeStatement('SELECT 1', { queryTags: { team: 'eng', app: 'etl' } });

      expect(driver.executeStatement.callCount).to.eq(1);
      const req = driver.executeStatement.firstCall.args[0];
      expect(req.confOverlay).to.deep.include({ query_tags: 'team:eng,app:etl' });
    });

    it('should not set confOverlay query_tags when queryTags is not provided', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const session = createSessionForTest({ handle: sessionHandleStub, context });

      await session.executeStatement('SELECT 1');

      expect(driver.executeStatement.callCount).to.eq(1);
      const req = driver.executeStatement.firstCall.args[0];
      expect(req.confOverlay?.query_tags).to.be.undefined;
    });

    it('should not set confOverlay query_tags when queryTags is empty', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const session = createSessionForTest({ handle: sessionHandleStub, context });

      await session.executeStatement('SELECT 1', { queryTags: {} });

      expect(driver.executeStatement.callCount).to.eq(1);
      const req = driver.executeStatement.firstCall.args[0];
      expect(req.confOverlay?.query_tags).to.be.undefined;
    });
  });

  describe('getTypeInfo', () => {
    it('should run operation', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTypeInfo();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTypeInfo({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTypeInfo({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getCatalogs', () => {
    it('should run operation', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getCatalogs();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getCatalogs({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getCatalogs({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getSchemas', () => {
    it('should run operation', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getSchemas();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use filters', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getSchemas({
        catalogName: 'catalog',
        schemaName: 'schema',
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getSchemas({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getSchemas({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getTables', () => {
    it('should run operation', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTables();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use filters', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTables({
        catalogName: 'catalog',
        schemaName: 'default',
        tableName: 't1',
        tableTypes: ['external'],
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTables({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTables({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getTableTypes', () => {
    it('should run operation', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTableTypes();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTableTypes({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getTableTypes({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getColumns', () => {
    it('should run operation', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getColumns();
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use filters', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getColumns({
        catalogName: 'catalog',
        schemaName: 'schema',
        tableName: 'table',
        columnName: 'column',
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getColumns({ maxRows: 10 });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getColumns({ maxRows: null });
      expect(result).instanceOf(DBSQLOperation);
    });
  });

  describe('getFunctions', () => {
    it('rejects a missing or non-string functionName before calling the backend', async () => {
      const getFunctions = sinon.stub();
      const session = new DBSQLSession({
        backend: { id: 'test', getFunctions } as unknown as ISessionBackend,
        context: new ClientContextStub(),
      });
      const invalidRequests = [undefined, null, {}, { functionName: null }, { functionName: 123 }];

      await Promise.all(
        invalidRequests.map(async (request) => {
          try {
            await session.getFunctions(request as unknown as Parameters<DBSQLSession['getFunctions']>[0]);
            expect.fail('Expected getFunctions to reject an invalid functionName');
          } catch (error) {
            expect(error).instanceOf(ParameterError);
            expect((error as Error).message).equal('functionName must be a string');
          }
        }),
      );
      expect(getFunctions.called).false;
    });

    it('should run operation', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getFunctions({
        catalogName: 'catalog',
        schemaName: 'schema',
        functionName: 'avg',
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('accepts an empty functionName string', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getFunctions({ functionName: '' });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getFunctions({
        catalogName: 'catalog',
        schemaName: 'schema',
        functionName: 'avg',
        maxRows: 10,
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
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
    it('should forward null, omitted, and empty namespace names unchanged for Thrift', async () => {
      for (const catalogName of [undefined, null, '', 'catalog']) {
        for (const schemaName of [undefined, null, '', 'schema']) {
          const context = new ClientContextStub();
          const session = createSessionForTest({ handle: sessionHandleStub, context });
          // eslint-disable-next-line no-await-in-loop
          const result = await session.getPrimaryKeys({
            catalogName,
            schemaName,
            tableName: 't1',
          } as PrimaryKeysRequest);
          expect(result).instanceOf(DBSQLOperation);
          expect(context.driver.getPrimaryKeysReq?.catalogName).to.equal(catalogName);
          expect(context.driver.getPrimaryKeysReq?.schemaName).to.equal(schemaName);
          expect(context.driver.getPrimaryKeysReq?.tableName).to.equal('t1');
        }
      }
    });

    it('should run operation', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getPrimaryKeys({
        catalogName: 'catalog',
        schemaName: 'schema',
        tableName: 't1',
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should use direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
      const result = await session.getPrimaryKeys({
        catalogName: 'catalog',
        schemaName: 'schema',
        tableName: 't1',
        maxRows: 10,
      });
      expect(result).instanceOf(DBSQLOperation);
    });

    it('should disable direct results', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
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
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
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
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
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
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
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

      const session = createSessionForTest({ handle: sessionHandleStub, context });
      expect(session['isOpen']).to.be.true;

      const result = await session.close();
      expect(result).instanceOf(Status);
      expect(session['isOpen']).to.be.false;
      expect(driver.closeSession.callCount).to.eq(1);
    });

    it('should not run operation twice', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);

      const session = createSessionForTest({ handle: sessionHandleStub, context });
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

    it('should send one close request for concurrent close calls', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const session = createSessionForTest({ handle: sessionHandleStub, context });

      const results = await Promise.all([session.close(), session.close(), session.close()]);
      results.forEach((result) => expect(result).instanceOf(Status));
      expect(session['isOpen']).to.be.false;
      expect(driver.closeSession.callCount).to.eq(1);
    });

    it('should reject every concurrent close call when the close request fails', async () => {
      const context = new ClientContextStub();
      const error = new Error('close failed');
      const closeSession = sinon.stub(context.driver, 'closeSession').rejects(error);
      const session = createSessionForTest({ handle: sessionHandleStub, context });

      const results = await Promise.allSettled([session.close(), session.close()]);
      expect(results).to.deep.equal([
        { status: 'rejected', reason: error },
        { status: 'rejected', reason: error },
      ]);
      expect(closeSession.callCount).to.eq(1);
      expect((await session.close()).isSuccess).to.be.true;
    });

    it('should retry close after closing its operations failed', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const session = createSessionForTest({ handle: sessionHandleStub, context });
      const error = new Error('operation close failed');
      const closeAll = sinon.stub(session['operations'], 'closeAll');
      closeAll.onFirstCall().rejects(error);
      closeAll.callThrough();

      try {
        await session.close();
        expect.fail('It should throw an error');
      } catch (e) {
        expect(e).to.equal(error);
      }
      expect(session['isOpen']).to.be.true;
      expect(driver.closeSession.callCount).to.eq(0);

      expect(await session.close()).instanceOf(Status);
      expect(session['isOpen']).to.be.false;
      expect(driver.closeSession.callCount).to.eq(1);
    });

    it('should close operations that belong to it', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
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

    it('should close the session when a finished operation fails to close', async () => {
      const context = new ClientContextStub();
      const closeSession = sinon.spy(context.driver, 'closeSession');
      sinon
        .stub(context.driver, 'closeOperation')
        .rejects(new Error('Received a response with a bad HTTP status: 404'));
      const session = createSessionForTest({ handle: sessionHandleStub, context });
      const operation = await session.executeStatement('SELECT * FROM table');
      await operation.finished();

      await session.close();

      expect(closeSession.calledOnce).to.be.true;
      expect(session['isOpen']).to.be.false;
      expect(session['operations']['items'].size).to.eq(0);
    });

    describe('right after execution', () => {
      function sessionWithFailingOperationClose(operationState: TOperationState) {
        const context = new ClientContextStub();
        const closeSession = sinon.spy(context.driver, 'closeSession');
        const getOperationStatus = sinon.spy(context.driver, 'getOperationStatus');
        sinon
          .stub(context.driver, 'closeOperation')
          .rejects(new Error('Received a response with a bad HTTP status: 404'));
        context.driver.executeStatementResp = {
          ...context.driver.executeStatementResp,
          directResults: {
            operationStatus: { status: { statusCode: TStatusCode.SUCCESS_STATUS }, operationState },
          },
        };
        const session = createSessionForTest({ handle: sessionHandleStub, context });
        return { session, closeSession, getOperationStatus };
      }

      it('should close the operation and session when direct results reported completion', async () => {
        const { session, closeSession, getOperationStatus } = sessionWithFailingOperationClose(
          TOperationState.FINISHED_STATE,
        );
        const operation = await session.executeStatement('SELECT * FROM table');

        const status = await operation.close();
        expect(status.isSuccess).to.be.true;
        expect(session['operations']['items'].size).to.eq(0);

        await session.close();
        expect(getOperationStatus.called).to.be.false;
        expect(closeSession.calledOnce).to.be.true;
        expect(session['isOpen']).to.be.false;
      });

      it('should close the session when direct results reported completion', async () => {
        const { session, closeSession, getOperationStatus } = sessionWithFailingOperationClose(
          TOperationState.FINISHED_STATE,
        );
        await session.executeStatement('SELECT * FROM table');

        await session.close();

        expect(getOperationStatus.called).to.be.false;
        expect(closeSession.calledOnce).to.be.true;
        expect(session['isOpen']).to.be.false;
        expect(session['operations']['items'].size).to.eq(0);
      });

      it('should still throw from session close when the operation may still be running', async () => {
        const { session, closeSession } = sessionWithFailingOperationClose(TOperationState.RUNNING_STATE);
        await session.executeStatement('SELECT * FROM table');

        await expectFailure(() => session.close());

        expect(closeSession.called).to.be.false;
        expect(session['isOpen']).to.be.true;
        expect(session['operations']['items'].size).to.eq(1);
      });
    });

    it('should reject all methods once closed', async () => {
      const session = createSessionForTest({ handle: sessionHandleStub, context: new ClientContextStub() });
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

  describe('Symbol.asyncDispose', () => {
    it('should close the session and its operations', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const session = createSessionForTest({ handle: sessionHandleStub, context });
      const operation = await session.executeStatement('SELECT * FROM table');
      if (!(operation instanceof DBSQLOperation)) {
        expect.fail('Assertion error: operation is not a DBSQLOperation');
      }

      const result = await session[Symbol.asyncDispose]();
      expect(result).to.be.undefined;
      expect(session['isOpen']).to.be.false;
      expect(operation['closed']).to.be.true;
      expect(driver.closeSession.callCount).to.eq(1);
    });

    it('should be a no-op for an already closed session', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const session = createSessionForTest({ handle: sessionHandleStub, context });

      await session.close();
      await session[Symbol.asyncDispose]();
      await session[Symbol.asyncDispose]();
      expect(driver.closeSession.callCount).to.eq(1);
    });

    it('should propagate close errors', async () => {
      const context = new ClientContextStub();
      const session = createSessionForTest({ handle: sessionHandleStub, context });
      const error = new Error('close failed');
      sinon.stub(context.driver, 'closeSession').rejects(error);

      try {
        await session[Symbol.asyncDispose]();
        expect.fail('It should throw an error');
      } catch (e) {
        expect(e).to.equal(error);
      }
      // A failed close still marks the session closed, so disposing again is a no-op.
      await session[Symbol.asyncDispose]();
    });
  });
});
