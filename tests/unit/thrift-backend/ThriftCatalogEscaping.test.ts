import { expect } from 'chai';
import { TSessionHandle } from '../../../thrift/TCLIService_types';
import ClientContextStub from '../.stubs/ClientContextStub';
import { createSessionForTest } from '../.stubs/createSessionForTest';

const sessionHandle: TSessionHandle = {
  sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
};

describe('Thrift metadata catalog escaping', () => {
  for (const enabled of [undefined, false, true]) {
    for (const [catalogName, escaped] of [
      [undefined, undefined],
      ['', ''],
      ['main', 'main'],
      ['comparator_tests', 'comparator\\_tests'],
      ['_a__b_', '\\_a\\_\\_b\\_'],
      ['comparator\\_tests', 'comparator\\_tests'],
      ['a\\_b_c', 'a\\_b\\_c'],
      [String.raw`my\\_cat`, String.raw`my\\_cat`],
      ['a_b%c', 'a\\_b%c'],
      ['%', '%'],
    ]) {
      it(`handles ${JSON.stringify(catalogName)} with escapeMetadataCatalogName=${enabled}`, async () => {
        const context = new ClientContextStub({ escapeMetadataCatalogName: enabled });
        const session = createSessionForTest({ handle: sessionHandle, context });
        const request = Object.freeze({
          catalogName,
          schemaName: 'schema_%',
          tableName: 'table_%',
          columnName: 'column_%',
          functionName: 'function_%',
        });

        for (const [method, requestField] of [
          ['getSchemas', 'getSchemasReq'],
          ['getTables', 'getTablesReq'],
          ['getColumns', 'getColumnsReq'],
          ['getFunctions', 'getFunctionsReq'],
        ] as const) {
          const operation = await session[method](request);
          expect(context.driver[requestField], method).to.include({
            catalogName: enabled ? escaped : catalogName,
            schemaName: request.schemaName,
          });
          await operation.close();
        }
        expect(context.driver.getTablesReq?.tableName).to.equal(request.tableName);
        expect(context.driver.getColumnsReq?.columnName).to.equal(request.columnName);
        expect(context.driver.getFunctionsReq?.functionName).to.equal(request.functionName);
        await session.close();
      });
    }
  }

  it('preserves literal catalog identifiers for key metadata', async () => {
    const context = new ClientContextStub({ escapeMetadataCatalogName: true });
    const session = createSessionForTest({ handle: sessionHandle, context });
    await session.getPrimaryKeys({ catalogName: 'parent_catalog', schemaName: 's', tableName: 't' });
    await session.getCrossReference({
      parentCatalogName: 'parent_catalog',
      parentSchemaName: 's',
      parentTableName: 't',
      foreignCatalogName: 'foreign_catalog',
      foreignSchemaName: 's',
      foreignTableName: 'f',
    });

    expect(context.driver.getPrimaryKeysReq?.catalogName).to.equal('parent_catalog');
    expect(context.driver.getCrossReferenceReq?.parentCatalogName).to.equal('parent_catalog');
    expect(context.driver.getCrossReferenceReq?.foreignCatalogName).to.equal('foreign_catalog');
    await session.close();
  });
});
