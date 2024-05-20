import { expect } from 'chai';
import sinon from 'sinon';
import GetCrossReferenceCommand from '../../../../lib/hive/Commands/GetCrossReferenceCommand';
import { TGetCrossReferenceReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('GetCrossReferenceCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new GetCrossReferenceCommand(thriftClient, new ClientContextStub());

    const request: TGetCrossReferenceReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
      parentCatalogName: 'parentCatalogName',
      parentSchemaName: 'parentSchemaName',
      parentTableName: 'parentTableName',
      foreignCatalogName: 'foreignCatalogName',
      foreignSchemaName: 'foreignSchemaName',
      foreignTableName: 'foreignTableName',
    };

    const response = await command.execute(request);
    expect(thriftClient.GetCrossReference.called).to.be.true;
    expect(thriftClient.getCrossReferenceReq).to.deep.equal(new TGetCrossReferenceReq(request));
    expect(response).to.be.deep.eq(thriftClient.getCrossReferenceResp);
  });
});
