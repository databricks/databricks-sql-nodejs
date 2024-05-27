import { expect } from 'chai';
import sinon from 'sinon';
import GetSchemasCommand from '../../../../lib/hive/Commands/GetSchemasCommand';
import { TGetSchemasReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('GetSchemasCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new GetSchemasCommand(thriftClient, new ClientContextStub());

    const request: TGetSchemasReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
      catalogName: 'catalog',
      schemaName: 'schema',
    };

    const response = await command.execute(request);
    expect(thriftClient.GetSchemas.called).to.be.true;
    expect(thriftClient.getSchemasReq).to.deep.equal(new TGetSchemasReq(request));
    expect(response).to.be.deep.eq(thriftClient.getSchemasResp);
  });
});
