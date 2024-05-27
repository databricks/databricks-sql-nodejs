import { expect } from 'chai';
import sinon from 'sinon';
import GetTableTypesCommand from '../../../../lib/hive/Commands/GetTableTypesCommand';
import { TGetTableTypesReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('GetTableTypesCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new GetTableTypesCommand(thriftClient, new ClientContextStub());

    const request: TGetTableTypesReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
    };

    const response = await command.execute(request);
    expect(thriftClient.GetTableTypes.called).to.be.true;
    expect(thriftClient.getTableTypesReq).to.deep.equal(new TGetTableTypesReq(request));
    expect(response).to.be.deep.eq(thriftClient.getTableTypesResp);
  });
});
