import { expect } from 'chai';
import sinon from 'sinon';
import GetTypeInfoCommand from '../../../../lib/hive/Commands/GetTypeInfoCommand';
import { TGetTypeInfoReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('GetTypeInfoCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new GetTypeInfoCommand(thriftClient, new ClientContextStub());

    const request: TGetTypeInfoReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
    };

    const response = await command.execute(request);
    expect(thriftClient.GetTypeInfo.called).to.be.true;
    expect(thriftClient.getTypeInfoReq).to.deep.equal(new TGetTypeInfoReq(request));
    expect(response).to.be.deep.eq(thriftClient.getTypeInfoResp);
  });
});
