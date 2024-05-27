import { expect } from 'chai';
import sinon from 'sinon';
import CloseSessionCommand from '../../../../lib/hive/Commands/CloseSessionCommand';
import { TCloseSessionReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('CloseSessionCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new CloseSessionCommand(thriftClient, new ClientContextStub());

    const request: TCloseSessionReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
    };

    const response = await command.execute(request);
    expect(thriftClient.CloseSession.called).to.be.true;
    expect(thriftClient.closeSessionReq).to.deep.equal(new TCloseSessionReq(request));
    expect(response).to.be.deep.eq(thriftClient.closeSessionResp);
  });
});
