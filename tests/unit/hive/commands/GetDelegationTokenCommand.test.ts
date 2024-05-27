import { expect } from 'chai';
import sinon from 'sinon';
import GetDelegationTokenCommand from '../../../../lib/hive/Commands/GetDelegationTokenCommand';
import { TGetDelegationTokenReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('GetDelegationTokenCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new GetDelegationTokenCommand(thriftClient, new ClientContextStub());

    const request: TGetDelegationTokenReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
      owner: 'user1',
      renewer: 'user2',
    };

    const response = await command.execute(request);
    expect(thriftClient.GetDelegationToken.called).to.be.true;
    expect(thriftClient.getDelegationTokenReq).to.deep.equal(new TGetDelegationTokenReq(request));
    expect(response).to.be.deep.eq(thriftClient.getDelegationTokenResp);
  });
});
