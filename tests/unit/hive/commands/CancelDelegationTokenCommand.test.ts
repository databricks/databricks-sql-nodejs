import { expect } from 'chai';
import sinon from 'sinon';
import CancelDelegationTokenCommand from '../../../../lib/hive/Commands/CancelDelegationTokenCommand';
import { TCancelDelegationTokenReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('CancelDelegationTokenCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new CancelDelegationTokenCommand(thriftClient, new ClientContextStub());

    const request: TCancelDelegationTokenReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
      delegationToken: 'token',
    };

    const response = await command.execute(request);
    expect(thriftClient.CancelDelegationToken.called).to.be.true;
    expect(thriftClient.cancelDelegationTokenReq).to.deep.equal(new TCancelDelegationTokenReq(request));
    expect(response).to.be.deep.eq(thriftClient.cancelDelegationTokenResp);
  });
});
