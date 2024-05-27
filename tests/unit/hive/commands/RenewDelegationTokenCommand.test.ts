import { expect } from 'chai';
import sinon from 'sinon';
import RenewDelegationTokenCommand from '../../../../lib/hive/Commands/RenewDelegationTokenCommand';
import { TRenewDelegationTokenReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('RenewDelegationTokenCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new RenewDelegationTokenCommand(thriftClient, new ClientContextStub());

    const request: TRenewDelegationTokenReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
      delegationToken: 'token',
    };

    const response = await command.execute(request);
    expect(thriftClient.RenewDelegationToken.called).to.be.true;
    expect(thriftClient.renewDelegationTokenReq).to.deep.equal(new TRenewDelegationTokenReq(request));
    expect(response).to.be.deep.eq(thriftClient.renewDelegationTokenResp);
  });
});
