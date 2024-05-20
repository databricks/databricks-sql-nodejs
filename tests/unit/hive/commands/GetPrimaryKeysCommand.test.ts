import { expect } from 'chai';
import sinon from 'sinon';
import GetPrimaryKeysCommand from '../../../../lib/hive/Commands/GetPrimaryKeysCommand';
import { TGetPrimaryKeysReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('GetPrimaryKeysCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new GetPrimaryKeysCommand(thriftClient, new ClientContextStub());

    const request: TGetPrimaryKeysReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
    };

    const response = await command.execute(request);
    expect(thriftClient.GetPrimaryKeys.called).to.be.true;
    expect(thriftClient.getPrimaryKeysReq).to.deep.equal(new TGetPrimaryKeysReq(request));
    expect(response).to.be.deep.eq(thriftClient.getPrimaryKeysResp);
  });
});
