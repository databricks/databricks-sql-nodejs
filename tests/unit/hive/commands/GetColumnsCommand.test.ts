import { expect } from 'chai';
import sinon from 'sinon';
import GetColumnsCommand from '../../../../lib/hive/Commands/GetColumnsCommand';
import { TGetColumnsReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('GetColumnsCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new GetColumnsCommand(thriftClient, new ClientContextStub());

    const request: TGetColumnsReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
    };

    const response = await command.execute(request);
    expect(thriftClient.GetColumns.called).to.be.true;
    expect(thriftClient.getColumnsReq).to.deep.equal(new TGetColumnsReq(request));
    expect(response).to.be.deep.eq(thriftClient.getColumnsResp);
  });
});
