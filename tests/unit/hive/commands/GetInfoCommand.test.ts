import { expect } from 'chai';
import sinon from 'sinon';
import GetInfoCommand from '../../../../lib/hive/Commands/GetInfoCommand';
import { TGetInfoReq, TGetInfoType } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('GetInfoCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new GetInfoCommand(thriftClient, new ClientContextStub());

    const request: TGetInfoReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
      infoType: TGetInfoType.CLI_SERVER_NAME,
    };

    const response = await command.execute(request);
    expect(thriftClient.GetInfo.called).to.be.true;
    expect(thriftClient.getInfoReq).to.deep.equal(new TGetInfoReq(request));
    expect(response).to.be.deep.eq(thriftClient.getInfoResp);
  });
});
