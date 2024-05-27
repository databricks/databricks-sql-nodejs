import { expect } from 'chai';
import sinon from 'sinon';
import GetFunctionsCommand from '../../../../lib/hive/Commands/GetFunctionsCommand';
import { TGetFunctionsReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('GetFunctionsCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new GetFunctionsCommand(thriftClient, new ClientContextStub());

    const request: TGetFunctionsReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
      functionName: 'test',
    };

    const response = await command.execute(request);
    expect(thriftClient.GetFunctions.called).to.be.true;
    expect(thriftClient.getFunctionsReq).to.deep.equal(new TGetFunctionsReq(request));
    expect(response).to.be.deep.eq(thriftClient.getFunctionsResp);
  });
});
