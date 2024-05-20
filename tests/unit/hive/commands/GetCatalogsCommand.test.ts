import { expect } from 'chai';
import sinon from 'sinon';
import GetCatalogsCommand from '../../../../lib/hive/Commands/GetCatalogsCommand';
import { TGetCatalogsReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('GetCatalogsCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new GetCatalogsCommand(thriftClient, new ClientContextStub());

    const request: TGetCatalogsReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
    };

    const response = await command.execute(request);
    expect(thriftClient.GetCatalogs.called).to.be.true;
    expect(thriftClient.getCatalogsReq).to.deep.equal(new TGetCatalogsReq(request));
    expect(response).to.be.deep.eq(thriftClient.getCatalogsResp);
  });
});
