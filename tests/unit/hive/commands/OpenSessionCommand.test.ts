import { expect } from 'chai';
import sinon from 'sinon';
import OpenSessionCommand from '../../../../lib/hive/Commands/OpenSessionCommand';
import { TProtocolVersion, TOpenSessionReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('OpenSessionCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new OpenSessionCommand(thriftClient, new ClientContextStub());

    const request: TOpenSessionReq = {
      client_protocol: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8,
    };

    const response = await command.execute(request);
    expect(thriftClient.OpenSession.called).to.be.true;
    expect(thriftClient.openSessionReq).to.deep.equal(new TOpenSessionReq(request));
    expect(response).to.be.deep.eq(thriftClient.openSessionResp);
  });
});
