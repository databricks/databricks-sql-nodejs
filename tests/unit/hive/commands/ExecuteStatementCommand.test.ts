import { expect } from 'chai';
import sinon from 'sinon';
import Int64 from 'node-int64';
import ExecuteStatementCommand from '../../../../lib/hive/Commands/ExecuteStatementCommand';
import { TExecuteStatementReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('ExecuteStatementCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new ExecuteStatementCommand(thriftClient, new ClientContextStub());

    const request: TExecuteStatementReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
      statement: 'SHOW TABLES',
      queryTimeout: new Int64(0),
    };

    const response = await command.execute(request);
    expect(thriftClient.ExecuteStatement.called).to.be.true;
    expect(thriftClient.executeStatementReq).to.deep.equal(new TExecuteStatementReq(request));
    expect(response).to.be.deep.eq(thriftClient.executeStatementResp);
  });
});
