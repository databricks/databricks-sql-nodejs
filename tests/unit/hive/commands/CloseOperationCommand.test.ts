import { expect } from 'chai';
import sinon from 'sinon';
import CloseOperationCommand from '../../../../lib/hive/Commands/CloseOperationCommand';
import { TCloseOperationReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('CloseOperationCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new CloseOperationCommand(thriftClient, new ClientContextStub());

    const request: TCloseOperationReq = {
      operationHandle: {
        hasResultSet: true,
        operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
        operationType: 0,
        modifiedRowCount: 0,
      },
    };

    const response = await command.execute(request);
    expect(thriftClient.CloseOperation.called).to.be.true;
    expect(thriftClient.closeOperationReq).to.deep.equal(new TCloseOperationReq(request));
    expect(response).to.be.deep.eq(thriftClient.closeOperationResp);
  });
});
