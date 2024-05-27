import { expect } from 'chai';
import sinon from 'sinon';
import CancelOperationCommand from '../../../../lib/hive/Commands/CancelOperationCommand';
import { TCancelOperationReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('CancelOperationCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new CancelOperationCommand(thriftClient, new ClientContextStub());

    const request: TCancelOperationReq = {
      operationHandle: {
        hasResultSet: true,
        operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
        operationType: 0,
        modifiedRowCount: 0,
      },
    };

    const response = await command.execute(request);
    expect(thriftClient.CancelOperation.called).to.be.true;
    expect(thriftClient.cancelOperationReq).to.deep.equal(new TCancelOperationReq(request));
    expect(response).to.be.deep.eq(thriftClient.cancelOperationResp);
  });
});
