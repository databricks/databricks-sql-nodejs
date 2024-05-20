import { expect } from 'chai';
import sinon from 'sinon';
import GetOperationStatusCommand from '../../../../lib/hive/Commands/GetOperationStatusCommand';
import { TGetOperationStatusReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('GetOperationStatusCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new GetOperationStatusCommand(thriftClient, new ClientContextStub());

    const request: TGetOperationStatusReq = {
      operationHandle: {
        hasResultSet: true,
        operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
        operationType: 0,
        modifiedRowCount: 0,
      },
      getProgressUpdate: true,
    };

    const response = await command.execute(request);
    expect(thriftClient.GetOperationStatus.called).to.be.true;
    expect(thriftClient.getOperationStatusReq).to.deep.equal(new TGetOperationStatusReq(request));
    expect(response).to.be.deep.eq(thriftClient.getOperationStatusResp);
  });
});
