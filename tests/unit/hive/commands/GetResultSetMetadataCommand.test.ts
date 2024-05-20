import { expect } from 'chai';
import sinon from 'sinon';
import GetResultSetMetadataCommand from '../../../../lib/hive/Commands/GetResultSetMetadataCommand';
import { TOperationType, TGetResultSetMetadataReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('GetResultSetMetadataCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new GetResultSetMetadataCommand(thriftClient, new ClientContextStub());

    const request: TGetResultSetMetadataReq = {
      operationHandle: {
        operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
        operationType: TOperationType.EXECUTE_STATEMENT,
        hasResultSet: true,
      },
    };

    const response = await command.execute(request);
    expect(thriftClient.GetResultSetMetadata.called).to.be.true;
    expect(thriftClient.getResultSetMetadataReq).to.deep.equal(new TGetResultSetMetadataReq(request));
    expect(response).to.be.deep.eq(thriftClient.getResultSetMetadataResp);
  });
});
