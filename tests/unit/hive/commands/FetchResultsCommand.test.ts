import { expect } from 'chai';
import sinon from 'sinon';
import Int64 from 'node-int64';
import FetchResultsCommand from '../../../../lib/hive/Commands/FetchResultsCommand';
import { TOperationType, TFetchOrientation, TFetchResultsReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('FetchResultsCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new FetchResultsCommand(thriftClient, new ClientContextStub());

    const request: TFetchResultsReq = {
      operationHandle: {
        operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
        operationType: TOperationType.EXECUTE_STATEMENT,
        hasResultSet: true,
      },
      orientation: TFetchOrientation.FETCH_FIRST,
      maxRows: new Int64(100),
      fetchType: 0,
    };

    const response = await command.execute(request);
    expect(thriftClient.FetchResults.called).to.be.true;
    expect(thriftClient.fetchResultsReq).to.deep.equal(new TFetchResultsReq(request));
    expect(response).to.be.deep.eq(thriftClient.fetchResultsResp);
  });
});
