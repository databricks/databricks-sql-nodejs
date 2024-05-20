import { expect } from 'chai';
import sinon from 'sinon';
import GetTablesCommand from '../../../../lib/hive/Commands/GetTablesCommand';
import { TGetTablesReq } from '../../../../thrift/TCLIService_types';

import ClientContextStub from '../../.stubs/ClientContextStub';
import ThriftClientStub from '../../.stubs/ThriftClientStub';

describe('GetTablesCommand', () => {
  it('should return response', async () => {
    const thriftClient = sinon.spy(new ThriftClientStub());
    const command = new GetTablesCommand(thriftClient, new ClientContextStub());

    const request: TGetTablesReq = {
      sessionHandle: {
        sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      },
      catalogName: 'catalog',
      schemaName: 'schema',
      tableName: 'table',
      tableTypes: ['TABLE', 'VIEW', 'SYSTEM TABLE', 'GLOBAL TEMPORARY', 'LOCAL TEMPORARY', 'ALIAS', 'SYNONYM'],
    };

    const response = await command.execute(request);
    expect(thriftClient.GetTables.called).to.be.true;
    expect(thriftClient.getTablesReq).to.deep.equal(new TGetTablesReq(request));
    expect(response).to.be.deep.eq(thriftClient.getTablesResp);
  });
});
