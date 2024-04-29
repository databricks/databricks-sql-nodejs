const { expect } = require('chai');
const sinon = require('sinon');
const { TCLIService_types } = require('../../../lib').thrift;
const HiveDriver = require('../../../lib/hive/HiveDriver').default;

const toTitleCase = (str) => str[0].toUpperCase() + str.slice(1);

const testCommand = async (command, request) => {
  const client = {};
  const clientContext = {
    getClient: sinon.stub().returns(Promise.resolve(client)),
  };
  const driver = new HiveDriver({
    context: clientContext,
  });

  const response = { response: 'value' };
  client[toTitleCase(command)] = function (req, cb) {
    expect(req).to.be.deep.eq(new TCLIService_types[`T${toTitleCase(command)}Req`](request));
    cb(null, response);
  };

  const resp = await driver[command](request);
  expect(resp).to.be.deep.eq(response);
  expect(clientContext.getClient.called).to.be.true;
};

describe('HiveDriver', () => {
  const sessionHandle = { sessionId: { guid: 'guid', secret: 'secret' } };
  const operationHandle = {
    operationId: { guid: 'guid', secret: 'secret' },
    operationType: '',
    hasResultSet: false,
  };

  it('should execute closeSession', () => {
    return testCommand('closeSession', { sessionHandle });
  });

  it('should execute executeStatement', () => {
    return testCommand('executeStatement', { sessionHandle, statement: 'SELECT * FROM t' });
  });

  it('should execute getResultSetMetadata', () => {
    return testCommand('getResultSetMetadata', { operationHandle });
  });

  it('should execute fetchResults', () => {
    return testCommand('fetchResults', { operationHandle, orientation: 1, maxRows: 100 });
  });

  it('should execute getInfo', () => {
    return testCommand('getInfo', { sessionHandle, infoType: 1 });
  });

  it('should execute getTypeInfo', () => {
    return testCommand('getTypeInfo', { sessionHandle });
  });

  it('should execute getCatalogs', () => {
    return testCommand('getCatalogs', { sessionHandle });
  });

  it('should execute getSchemas', () => {
    return testCommand('getSchemas', { sessionHandle });
  });

  it('should execute getTables', () => {
    return testCommand('getTables', { sessionHandle });
  });

  it('should execute getTableTypes', () => {
    return testCommand('getTableTypes', { sessionHandle });
  });

  it('should execute getColumns', () => {
    return testCommand('getColumns', { sessionHandle });
  });

  it('should execute getFunctions', () => {
    return testCommand('getFunctions', { sessionHandle, functionName: 'AVG' });
  });

  it('should execute getPrimaryKeys', () => {
    return testCommand('getPrimaryKeys', { sessionHandle });
  });

  it('should execute getCrossReference', () => {
    return testCommand('getCrossReference', { sessionHandle });
  });

  it('should execute getOperationStatus', () => {
    return testCommand('getOperationStatus', { operationHandle });
  });

  it('should execute cancelOperation', () => {
    return testCommand('cancelOperation', { operationHandle });
  });

  it('should execute closeOperation', () => {
    return testCommand('closeOperation', { operationHandle });
  });

  it('should execute getDelegationToken', () => {
    return testCommand('getDelegationToken', { sessionHandle, owner: 'owner', renewer: 'renewer' });
  });

  it('should execute cancelDelegationToken', () => {
    return testCommand('cancelDelegationToken', { sessionHandle, delegationToken: 'delegationToken' });
  });

  it('should execute renewDelegationToken', () => {
    return testCommand('renewDelegationToken', { sessionHandle, delegationToken: 'delegationToken' });
  });
});
