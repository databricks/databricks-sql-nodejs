import { expect, AssertionError } from 'chai';
import SeaBackend from '../../../lib/sea/SeaBackend';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import { ConnectionOptions, OpenSessionRequest } from '../../../lib/contracts/IDBSQLClient';

describe('SeaBackend stub', () => {
  it('connect() rejects with HiveDriverError until M1 wires the binding', async () => {
    const backend = new SeaBackend();
    try {
      await backend.connect({ host: '', path: '', token: '' } as ConnectionOptions);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(error).to.be.instanceOf(HiveDriverError);
      expect(error.message).to.contain('not implemented');
    }
  });

  it('openSession() rejects with HiveDriverError until M1 wires the binding', async () => {
    const backend = new SeaBackend();
    try {
      await backend.openSession({} as OpenSessionRequest);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(error).to.be.instanceOf(HiveDriverError);
      expect(error.message).to.contain('not implemented');
    }
  });

  it('close() is a no-op so DBSQLClient.close() can finish state-clearing after a failed connect', async () => {
    const backend = new SeaBackend();
    await backend.close();
  });
});
