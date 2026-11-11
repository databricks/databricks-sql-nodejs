import { expect } from 'chai';
import sinon from 'sinon';
import https from 'https';
import { DBSQLClient } from '../../lib';
import CloudFetchResultHandler from '../../lib/result/CloudFetchResultHandler';

import config from './utils/config';

describe('httpAgent', () => {
  it('should send Thrift and CloudFetch requests through the supplied agent', async () => {
    const httpAgent = new https.Agent({ keepAlive: true });
    const createConnection = sinon.spy(httpAgent as any, 'createConnection');
    const client = new DBSQLClient();

    try {
      await client.connect({
        host: config.host,
        path: config.path,
        token: config.token,
        httpAgent,
      });
      const session = await client.openSession({
        initialCatalog: config.catalog,
        initialSchema: config.schema,
      });

      // Large enough for the server to return CloudFetch links.
      const operation = await session.executeStatement(
        'SELECT * FROM range(0, 10000000) AS t1 LEFT JOIN (SELECT 1) AS t2',
        { maxRows: null, useCloudFetch: true },
      );
      const chunk = await operation.fetchChunk({ maxRows: 100000, disableBuffering: true });
      expect(chunk.length).to.be.greaterThan(0);

      // @ts-expect-error TS2339: Property getResultHandler does not exist on type IOperation
      const resultHandler = await operation.getResultHandler();
      expect(resultHandler.source.source).to.be.instanceOf(CloudFetchResultHandler);

      await operation.close();
      await session.close();

      const hosts = createConnection.args.map(([options]) => (options as { host: string }).host);
      expect(hosts).to.include(config.host);
      // CloudFetch links point at cloud storage, not the workspace host.
      expect(hosts.some((host) => host !== config.host)).to.be.true;
    } finally {
      await client.close();
      httpAgent.destroy();
    }
  });
});
