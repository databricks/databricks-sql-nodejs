import { expect } from 'chai';
import sinon from 'sinon';
import fs from 'fs';
import os from 'os';
import path from 'path';
import * as uuid from 'uuid';
import { DBSQLClient } from '../../lib';
import { ClientConfig } from '../../lib/contracts/IClientContext';
import IDBSQLSession from '../../lib/contracts/IDBSQLSession';
import ArrowResultHandler from '../../lib/result/ArrowResultHandler';
import CloudFetchResultHandler from '../../lib/result/CloudFetchResultHandler';
import JsonResultHandler from '../../lib/result/JsonResultHandler';

import config from './utils/config';

// From #118: same-named columns overwrite each other in object rows.
const DUPLICATE_ALIAS_QUERY =
  "SELECT carat AS a, color AS a FROM VALUES (0.23, 'E'), (0.21, 'I') AS t(carat, color) ORDER BY carat DESC";

async function openSession(customConfig: Partial<ClientConfig> = {}): Promise<IDBSQLSession> {
  const client = new DBSQLClient();
  const clientConfig = client.getConfig();
  sinon.stub(client, 'getConfig').returns({ ...clientConfig, ...customConfig });
  const connection = await client.connect({ host: config.host, path: config.path, token: config.token });
  return connection.openSession({ initialCatalog: config.catalog, initialSchema: config.schema });
}

// Whether the operation's result pipeline (converter or its source) is an instance of `handlerClass`.
async function usesResultHandler(operation: unknown, handlerClass: new (...args: any[]) => unknown): Promise<boolean> {
  // @ts-expect-error TS2339: getResultHandler is not part of IOperation
  const resultHandler = await operation.getResultHandler();
  return [resultHandler.source, resultHandler.source.source].some((source) => source instanceof handlerClass);
}

describe('rowMode', () => {
  for (const [name, clientConfig, handlerClass] of [
    ['column-based results', { arrowEnabled: false }, JsonResultHandler],
    ['inline Arrow results', { arrowEnabled: true, useLZ4Compression: true }, ArrowResultHandler],
  ] as const) {
    it(`keeps every same-named column with ${name}`, async () => {
      const session = await openSession(clientConfig);

      const arrayOperation = await session.executeStatement(DUPLICATE_ALIAS_QUERY, {
        useCloudFetch: false,
        rowMode: 'array',
      });
      expect(await usesResultHandler(arrayOperation, handlerClass)).to.be.true;
      expect(await arrayOperation.fetchAll()).to.deep.equal([
        [0.23, 'E'],
        [0.21, 'I'],
      ]);
      await arrayOperation.close();

      const objectOperation = await session.executeStatement(DUPLICATE_ALIAS_QUERY, { useCloudFetch: false });
      expect(await objectOperation.fetchAll()).to.deep.equal([{ a: 'E' }, { a: 'I' }]);
      await objectOperation.close();

      await session.close();
    });
  }

  it('keeps every same-named column with CloudFetch results', async () => {
    const session = await openSession({ cloudFetchConcurrentDownloads: 5 });
    const rowCount = 2000000; // result has to be quite big to enable CloudFetch
    const operation = await session.executeStatement(
      `SELECT id AS a, CAST(id AS STRING) AS a FROM range(0, ${rowCount})`,
      { maxRows: null, useCloudFetch: true, rowMode: 'array' },
    );
    await operation.finished();
    expect(await usesResultHandler(operation, CloudFetchResultHandler)).to.be.true;

    let fetched = 0;
    do {
      // eslint-disable-next-line no-await-in-loop
      const chunk = (await operation.fetchChunk({ maxRows: 100000, disableBuffering: true })) as Array<
        [number, string]
      >;
      for (const [id, text] of chunk) {
        expect(text).to.equal(String(id));
      }
      fetched += chunk.length;
    } while (await operation.hasMoreRows()); // eslint-disable-line no-await-in-loop
    expect(fetched).to.equal(rowCount);

    await operation.close();
    await session.close();
  });

  it('runs staging statements in array mode', async () => {
    const { catalog, schema, volume } = config;
    const localPath = fs.mkdtempSync(path.join(os.tmpdir(), 'databricks-sql-tests-'));
    try {
      const session = await openSession();
      const stagingFileName = `/Volumes/${catalog}/${schema}/${volume}/${uuid.v4()}.csv`;
      const localFile = path.join(localPath, `${uuid.v4()}.csv`);
      const options = { stagingAllowedLocalPath: [localPath], rowMode: 'array' as const };

      fs.writeFileSync(localFile, 'Hello World!');
      await session.executeStatement(`PUT '${localFile}' INTO '${stagingFileName}' OVERWRITE`, options);
      fs.rmSync(localFile);

      await session.executeStatement(`GET '${stagingFileName}' TO '${localFile}'`, options);
      expect(fs.readFileSync(localFile).toString()).to.equal('Hello World!');

      await session.executeStatement(`REMOVE '${stagingFileName}'`, options);
      await session.close();
    } finally {
      fs.rmSync(localPath, { recursive: true, force: true });
    }
  });
});
