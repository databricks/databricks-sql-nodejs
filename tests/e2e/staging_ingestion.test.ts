import { expect } from 'chai';
import assert from 'assert';
import fs from 'fs';
import path from 'path';
import os from 'os';
import * as uuid from 'uuid';
import { DBSQLClient } from '../../lib';
import StagingError from '../../lib/errors/StagingError';
import { ResultFormat } from '../../lib/contracts/ResultMetadata';

import config from './utils/config';

describe('Staging Test', () => {
  const localPath = fs.mkdtempSync(path.join(os.tmpdir(), 'databricks-sql-tests-'));

  after(() => {
    fs.rmSync(localPath, {
      recursive: true,
      force: true,
    });
  });

  it('put staging data and receive it', async () => {
    const { catalog, schema, volume } = config;

    const client = new DBSQLClient();
    await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
    });

    const session = await client.openSession({
      initialCatalog: catalog,
      initialSchema: schema,
    });

    const expectedData = 'Hello World!';
    const stagingFileName = `/Volumes/${catalog}/${schema}/${volume}/${uuid.v4()}.csv`;
    const localFile = path.join(localPath, `${uuid.v4()}.csv`);

    fs.writeFileSync(localFile, expectedData);
    await session.executeStatement(`PUT '${localFile}' INTO '${stagingFileName}' OVERWRITE`, {
      stagingAllowedLocalPath: [localPath],
    });
    fs.rmSync(localFile);

    await session.executeStatement(`GET '${stagingFileName}' TO '${localFile}'`, {
      stagingAllowedLocalPath: [localPath],
    });
    const result = fs.readFileSync(localFile);
    fs.rmSync(localFile);
    expect(result.toString() === expectedData).to.be.true;
  });

  it('put staging data and remove it', async () => {
    const { catalog, schema, volume } = config;

    const client = new DBSQLClient();
    await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
    });

    const session = await client.openSession({
      initialCatalog: catalog,
      initialSchema: schema,
    });

    const expectedData = 'Hello World!';
    const stagingFileName = `/Volumes/${catalog}/${schema}/${volume}/${uuid.v4()}.csv`;
    const localFile = path.join(localPath, `${uuid.v4()}.csv`);

    fs.writeFileSync(localFile, expectedData);
    await session.executeStatement(`PUT '${localFile}' INTO '${stagingFileName}' OVERWRITE`, {
      stagingAllowedLocalPath: [localPath],
    });
    fs.rmSync(localFile);

    await session.executeStatement(`REMOVE '${stagingFileName}'`, { stagingAllowedLocalPath: [localPath] });

    try {
      await session.executeStatement(`GET '${stagingFileName}' TO '${localFile}'`, {
        stagingAllowedLocalPath: [localPath],
      });
      expect.fail('It should throw HTTP 404 error');
    } catch (error) {
      if (error instanceof StagingError) {
        // File should not exist after deleting
        expect(error.message).to.contain('404');
      } else {
        throw error;
      }
    } finally {
      fs.rmSync(localFile, { force: true });
    }
  });

  it('put, get and remove staging data with zero rowLimit and default Arrow', async () => {
    const { catalog, schema, volume } = config;
    const client = new DBSQLClient();
    expect(client.getConfig().arrowEnabled).to.be.true;
    const stagingFileName = `/Volumes/${catalog}/${schema}/${volume}/${uuid.v4()}.csv`;
    const localFile = path.join(localPath, `${uuid.v4()}.csv`);
    const expectedData = 'zero-row cap staging regression';
    const stagingOptions = { stagingAllowedLocalPath: [localPath] };
    const options = { ...stagingOptions, rowLimit: 0 };
    fs.writeFileSync(localFile, expectedData);
    try {
      await client.connect({ host: config.host, path: config.path, token: config.token });
      const session = await client.openSession({ initialCatalog: catalog, initialSchema: schema });
      try {
        const put = await session.executeStatement(`PUT '${localFile}' INTO '${stagingFileName}' OVERWRITE`, options);
        try {
          expect((await put.getResultMetadata()).resultFormat).to.equal(ResultFormat.ArrowBased);
        } finally {
          await put.close();
        }
        fs.rmSync(localFile);
        const get = await session.executeStatement(`GET '${stagingFileName}' TO '${localFile}'`, options);
        try {
          expect(fs.readFileSync(localFile, 'utf8')).to.equal(expectedData);
        } finally {
          await get.close();
        }
        const remove = await session.executeStatement(`REMOVE '${stagingFileName}'`, options);
        await remove.close();
        await assert.rejects(
          session.executeStatement(`GET '${stagingFileName}' TO '${localFile}'`, stagingOptions),
          (error: unknown) => error instanceof StagingError && error.message.includes('404'),
        );
      } finally {
        try {
          const cleanup = await session.executeStatement(`REMOVE '${stagingFileName}'`, stagingOptions);
          await cleanup.close();
        } finally {
          await session.close();
        }
      }
    } finally {
      await client.close();
    }
  });

  it('delete non-existent data', async () => {
    const { catalog, schema, volume } = config;

    const client = new DBSQLClient();
    await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
    });

    const session = await client.openSession({
      initialCatalog: catalog,
      initialSchema: schema,
    });

    const stagingFileName = `/Volumes/${catalog}/${schema}/${volume}/${uuid.v4()}.csv`;
    const localFile = path.join(localPath, `${uuid.v4()}.csv`);

    // File should not exist before removing
    try {
      await session.executeStatement(`GET '${stagingFileName}' TO '${localFile}'`, {
        stagingAllowedLocalPath: [localPath],
      });
      expect.fail('It should throw HTTP 404 error');
    } catch (error) {
      if (error instanceof StagingError) {
        expect(error.message).to.contain('404');
      } else {
        throw error;
      }
    } finally {
      fs.rmSync(localFile, { force: true });
    }

    // Try to remove the file - it should succeed and not throw any errors
    await session.executeStatement(`REMOVE '${stagingFileName}'`, { stagingAllowedLocalPath: [localPath] });
  });
});
