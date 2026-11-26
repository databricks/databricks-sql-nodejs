import { expect } from 'chai';
import fs from 'fs';
import path from 'path';
import os from 'os';
import * as uuid from 'uuid';
import { DBSQLClient } from '../../../lib';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import StagingError from '../../../lib/errors/StagingError';

import config from './config';

export default function stagingTests(connectOptions: () => ConnectionOptions) {
  let localPath: string;
  before(() => {
    localPath = fs.mkdtempSync(path.join(os.tmpdir(), 'databricks-sql-tests-'));
  });

  after(() => {
    fs.rmSync(localPath, {
      recursive: true,
      force: true,
    });
  });

  it('put staging data and receive it', async () => {
    const { catalog, schema, volume } = config;

    const client = new DBSQLClient();
    await client.connect(connectOptions());

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
    expect(result.toString()).to.equal(expectedData);
  });

  it('put staging data and remove it', async () => {
    const { catalog, schema, volume } = config;

    const client = new DBSQLClient();
    await client.connect(connectOptions());

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

  it('delete non-existent data', async () => {
    const { catalog, schema, volume } = config;

    const client = new DBSQLClient();
    await client.connect(connectOptions());

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
}
