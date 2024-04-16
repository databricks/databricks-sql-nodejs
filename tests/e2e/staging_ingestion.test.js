const { expect } = require('chai');
const fs = require('fs');
const path = require('path');
const os = require('os');
const uuid = require('uuid');
const config = require('./utils/config');
const { DBSQLClient } = require('../..');
const StagingError = require('../../dist/errors/StagingError').default;

describe('Staging Test', () => {
  const catalog = config.database[0];
  const schema = config.database[1];
  const volume = config.volume;

  const localPath = fs.mkdtempSync(path.join(os.tmpdir(), 'databricks-sql-tests-'));

  before(() => {
    expect(catalog).to.not.be.undefined;
    expect(schema).to.not.be.undefined;
    expect(volume).to.not.be.undefined;
  });

  after(() => {
    fs.rmSync(localPath, {
      recursive: true,
      force: true,
    });
  });

  it('put staging data and receive it', async () => {
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

  it('delete non-existent data', async () => {
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
