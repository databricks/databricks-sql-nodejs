const { expect } = require('chai');
const config = require('./utils/config');
const { DBSQLClient } = require('../..');
const fs = require('fs');

// TODO: Temporarily disable those tests until we figure out issues with E2E test env
describe.skip('Staging Test', () => {
  it('put staging data and receive it', async () => {
    const client = new DBSQLClient();
    await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
    });
    let tempPath = 'tests/e2e/staging/data';
    fs.writeFileSync(tempPath, 'Hello World!');

    const session = await client.openSession({
      initialCatalog: config.database[0],
      initialSchema: config.database[1],
    });
    await session.executeStatement(
      `PUT '${tempPath}' INTO '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv' OVERWRITE`,
      { stagingAllowedLocalPath: ['tests/e2e/staging'] },
    );
    await session.executeStatement(
      `GET '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv' TO 'tests/e2e/staging/file'`,
      { stagingAllowedLocalPath: ['tests/e2e/staging'] },
    );
    let result = fs.readFileSync('tests/e2e/staging/file');
    expect(result.toString() === 'Hello World!').to.be.true;
  });

  it('put staging data and remove it', async () => {
    const client = new DBSQLClient();
    await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
    });
    let tempPath = 'tests/e2e/staging/data';
    fs.writeFileSync(tempPath, (data = 'Hello World!'));

    let session = await client.openSession({
      initialCatalog: config.database[0],
      initialSchema: config.database[1],
    });
    await session.executeStatement(
      `PUT '${tempPath}' INTO '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv' OVERWRITE`,
      { stagingAllowedLocalPath: ['tests/e2e/staging'] },
    );
    await session.executeStatement(`REMOVE '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv'`, {
      stagingAllowedLocalPath: ['tests/e2e/staging'],
    });
  });

  it('delete non-existent data', async () => {
    const client = new DBSQLClient();
    await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
    });
    let tempPath = 'tests/e2e/staging/data';
    fs.writeFileSync(tempPath, (data = 'Hello World!'));

    let session = await client.openSession({
      initialCatalog: config.database[0],
      initialSchema: config.database[1],
    });
    await session.executeStatement(
      `PUT '${tempPath}' INTO '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv' OVERWRITE`,
      { stagingAllowedLocalPath: ['tests/e2e/staging'] },
    );
    await session.executeStatement(
      `GET '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv' TO 'tests/e2e/staging/file'`,
      { stagingAllowedLocalPath: ['tests/e2e/staging'] },
    );
    let result = fs.readFileSync('tests/e2e/staging/file');
    expect(result.toString() === 'Hello World!').to.be.true;
  });
});
