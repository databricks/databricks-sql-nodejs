const { expect } = require('chai');
const config = require('./utils/config');
const logger = require('./utils/logger')(config.logger);
const { DBSQLClient } = require('../..');
const fs = require('fs')
const globalConfig = require('../../dist/globalConfig').default;


describe('Staging Test', () => {
it("put staging data and receive it", async () => {
  const client = new DBSQLClient();
  await client.connect({
    host: config.host,
    path: config.path,
    token: config.token,
  })
    let temp_path = "tests/e2e/staging/data"
    fs.writeFileSync(temp_path,data="Hello World!")
  
    let session = await client.openSession({
      initialCatalog: config.database[0],
      initialSchema: config.database[1],
    });
    await session.executeStatement(`PUT '${temp_path}' INTO '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv' OVERWRITE`,{stagingAllowedLocalPath: ["tests/e2e/staging"]})
    await session.executeStatement(`GET '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv' TO 'tests/e2e/staging/file'`,{stagingAllowedLocalPath: ["tests/e2e/staging"]})
    let result = fs.readFileSync('tests/e2e/staging/file')
    expect(result.toString() === "Hello World!").to.be.true
  })

  it("put staging data and receive it", async () => {
    const client = new DBSQLClient();
    await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
    })
      let temp_path = "tests/e2e/staging/data"
      fs.writeFileSync(temp_path,data="Hello World!")
    
      let session = await client.openSession({
        initialCatalog: config.database[0],
        initialSchema: config.database[1],
      });
      await session.executeStatement(`PUT '${temp_path}' INTO '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv' OVERWRITE`,{stagingAllowedLocalPath: ["tests/e2e/staging"]})
      await session.executeStatement(`GET '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv' TO 'tests/e2e/staging/file'`,{stagingAllowedLocalPath: ["tests/e2e/staging"]})
      let result = fs.readFileSync('tests/e2e/staging/file')
      expect(result.toString() === "Hello World!").to.be.true
    })

  it("put staging data and remove it", async () => {
    const client = new DBSQLClient();
    await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
    })
      let temp_path = "tests/e2e/staging/data"
      fs.writeFileSync(temp_path,data="Hello World!")
    
      let session = await client.openSession({
        initialCatalog: config.database[0],
        initialSchema: config.database[1],
      });
      await session.executeStatement(`PUT '${temp_path}' INTO '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv' OVERWRITE`,{stagingAllowedLocalPath: ["tests/e2e/staging"]})
      await session.executeStatement(`REMOVE '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv'`,{stagingAllowedLocalPath: ["tests/e2e/staging"]})
    })

  it("delete non-existent data", async () => {
    const client = new DBSQLClient();
    await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
    })
      let temp_path = "tests/e2e/staging/data"
      fs.writeFileSync(temp_path,data="Hello World!")
    
      let session = await client.openSession({
        initialCatalog: config.database[0],
        initialSchema: config.database[1],
      });
      await session.executeStatement(`PUT '${temp_path}' INTO '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv' OVERWRITE`,{stagingAllowedLocalPath: ["tests/e2e/staging"]})
      await session.executeStatement(`GET '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv' TO 'tests/e2e/staging/file'`,{stagingAllowedLocalPath: ["tests/e2e/staging"]})
      let result = fs.readFileSync('tests/e2e/staging/file')
      expect(result.toString() === "Hello World!").to.be.true
    })
})

