const { expect } = require('chai');
const config = require('./utils/config');
const logger = require('./utils/logger')(config.logger);
const { DBSQLClient } = require('../..');
const fs = require('fs')
const globalConfig = require('../../dist/globalConfig').default;


const client = new DBSQLClient({stagingAllowedLocalPath: ["tests/e2e/staging"]});

client.connect({
    host: config.host,
    path: config.path,
    token: config.token,
  }).then(async (client) => {
    let temp_path = "tests/e2e/staging/data"
    fs.writeFileSync(temp_path,data="Hello World!")
    const connection = await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
    });
  
    let session = await client.openSession({
      initialCatalog: config.database[0],
      initialSchema: config.database[1],
    });
    let result = await session.executeStagingStatement(`PUT '${temp_path}' INTO '/Volumes/${config.database[0]}/${config.database[1]}/e2etests/file1.csv' OVERWRITE`)
  }
  )
