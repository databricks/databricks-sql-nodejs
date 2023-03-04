const { DBSQLClient, thrift } = require('../');

const client = new DBSQLClient();

const host = '****.databricks.com';
const path = '/sql/1.0/endpoints/****';
const token = 'dapi********************************';
const useAADToken = false;
// For AAD tokens
// const token = 'ey***********';
// const useAADToken=true;
client
  .connect({ host, path, token, useAADToken })
  .then(async (client) => {
    const session = await client.openSession();
    const response = await session.getInfo(thrift.TCLIService_types.TGetInfoType.CLI_DBMS_VER);

    console.log(response.getValue());

    await session.close();
    await client.close();
  })
  .catch((error) => {
    console.log(error);
  });
