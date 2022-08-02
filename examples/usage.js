const { DBSQLClient, thrift } = require('../');

const client = new DBSQLClient();

const [host, path, token] = process.argv.slice(2);

client
  .connect({ host, path, token })
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
