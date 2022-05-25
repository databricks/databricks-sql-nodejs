const driver = require('../');
const { TCLIService, TCLIService_types } = driver.thrift;

const client = new driver.DBSQLClient(
    TCLIService,
    TCLIService_types
);

const [host, path, token] = process.argv.slice(2);

client.connect({ host, path, token }).then(async client => {
    const session = await client.openSession();
    const response = await session.getInfo(
        TCLIService_types.TGetInfoType.CLI_DBMS_VER
    );

    console.log(response.getValue());

    await session.close();
}).catch(error => {
    console.log(error);
});
