const driver = require('../');
const { TCLIService, TCLIService_types } = driver.thrift;

const client = new driver.DBSQLClient(
    TCLIService,
    TCLIService_types
);

const utils = new driver.HiveUtils(
    TCLIService_types
);


const [host, path, token] = process.argv.slice(2);

client.connect({ host, path, token }).then(async client => {
    const session = await client.openSession();
    await createTables(session);

    const typeInfo = await session.getTypeInfo().then(handleOperation);
    console.log(typeInfo);
    console.log();

    const catalogs = await session.getCatalogs().then(handleOperation);
    console.log(catalogs);
    console.log();

    const schemas = await session.getSchemas({}).then(handleOperation);
    console.log(schemas);
    console.log();

    const tables = await session.getTables({}).then(handleOperation);
    console.log(tables);
    console.log();

    const tableTypes = await session.getTableTypes({}).then(handleOperation);
    console.log(tableTypes);
    console.log();

    const columns = await session.getColumns({}).then(handleOperation);
    console.log(columns);
    console.log();
    
    await session.close();
}).catch(error => {
    console.log(error);
});

async function handleOperation(operation) {
    await utils.waitUntilReady(operation, true, (stateResponse) => {
        console.log(stateResponse.taskStatus);
    });
    await utils.fetchAll(operation);
    await operation.close();
    return utils.getResult(operation).getValue();
}

const createTables = async (session) => {
    await session.executeStatement('create table if not exists table1 ( id string, value integer )').then(handleOperation);
    await session.executeStatement('create table if not exists table2 ( id string, table1_fk integer )').then(handleOperation);
};
