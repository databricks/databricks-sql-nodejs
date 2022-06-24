const { DBSQLClient } = require('../');

const client = new DBSQLClient();

const utils = DBSQLClient.utils;


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
    await session.executeStatement('CREATE TABLE IF NOT EXISTS table1 ( id STRING, value INTEGER )').then(handleOperation);
    await session.executeStatement('CREATE TABLE IF NOT EXISTS table2 ( id STRING, table1_fk INTEGER )').then(handleOperation);
};
