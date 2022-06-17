# Preview Databricks SQL Node.js Driver

## Description

Databricks SQL Driver is a JavaScript driver for connection to [Databricks SQL](https://databricks.com/product/databricks-sql) and issue SQL queries.

**NOTE: This Driver is in Preview mode.**

## Installation

```bash
npm i databricks-sql-node
```

## Usage

[examples/usage.js](examples/usage.js)
```javascript
const { DBSQLClient } = require('databricks-sql-node');

const client = new DBSQLClient();
const utils = DBSQLClient.utils;

client.connect({
    host: '********.databricks.com',
    path: '/sql/1.0/endpoints/****************',
    token: 'dapi********************************',
}).then(async client => {
    const session = await client.openSession();

    const queryOperation = await session.executeStatement('SELECT "Hello, World!"', { runAsync: true });
    await utils.waitUntilReady(queryOperation, false, () => {});
    await utils.fetchAll(queryOperation);
    await queryOperation.close();

    const result = utils.getResult(queryOperation).getValue();
    console.table(result);

    await session.close();
    await client.close();
}).catch(error => {
    console.log(error);
});
```

For more details see: [Getting Started](docs/readme.md) 

## Test

Unit tests:

```bash
npm run test
```

e2e tests:

```bash
npm run e2e
```

Before running e2e tests, create local [configuration file](tests/e2e/utils/config.js)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

## Issues

If you find some issues, feel free to create an issue or send a pull request.

## License
 
[Apache License 2.0](LICENSE)
