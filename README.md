# Databricks SQL Node.js Driver (Beta)

## Description

Databricks SQL Driver is a JavaScript driver for connecting to [Databricks SQL](https://databricks.com/product/databricks-sql) and issuing SQL queries.

**NOTE: This Driver is Beta.**

## Requirements

- Node.js 14 or newer

## Installation

```bash
npm i @databricks/sql
```

## Usage

[examples/usage.js](examples/usage.js)
```javascript
const { DBSQLClient } = require('@databricks/sql');

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
    client.close();
}).catch(error => {
    console.log(error);
});
```

For more details see [Getting Started](docs/readme.md).

## Tests

Unit tests:

```bash
npm run test
```

End-to-end tests:

```bash
npm run e2e
```

Before running end-to-end tests, create local [configuration file](tests/e2e/utils/config.js)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

## Issues

If you find any issue, feel free to create an issue or send a pull request directly.

## License
 
[Apache License 2.0](LICENSE)
