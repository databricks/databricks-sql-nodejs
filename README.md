# Databricks SQL Driver for NodeJS (Beta)

![http://www.apache.org/licenses/LICENSE-2.0.txt](http://img.shields.io/:license-Apache%202-brightgreen.svg)

## Description

The Databricks SQL Driver for NodeJS is a Javascript driver for applications that connect to Databricks clusters and SQL warehouses. This project is a fork of [Hive Driver](https://github.com/lenchv/hive-driver) which connects via Thrift API.

**NOTE: This Driver is Beta.**

## Documentation

For detailed documentation and usage examples, read the [Getting Started](docs/readme.md) guide.

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
    await client.close();
}).catch(error => {
    console.log(error);
});
```

## Run Tests

### Unit tests

```bash
npm run test
```

You can specify a specific test to run by changing `package.json`:

```json
"scripts": {
    "e2e": "mocha 'tests/e2e/utils/GetResult.test.js' --timeout=300000"
}
```

Or to run all unit tests:

```json
"scripts": {
    "e2e": "mocha 'tests/e2e/**/*.test.js' --timeout=300000"
}
```

### e2e tests


Before running end-to-end tests, copy the [sample configuration file](tests/e2e/utils/config.js) into the repository root and set the Databricks SQL connection info:

```javascript
{
    host: '***.databricks.com',
    path: '/sql/1.0/endpoints/***',
    token: 'dapi***',
    database: ['catalog', 'database'],
}
```

Then run

```bash
npm run e2e
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

## Issues

If you find any issues, feel free to create an issue or send a pull request directly.

## License
 
[Apache License 2.0](LICENSE)
