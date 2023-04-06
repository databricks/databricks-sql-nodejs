# Databricks SQL Driver for NodeJS

![http://www.apache.org/licenses/LICENSE-2.0.txt](http://img.shields.io/:license-Apache%202-brightgreen.svg)
[![npm](https://img.shields.io/npm/v/@databricks/sql?color=blue&style=flat)](https://www.npmjs.com/package/@databricks/sql)
[![test](https://github.com/databricks/databricks-sql-nodejs/workflows/test/badge.svg?branch=main)](https://github.com/databricks/databricks-sql-nodejs/actions?query=workflow%3Atest+branch%3Amain)
[![coverage](https://codecov.io/gh/databricks/databricks-sql-nodejs/branch/main/graph/badge.svg)](https://codecov.io/gh/databricks/databricks-sql-nodejs)

## Description

The Databricks SQL Driver for NodeJS is a Javascript driver for applications that connect to Databricks clusters and SQL warehouses. This project is a fork of [Hive Driver](https://github.com/lenchv/hive-driver) which connects via Thrift API.

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

client
  .connect({
    host: '********.databricks.com',
    path: '/sql/2.0/warehouses/****************',
    token: 'dapi********************************',
  })
  .then(async (client) => {
    const session = await client.openSession();

    const queryOperation = await session.executeStatement('SELECT "Hello, World!"', { runAsync: true });
    const result = await queryOperation.fetchAll();
    await queryOperation.close();

    console.table(result);

    await session.close();
    await client.close();
  })
  .catch((error) => {
    console.log(error);
  });
```

## Run Tests

### Unit tests

You can run all unit tests, or specify a specific test to run:

```bash
npm test
npm test <path/to/file.test.js>
```

### e2e tests

Before running end-to-end tests, create a file named `tests/e2e/utils/config.local.js` and set the Databricks SQL connection info:

```javascript
{
    host: '***.databricks.com',
    path: '/sql/2.0/warehouses/***',
    token: 'dapi***',
    database: ['catalog', 'database'],
}
```

Then run

```bash
npm run e2e
npm run e2e <path/to/file.test.js>
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

## Issues

If you find any issues, feel free to create an issue or send a pull request directly.

## License

[Apache License 2.0](LICENSE)
