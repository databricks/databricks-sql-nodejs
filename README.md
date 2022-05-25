# Hive Driver

## Description

Databricks SQL Driver is a JavaScript driver for connection to [Databricks SQL](https://databricks.com/product/databricks-sql) via [Thrift API](https://github.com/apache/hive/blob/master/service-rpc/if/TCLIService.thrift).

## Installation

```bash
npm i databricks-sql-node
```

## Usage

[examples/usage.js](examples/usage.js)
```javascript
const driver = require('databricks-sql-node');
const { TCLIService, TCLIService_types } = driver.thrift;
const client = new driver.DBSQLClient(
    TCLIService,
    TCLIService_types
);

client.connect({
    host: '********.databricks.com',
    path: '/sql/1.0/endpoints/****************',
    token: 'dapi********************************',
}).then(async client => {
    const session = await client.openSession();
    const response = await session.getInfo(
        TCLIService_types.TGetInfoType.CLI_DBMS_VER
    );

    console.log(response.getValue());

    await session.close();
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
 
[MIT License](LICENSE)
