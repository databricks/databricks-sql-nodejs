# Hive Driver

## Description

Hive Driver is a Java Script driver for connection to [Apache Hive](https://hive.apache.org/) via [Thrift API](https://github.com/apache/hive/blob/master/service-rpc/if/TCLIService.thrift).

## Installation

```bash
npm i hive-driver
```

## Usage

[examples/usage.js](examples/usage.js)
```javascript
const hive = require('hive-driver');
const { TCLIService, TCLIService_types } = hive.thrift;
const client = new hive.DBSQLClient(
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

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

## Issues

If you find some issues, feel free to create an issue or send a pull request.

## License
 
[MIT License](LICENSE)

Copyright (c) 2020 Volodymyr Liench
