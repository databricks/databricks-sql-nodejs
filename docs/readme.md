# Getting started

## Table of Contents

- [Getting started](#getting-started)
  - [Table of Contents](#table-of-contents)
  - [Foreword](#foreword)
  - [Example](#example)
    - [Error handling](#error-handling)
  - [DBSQLSession](#dbsqlsession)
  - [DBSQLOperation](#dbsqloperation)
    - [Example](#example-1)
  - [Status](#status)
  - [Finalize](#finalize)

## Foreword

The library is written using TypeScript, so the best way to get to know how it works is to look through the code [lib/](/lib/), [tests/e2e](/tests/e2e/) and [examples](/examples).

If you find any mistakes, misleading or some confusion feel free to create an issue or send a pull request.

The token can be a workspace PAT token or an Azure AD access token. To generate an AAD token, use the command line `az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d` or use the MSAL authentication library with `scope = 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default`.

## Example

```javascript
const { DBSQLClient } = require('@databricks/sql');

const client = new DBSQLClient();

client
  .connect({
    host: '...',
    path: '/sql/1.0/endpoints/****************',
    token: 'dapi********************************',
  })
  .then(async (client) => {
    const session = await client.openSession();

    const createTableOperation = await session.executeStatement(
      'CREATE TABLE IF NOT EXISTS pokes (foo INT, bar STRING)',
    );
    await createTableOperation.fetchAll();
    await createTableOperation.close();

    const loadDataOperation = await session.executeStatement('INSERT INTO pokes VALUES(123, "Hello, world!"');
    await loadDataOperation.fetchAll();
    await loadDataOperation.close();

    const selectDataOperation = await session.executeStatement('SELECT * FROM pokes', { runAsync: true });
    const result = await selectDataOperation.fetchAll(selectDataOperation);
    await selectDataOperation.close();

    console.log(JSON.stringify(result, null, '\t'));

    await session.close();
    await client.close();
  })
  .catch((error) => {
    console.error(error);
  });
```

### Error handling

You may guess that some errors related to the network are thrown asynchronously and the driver does not maintain these cases, you should handle it on your own. The simplest way is to subscribe on "error" event:

```javascript
client.on('error', (error) => {
  // ...
});
```

## DBSQLSession

After you connect to the server you should open session to start working with server.

```javascript
...
const session = await client.openSession();
```

To open session you must provide [OpenSessionRequest](/lib/hive/Commands/OpenSessionCommand.ts#L20) - the only required parameter is "client_protocol", which synchronizes the version of HiveServer2 API.

Into "configuration" you may set any of the configurations that required for the session of your Hive instance.

After the session is opened you will have the [DBSQLSession](/lib/DBSQLSession.ts) instance.

Class [DBSQLSession](/lib/DBSQLSession.ts) is a facade for API that works with [SessionHandle](/lib/hive/Types/index.ts#L77).

The method you will use the most is `executeStatement`

```javascript
...
const operation = await session.executeStatement(
    'CREATE TABLE IF NOT EXISTS pokes (foo INT, bar STRING)',
    { runSync: true }
);
```

- "statement" is DDL/DML statement (CREATE TABLE, INSERT, UPDATE, SELECT, LOAD, etc.)

- [options](/lib/contracts/IDBSQLSession.ts#L14)

  - runAsync allows executing operation asynchronously.

  - confOverlay overrides session configuration properties.

  - timeout is the maximum time to execute an operation. It has Buffer type because timestamp in Hive has capacity 64. So for such value, you should use [node-int64](https://www.npmjs.com/package/node-int64) npm module.

To know other methods see [IDBSQLSession](/lib/contracts/IDBSQLSession.ts) and [examples/session.js](/examples/session.js).

## DBSQLOperation

In most cases, DBSQLSession methods return [DBSQLOperation](/lib/DBSQLOperation.ts), which helps you to retrieve requested data.

After you fetch the result, the operation will have [TableSchema](/lib/hive/Types/index.ts#L143) and data.

Operation is executed asynchronously, but `fetchChunk`/`fetchAll` will wait until it has finished. You can
get current status of operation any time using a dedicated method:

```javascript
...
const response = await operation.status();
const isReady = response.operationState === TCLIService_types.TOperationState.FINISHED_STATE;
```

Also, the result is fetched by portions, the size of a portion you can pass as option to `fetchChunk`/`fetchAll`.

```javascript
...
const results = await operation.fetchChunk({ maxRows: 500 });
```

Schema becomes available after you start fetching data.

```javascript
...
await operation.fetchChunk();
const schema = operation.getSchema();
```

_NOTICE_

- [node-int64](https://www.npmjs.com/package/node-int64) is used for types with capacity 64
- to know how data is presented in JSON you may look at [JsonResult.test.js](/tests/unit/result/JsonResult.test.js)

For more details see [IOperation](/lib/contracts/IOperation.ts).

### Example

```javascript
...
const result = await operation.fetchAll({
  progress: true,
  callback: (stateResponse) => {
    console.log(stateResponse.taskStatus);
  },
});
```

## Status

You may notice, that most of the operations return [Status](/lib/dto/Status.ts) that helps you to determine the state of an operation. Also, status contains the error.

## Finalize

After you finish working with the operation, session or client, it is better to close it, each of them has a respective method (`close()`).
