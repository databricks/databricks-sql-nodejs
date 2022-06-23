# Getting started

## Table of Contents

1. [Foreword](#foreword) 
2. [Example](#example) \
   2.1. [Error handling](#error-handling) \
   2.2. [TCLIService and TCLIService_types](#tcliservice-and-tcliservice_types)
3. [HiveSession](#hivesession) 
4. [HiveOperation](#hiveoperation) \
   4.1. [HiveUtils](#hiveutils)
5. [Status](#status) 
6. [Finalize](#finalize)

## Foreword

The library is written using TypeScript, so the best way to get to know how it works is to look through the code [lib/](/lib/), [tests/e2e](/tests/e2e/) and [examples](/examples).

If you find any mistakes, misleading or some confusion feel free to create an issue or send a pull request.

## Example

```javascript
const { DBSQLClient } = require('@databricks/sql');

const client = new DBSQLClient();
const utils = DBSQLClient.utils;

client.connect({ 
    host: '...', 
    path: '/sql/1.0/endpoints/****************', 
    token: 'dapi********************************', 
}).then(async client => {
    const session = await client.openSession();
    
    const createTableOperation = await session.executeStatement(
        'CREATE TABLE IF NOT EXISTS pokes (foo INT, bar STRING)'
    );
    await utils.waitUntilReady(createTableOperation, false, () => {});
    await createTableOperation.close();
    
    const loadDataOperation = await session.executeStatement(
        'INSERT INTO pokes VALUES(123, "Hello, world!"'
    );
    await utils.waitUntilReady(loadDataOperation, false, () => {});
    await loadDataOperation.close();
    
    const selectDataOperation = await session.executeStatement(
        'SELECT * FROM pokes', { runAsync: true }
    );
    await utils.waitUntilReady(selectDataOperation, false, () => {});
    await utils.fetchAll(selectDataOperation);
    await selectDataOperation.close();
    
    const result = utils.getResult(selectDataOperation).getValue();
    
    console.log(JSON.stringify(result, null, '\t'));
    
    await session.close();
    await client.close();
})
.catch(error => {
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

### TCLIService and TCLIService_types

TCLIService and TCLIService_types are generated from [TCLIService.thrift](https://github.com/apache/hive/blob/master/service-rpc/if/TCLIService.thrift).

You can use the ones are provided by the driver or you can compile it on your own and provide via constructor to DBSQLClient ([details](https://thrift.apache.org/tutorial/)).

```
thrift -r --gen js TCLIService.thrift
```

TCLIService_types contains a number of constants that API uses, you do not have to know all of them, but sometimes it is useful to refer to [TCLIService.thrift](/thrift/TCLIService.thrift). Also, you may notice that most of the internal structures repeat the structures from [TCLIService.thrift](/thrift/TCLIService.thrift).

## HiveSession

After you connect to the server you should open session to start working with Hive server.

```javascript
...
const session = await client.openSession();
```

To open session you must provide [OpenSessionRequest](/lib/hive/Commands/OpenSessionCommand.ts#L20) - the only required parameter is "client_protocol", which synchronizes the version of HiveServer2 API.

Into "configuration" you may set any of the configurations that required for the session of your Hive instance.

After the session is opened you will have the [HiveSession](/lib/HiveSession.ts) instance.

Class [HiveSession](/lib/HiveSession.ts) is a facade for API that works with [SessionHandle](/lib/hive/Types/index.ts#L77).

The method you will use the most is `executeStatement`

```javascript
...
const operation = await session.executeStatement(
    'CREATE TABLE IF NOT EXISTS pokes (foo INT, bar STRING)',
    { runSync: true }
);
```

- "statement" is DDL/DML statement (CREATE TABLE, INSERT, UPDATE, SELECT, LOAD, etc.)

- [options](/lib/contracts/IHiveSession.ts#L14)

   - runAsync allows executing operation asynchronously.

   - confOverlay overrides session configuration properties.

   - timeout is the maximum time to execute an operation. It has Buffer type because timestamp in Hive has capacity 64. So for such value, you should use [node-int64](https://www.npmjs.com/package/node-int64) npm module.

To know other methods see [IHiveSession](/lib/contracts/IHiveSession.ts) and [examples/session.js](/examples/session.js).

## HiveOperation

In most cases, HiveSession methods return [HiveOperation](/lib/HiveOperation.ts), which helps you to retrieve requested data.

After you fetch the result, the operation will have [TableSchema](/lib/hive/Types/index.ts#L143) and data (Array<[RowSet](/lib/hive/Types/index.ts#L218)>).

### HiveUtils

Operation is executed asynchrnously, so before retrieving the result, you have to wait until it has finished state.

```javascript
...
const response = await operation.status();
const isReady = response.operationState === TCLIService_types.TOperationState.FINISHED_STATE;
```

Also, the result is fetched by portitions, the size of a portion you can set by method [setMaxRows()](/lib/HiveOperation.ts#L115).

```javascript
...
operation.setMaxRows(500);
const status = await operation.fetch();
```

After you fetch all data and you have schema and set of data, you can transfrom data in readable format. 

```javascript
...
const schema = operation.getSchema();
const data = operation.getData();
```

To simplify this process, you may use [HiveUtils](/lib/utils/HiveUtils.ts).

```typescript
/**
 * Executes until operation has status finished or has one of the invalid states
 * 
 * @param operation
 * @param progress flag for operation status command. If it sets true, response will include progressUpdateResponse with progress information
 * @param callback if callback specified it will be called each time the operation status response received and it will be passed as first parameter
 */
waitUntilReady(
    operation: IOperation,
    progress?: boolean,
    callback?: Function
): Promise<IOperation>

/**
 * Fetch data until operation hasMoreRows
 * 
 * @param operation
 */
fetchAll(operation: IOperation): Promise<IOperation>

/**
 * Transforms operation result
 * 
 * @param operation
 * @param resultHandler - you may specify your own handler. If not specified the result is transformed to JSON
 */
getResult(
    operation: IOperation,
    resultHandler?: IOperationResult
): IOperationResult
```

*NOTICE*

- [node-int64](https://www.npmjs.com/package/node-int64) is used for types with capacity 64

- to know how data is presented in JSON you may look at [JsonResult.test.js](/tests/unit/result/JsonResult.test.js)

For more details see [IOperation](/lib/contracts/IOperation.ts).

### Example

```javascript
const { DBSQLClient } = require('@databricks/sql');
const utils = DBSQLClient.utils;
...
await utils.waitUntilReady(
    operation,
    true,
    (stateResponse) => {
        console.log(stateResponse.taskStatus);
    }
);
await utils.fetchAll(operation);

const result = utils.getResult(operation).getValue();
```

## Status

You may notice, that most of the operations return [Status](/lib/dto/Status.ts) that helps you to determine the state of an operation. Also, status contains the error.

## Finalize

After you finish working with the operation, session or client it is better to close it, each of them has a respective method (`close()`).
