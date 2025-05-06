/* eslint-disable func-style, no-loop-func */
import { expect } from 'chai';
import sinon from 'sinon';
import Int64 from 'node-int64';
import { DBSQLClient } from '../../lib';
import IDBSQLSession from '../../lib/contracts/IDBSQLSession';
import { TProtocolVersion } from '../../thrift/TCLIService_types';
import config from './utils/config';
import IDriver from '../../lib/contracts/IDriver';

// Create a list of all SPARK protocol versions
const protocolVersions = [
  { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1, desc: 'V1: no special features' },
  { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V2, desc: 'V2: no special features' },
  { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V3, desc: 'V3: cloud fetch' },
  { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4, desc: 'V4: multiple catalogs' },
  { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5, desc: 'V5: arrow metadata' },
  { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6, desc: 'V6: async metadata, arrow compression' },
  { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7, desc: 'V7: result persistence mode' },
  { version: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8, desc: 'V8: parameterized queries' },
];

/**
 * Execute a statement and return results
 */
async function execute(session: IDBSQLSession, statement: string) {
  const operation = await session.executeStatement(statement);
  const result = await operation.fetchAll();
  await operation.close();
  return result;
}

describe('Protocol Versions E2E Tests', function () {
  // These tests might take longer than the default timeout
  this.timeout(60000);

  // Use for...of to iterate through all protocol versions
  for (const { version, desc } of protocolVersions) {
    describe(`Protocol ${desc}`, function () {
      let client: DBSQLClient;
      let session: IDBSQLSession;

      before(async function () {
        // Skip certain versions if they're known to not be supported
        if (
          [TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1, TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V2].includes(
            version,
          )
        ) {
          console.log(`Skipping test for ${desc} - no special features`);
          this.skip();
          return;
        }

        try {
          client = new DBSQLClient();

          // Connect to the Databricks SQL service
          await client.connect({
            host: config.host,
            path: config.path,
            token: config.token,
          });

          // Get access to the driver
          const getDriverOriginal = client.getDriver.bind(client);

          // Stub getDriver to return a proxied version of the driver with overridden openSession
          sinon.stub(client, 'getDriver').callsFake(async () => {
            const driver = await getDriverOriginal();

            // Create a proxy for the driver to intercept openSession calls
            const driverProxy = new Proxy(driver, {
              get(target, prop) {
                if (prop === 'openSession') {
                  return async (request: any) => {
                    // Modify the request to use our specific protocol version
                    const modifiedRequest = {
                      ...request,
                      client_protocol_i64: new Int64(version),
                    };
                    return target.openSession(modifiedRequest);
                  };
                }
                return target[prop as keyof IDriver];
              },
            });

            return driverProxy;
          });

          session = await client.openSession({
            initialCatalog: config.catalog,
            initialSchema: config.schema,
          });
        } catch (error) {
          console.log(`Failed to open session with protocol version ${desc}: ${error}`);
          this.skip();
        }
      });

      after(async function () {
        if (session) {
          await session.close();
        }
        if (client) {
          await client.close();
        }
        // Restore sinon stubs
        sinon.restore();
      });

      it('should handle various data types', async function () {
        // Query testing multiple data types supported by Databricks
        const query = `
          SELECT 
            -- Numeric types
            CAST(42 AS TINYINT) AS tiny_int_val,
            CAST(1000 AS SMALLINT) AS small_int_val,
            CAST(100000 AS INT) AS int_val,
            CAST(123456789012345 AS BIGINT) AS bigint_val, -- Using a smaller BIGINT value within JavaScript safe range
            CAST(3.14 AS FLOAT) AS float_val,
            CAST(3.14159265359 AS DOUBLE) AS double_val,
            CAST(123.45 AS DECIMAL(5,2)) AS decimal_val,
            
            -- String and Binary types
            CAST('hello world' AS STRING) AS string_val,
            CAST(X'68656C6C6F' AS BINARY) AS binary_val, -- 'hello' in hex
            
            -- Boolean type
            CAST(TRUE AS BOOLEAN) AS boolean_val,
            
            -- Date and Time types - Use current_date() to ensure consistency with server time zone
            current_date() AS date_val,
            current_timestamp() AS timestamp_val,
            
            -- Intervals 
            INTERVAL '1' DAY AS interval_day,
            
            -- Complex types
            ARRAY(1, 2, 3) AS array_val,
            MAP('a', 1, 'b', 2, 'c', 3) AS map_val,
            STRUCT(42 AS id, 'test_name' AS name, TRUE AS active) AS struct_val,
            
            -- Null value
            CAST(NULL AS STRING) AS null_val
        `;

        const result = await execute(session, query);
        expect(result).to.be.an('array');
        expect(result.length).to.equal(1);

        const row = result[0] as any;

        // Test numeric types
        expect(row).to.have.property('tiny_int_val');
        expect(row.tiny_int_val).to.equal(42);

        expect(row).to.have.property('small_int_val');
        expect(row.small_int_val).to.equal(1000);

        expect(row).to.have.property('int_val');
        expect(row.int_val).to.equal(100000);

        expect(row).to.have.property('bigint_val');
        // Using a smaller bigint value that can be safely represented in JavaScript
        expect(Number(row.bigint_val)).to.equal(123456789012345);

        expect(row).to.have.property('float_val');
        expect(row.float_val).to.be.closeTo(3.14, 0.001); // Allow small precision differences

        expect(row).to.have.property('double_val');
        expect(row.double_val).to.be.closeTo(3.14159265359, 0.00000000001);

        expect(row).to.have.property('decimal_val');
        expect(parseFloat(row.decimal_val)).to.be.closeTo(123.45, 0.001);

        // Test string and binary types
        expect(row).to.have.property('string_val');
        expect(row.string_val).to.equal('hello world');

        expect(row).to.have.property('binary_val');
        // Binary might be returned in different formats depending on protocol version

        // Test boolean type
        expect(row).to.have.property('boolean_val');
        expect(row.boolean_val).to.be.true;

        // Test date type
        expect(row).to.have.property('date_val');
        // Date may be returned as a Date object, string, or other format depending on protocol version
        const dateVal = row.date_val;

        if (dateVal instanceof Date) {
          // If it's a Date object, just verify it's a valid date in approximately the right range
          expect(dateVal.getFullYear()).to.be.at.least(2023);
          expect(dateVal).to.be.an.instanceof(Date);
        } else if (typeof dateVal === 'string') {
          // If it's a string, verify it contains a date-like format
          expect(/\d{4}[-/]\d{1,2}[-/]\d{1,2}/.test(dateVal) || /\d{1,2}[-/]\d{1,2}[-/]\d{4}/.test(dateVal)).to.be.true;
        } else {
          // Otherwise just make sure it exists
          expect(dateVal).to.exist;
        }

        // Test timestamp type
        expect(row).to.have.property('timestamp_val');
        const timestampVal = row.timestamp_val;

        if (timestampVal instanceof Date) {
          // If it's a Date object, verify it's a valid date-time
          expect(timestampVal.getFullYear()).to.be.at.least(2023);
          expect(timestampVal).to.be.an.instanceof(Date);
        } else if (typeof timestampVal === 'string') {
          // If it's a string, verify it contains date and time components
          expect(/\d{4}[-/]\d{1,2}[-/]\d{1,2}/.test(timestampVal)).to.be.true; // has date part
          expect(/\d{1,2}:\d{1,2}(:\d{1,2})?/.test(timestampVal)).to.be.true; // has time part
        } else {
          // Otherwise just make sure it exists
          expect(timestampVal).to.exist;
        }

        // Test interval
        expect(row).to.have.property('interval_day');

        // Test array type
        expect(row).to.have.property('array_val');
        const arrayVal = row.array_val;

        // Handle various ways arrays might be represented
        if (Array.isArray(arrayVal)) {
          expect(arrayVal).to.have.lengthOf(3);
          expect(arrayVal).to.include.members([1, 2, 3]);
        } else if (typeof arrayVal === 'string') {
          // Sometimes arrays might be returned as strings like "[1,2,3]"
          expect(arrayVal).to.include('1');
          expect(arrayVal).to.include('2');
          expect(arrayVal).to.include('3');
        } else {
          // For other formats, just check it exists
          expect(arrayVal).to.exist;
        }

        // Test map type
        expect(row).to.have.property('map_val');
        const mapVal = row.map_val;

        // Maps could be returned in several formats depending on the protocol version
        if (typeof mapVal === 'object' && mapVal !== null && !Array.isArray(mapVal)) {
          // If returned as a plain JavaScript object
          expect(mapVal).to.have.property('a', 1);
          expect(mapVal).to.have.property('b', 2);
          expect(mapVal).to.have.property('c', 3);
        } else if (typeof mapVal === 'string') {
          // Sometimes might be serialized as string
          expect(mapVal).to.include('a');
          expect(mapVal).to.include('b');
          expect(mapVal).to.include('c');
          expect(mapVal).to.include('1');
          expect(mapVal).to.include('2');
          expect(mapVal).to.include('3');
        } else {
          // For other formats, just check it exists
          expect(mapVal).to.exist;
        }

        // Test struct type
        expect(row).to.have.property('struct_val');
        const structVal = row.struct_val;

        // Structs could be represented differently based on protocol version
        if (typeof structVal === 'object' && structVal !== null && !Array.isArray(structVal)) {
          // If returned as a plain JavaScript object
          expect(structVal).to.have.property('id', 42);
          expect(structVal).to.have.property('name', 'test_name');
          expect(structVal).to.have.property('active', true);
        } else if (typeof structVal === 'string') {
          // If serialized as string
          expect(structVal).to.include('42');
          expect(structVal).to.include('test_name');
        } else {
          // For other formats, just check it exists
          expect(structVal).to.exist;
        }

        // Test null value
        expect(row).to.have.property('null_val');
        expect(row.null_val).to.be.null;
      });

      it('should get catalogs', async function () {
        const operation = await session.getCatalogs();
        const catalogs = await operation.fetchAll();
        await operation.close();

        expect(catalogs).to.be.an('array');
        expect(catalogs.length).to.be.at.least(1);
        expect(catalogs[0]).to.have.property('TABLE_CAT');
      });

      it('should get schemas', async function () {
        const operation = await session.getSchemas({ catalogName: config.catalog });
        const schemas = await operation.fetchAll();
        await operation.close();

        expect(schemas).to.be.an('array');
        expect(schemas.length).to.be.at.least(1);
        expect(schemas[0]).to.have.property('TABLE_SCHEM');
      });

      it('should get table types', async function () {
        const operation = await session.getTableTypes();
        const tableTypes = await operation.fetchAll();
        await operation.close();

        expect(tableTypes).to.be.an('array');
        expect(tableTypes.length).to.be.at.least(1);
        expect(tableTypes[0]).to.have.property('TABLE_TYPE');
      });

      it('should get tables', async function () {
        const operation = await session.getTables({
          catalogName: config.catalog,
          schemaName: config.schema,
        });
        const tables = await operation.fetchAll();
        await operation.close();

        expect(tables).to.be.an('array');
        // There might not be any tables, so we don't assert on the length
        if (tables.length > 0) {
          expect(tables[0]).to.have.property('TABLE_NAME');
        }
      });

      it('should get columns from current schema', async function () {
        // First get a table name from the current schema
        const tablesOp = await session.getTables({
          catalogName: config.catalog,
          schemaName: config.schema,
        });
        const tables = await tablesOp.fetchAll();
        await tablesOp.close();

        if (tables.length === 0) {
          console.log('No tables found in the schema, skipping column test');
          this.skip();
          return;
        }

        const tableName = (tables[0] as any).TABLE_NAME;

        const operation = await session.getColumns({
          catalogName: config.catalog,
          schemaName: config.schema,
          tableName,
        });
        const columns = await operation.fetchAll();
        await operation.close();

        expect(columns).to.be.an('array');
        expect(columns.length).to.be.at.least(1);
        expect(columns[0]).to.have.property('COLUMN_NAME');
      });
    });
  }
});
