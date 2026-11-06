import { expect, AssertionError } from 'chai';
import Int64 from 'node-int64';
import { DBSQLClient, DBSQLParameter, DBSQLParameterType } from '../../lib';
import ParameterError from '../../lib/errors/ParameterError';

import config from './utils/config';

const openSession = async () => {
  const client = new DBSQLClient();

  const connection = await client.connect({
    host: config.host,
    path: config.path,
    token: config.token,
  });

  return connection.openSession({
    initialCatalog: config.catalog,
    initialSchema: config.schema,
    configuration: {
      QUERY_TAGS: 'test:e2e,driver:node',
    },
  });
};

describe('Query parameters', () => {
  it('should use named parameters', async () => {
    const session = await openSession();
    const operation = await session.executeStatement(
      `
        SELECT
          :p_null_1 AS col_null_1,
          :p_null_2 AS col_null_2,
          :p_null_3 AS col_null_3,
          :p_bool AS col_bool,
          :p_int AS col_int,
          :p_double AS col_double,
          :p_bigint_1 AS col_bigint_1,
          :p_bigint_2 AS col_bigint_2,
          :p_date AS col_date,
          :p_timestamp AS col_timestamp,
          :p_str AS col_str
      `,
      {
        namedParameters: {
          p_null_1: new DBSQLParameter({ value: undefined }),
          p_null_2: new DBSQLParameter({ value: null }),
          p_null_3: new DBSQLParameter({ type: DBSQLParameterType.VOID, value: 'Test' }),
          p_bool: new DBSQLParameter({ value: true }),
          p_int: new DBSQLParameter({ value: 1234 }),
          p_double: new DBSQLParameter({ value: 3.14 }),
          p_bigint_1: new DBSQLParameter({ value: BigInt(1234) }),
          p_bigint_2: new DBSQLParameter({ value: new Int64(1234) }),
          p_date: new DBSQLParameter({ value: new Date('2023-09-06T03:14:27.843Z'), type: DBSQLParameterType.DATE }),
          p_timestamp: new DBSQLParameter({ value: new Date('2023-09-06T03:14:27.843Z') }),
          p_str: new DBSQLParameter({ value: 'Hello' }),
        },
      },
    );
    const result = await operation.fetchAll();
    expect(result).to.deep.equal([
      {
        col_null_1: null,
        col_null_2: null,
        col_null_3: null,
        col_bool: true,
        col_int: 1234,
        col_double: 3.14,
        col_bigint_1: 1234,
        col_bigint_2: 1234,
        col_date: new Date('2023-09-06T00:00:00.000Z'),
        col_timestamp: new Date('2023-09-06T03:14:27.843Z'),
        col_str: 'Hello',
      },
    ]);
  });

  it('should accept primitives as values for named parameters', async () => {
    const session = await openSession();
    const operation = await session.executeStatement(
      `
        SELECT
          :p_null_1 AS col_null_1,
          :p_null_2 AS col_null_2,
          :p_bool AS col_bool,
          :p_int AS col_int,
          :p_double AS col_double,
          :p_bigint_1 AS col_bigint_1,
          :p_bigint_2 AS col_bigint_2,
          :p_timestamp AS col_timestamp,
          :p_str AS col_str
      `,
      {
        namedParameters: {
          p_null_1: undefined,
          p_null_2: null,
          p_bool: true,
          p_int: 1234,
          p_double: 3.14,
          p_bigint_1: BigInt(1234),
          p_bigint_2: new Int64(1234),
          p_timestamp: new Date('2023-09-06T03:14:27.843Z'),
          p_str: 'Hello',
        },
      },
    );
    const result = await operation.fetchAll();
    expect(result).to.deep.equal([
      {
        col_null_1: null,
        col_null_2: null,
        col_bool: true,
        col_int: 1234,
        col_double: 3.14,
        col_bigint_1: 1234,
        col_bigint_2: 1234,
        col_timestamp: new Date('2023-09-06T03:14:27.843Z'),
        col_str: 'Hello',
      },
    ]);
  });

  it('should use ordinal parameters', async () => {
    const session = await openSession();
    const operation = await session.executeStatement(
      `
        SELECT
          ? AS col_null_1,
          ? AS col_null_2,
          ? AS col_null_3,
          ? AS col_bool,
          ? AS col_int,
          ? AS col_double,
          ? AS col_bigint_1,
          ? AS col_bigint_2,
          ? AS col_date,
          ? AS col_timestamp,
          ? AS col_str
      `,
      {
        ordinalParameters: [
          new DBSQLParameter({ value: undefined }),
          new DBSQLParameter({ value: null }),
          new DBSQLParameter({ type: DBSQLParameterType.VOID, value: 'Test' }),
          new DBSQLParameter({ value: true }),
          new DBSQLParameter({ value: 1234 }),
          new DBSQLParameter({ value: 3.14 }),
          new DBSQLParameter({ value: BigInt(1234) }),
          new DBSQLParameter({ value: new Int64(1234) }),
          new DBSQLParameter({ value: new Date('2023-09-06T03:14:27.843Z'), type: DBSQLParameterType.DATE }),
          new DBSQLParameter({ value: new Date('2023-09-06T03:14:27.843Z') }),
          new DBSQLParameter({ value: 'Hello' }),
        ],
      },
    );
    const result = await operation.fetchAll();
    expect(result).to.deep.equal([
      {
        col_null_1: null,
        col_null_2: null,
        col_null_3: null,
        col_bool: true,
        col_int: 1234,
        col_double: 3.14,
        col_bigint_1: 1234,
        col_bigint_2: 1234,
        col_date: new Date('2023-09-06T00:00:00.000Z'),
        col_timestamp: new Date('2023-09-06T03:14:27.843Z'),
        col_str: 'Hello',
      },
    ]);
  });

  it('should accept primitives as values for ordinal parameters', async () => {
    const session = await openSession();
    const operation = await session.executeStatement(
      `
        SELECT
          ? AS col_null_1,
          ? AS col_null_2,
          ? AS col_bool,
          ? AS col_int,
          ? AS col_double,
          ? AS col_bigint_1,
          ? AS col_bigint_2,
          ? AS col_timestamp,
          ? AS col_str
      `,
      {
        ordinalParameters: [
          undefined,
          null,
          true,
          1234,
          3.14,
          BigInt(1234),
          new Int64(1234),
          new Date('2023-09-06T03:14:27.843Z'),
          'Hello',
        ],
      },
    );
    const result = await operation.fetchAll();
    expect(result).to.deep.equal([
      {
        col_null_1: null,
        col_null_2: null,
        col_bool: true,
        col_int: 1234,
        col_double: 3.14,
        col_bigint_1: 1234,
        col_bigint_2: 1234,
        col_timestamp: new Date('2023-09-06T03:14:27.843Z'),
        col_str: 'Hello',
      },
    ]);
  });

  it('should fail if both named and ordinal parameters used', async () => {
    const session = await openSession();

    try {
      await session.executeStatement(`SELECT :p, ?`, {
        namedParameters: { p: 1234 },
        ordinalParameters: ['test'],
      });
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error).to.be.instanceof(ParameterError);
    }
  });

  // https://github.com/databricks/databricks-sql-nodejs/issues/261
  describe('non-scalar parameter values (issue #261)', () => {
    const HANG_TIMEOUT_MS = 30000;

    // Run the statement end-to-end against the live warehouse and return whatever error it
    // produced. A hang (the reported bug) surfaces as a sentinel error after HANG_TIMEOUT_MS
    // instead of stalling the suite.
    async function runAndCaptureError(
      statement: string,
      options: Parameters<Awaited<ReturnType<typeof openSession>>['executeStatement']>[1],
    ): Promise<unknown> {
      const session = await openSession();
      let timer: NodeJS.Timeout | undefined;
      try {
        const run = (async () => {
          const operation = await session.executeStatement(statement, options);
          try {
            await operation.fetchAll();
          } finally {
            await operation.close().catch(() => {});
          }
        })();
        const hang = new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => reject(new Error(`executeStatement hung for more than ${HANG_TIMEOUT_MS}ms`)),
            HANG_TIMEOUT_MS,
          );
        });
        await Promise.race([run, hang]);
        return undefined;
      } catch (error) {
        return error;
      } finally {
        if (timer) clearTimeout(timer);
        // Bound cleanup too: when the bug reproduces, closing the session can hang as well.
        await Promise.race([
          session.close().catch(() => {}),
          new Promise<void>((resolve) => {
            setTimeout(resolve, 5000).unref();
          }),
        ]);
      }
    }

    it('should reject an array-of-arrays of ordinal parameters with ParameterError instead of hanging', async function () {
      this.timeout(HANG_TIMEOUT_MS + 30000);
      const error = await runAndCaptureError('SELECT ? AS c', { ordinalParameters: [[1], [2], [3]] as never });
      expect(error).to.be.instanceof(ParameterError);
      expect((error as Error).message).to.match(/array/);
    });

    it('should reject an array value for a named parameter with ParameterError', async function () {
      this.timeout(HANG_TIMEOUT_MS + 30000);
      const error = await runAndCaptureError('SELECT :p AS c', { namedParameters: { p: [1, 2] as never } });
      expect(error).to.be.instanceof(ParameterError);
      expect((error as Error).message).to.match(/array/);
    });

    it('should reject a plain-object ordinal parameter value with ParameterError', async function () {
      this.timeout(HANG_TIMEOUT_MS + 30000);
      const error = await runAndCaptureError('SELECT ? AS c', { ordinalParameters: [{ a: 1 } as never] });
      expect(error).to.be.instanceof(ParameterError);
      expect((error as Error).message).to.match(/object/);
    });
  });
});
