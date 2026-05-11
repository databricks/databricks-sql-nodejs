const { DBSQLClient } = require('..');

const client = new DBSQLClient();

const host = process.env.DATABRICKS_HOST;
const path = process.env.DATABRICKS_HTTP_PATH;
const token = process.env.DATABRICKS_TOKEN;

client
  .connect({ host, path, token })
  .then(async (client) => {
    // Session-level query tags: applied to every statement run on this session
    // (serialized into the session's QUERY_TAGS configuration).
    const session = await client.openSession({
      queryTags: {
        team: 'engineering',
        env: 'dev',
        driver: 'node',
      },
    });

    // Statement A: inherits session-level tags only.
    const opA = await session.executeStatement('SELECT 1 AS inherits_session_tags');
    console.log(await opA.fetchAll());
    await opA.close();

    // Statement B: statement-level query tags via executeStatement options.
    // These are passed via confOverlay as "query_tags" and apply ONLY to this statement.
    // Note: `env` here overrides the session-level `env: 'dev'` — for this statement
    // it will be `env: 'prod'`. Subsequent statements without statement-level tags
    // revert to the session-level values.
    const opB = await session.executeStatement('SELECT 2 AS has_statement_tags', {
      queryTags: {
        env: 'prod',
        request_id: 'abc-123',
        feature: 'reporting',
      },
    });
    console.log(await opB.fetchAll());
    await opB.close();

    // Statement C: demonstrates escaping of special characters (`\`, `:`, `,`)
    // in tag values, plus null/undefined values which serialize as bare keys.
    const opC = await session.executeStatement('SELECT 3 AS escaped_and_null_tags', {
      queryTags: {
        path: 'C:\\users\\me',
        note: 'hello, world',
        flag: null,
      },
    });
    console.log(await opC.fetchAll());
    await opC.close();

    await session.close();
    await client.close();
  })
  .catch((error) => {
    console.log(error);
  });
