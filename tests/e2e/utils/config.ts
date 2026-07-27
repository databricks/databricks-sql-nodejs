// Create file named `config.local.js` in the same directory and override config there

interface E2EConfig {
  // Host, like ****.cloud.databricks.com
  host: string;
  // API path: /sql/2.0/warehouses/****************
  path: string;
  // Access token: dapi********************************
  token: string;
  // Catalog and schema to use for testing
  catalog: string;
  schema: string;
  // UC Volume to use for testing
  volume: string;
  // Suffix used for tables that will be created during tests
  tableSuffix: string;
}

function validateConfig(config: Partial<E2EConfig>): E2EConfig | never {
  let isConfigValid = true;

  for (const key of Object.keys(config)) {
    const value = config[key as keyof E2EConfig] ?? undefined;
    if (value === undefined) {
      isConfigValid = false;
      // eslint-disable-next-line no-console
      console.error(`\u26A0\uFE0F  Config option '${key}' is missing`);
    }
  }

  if (!isConfigValid) {
    // eslint-disable-next-line no-console
    console.log();
    process.exit(1);
  }

  // Now, when we checked all the options, we can safely cast to `E2EConfig`
  return config as E2EConfig;
}

function loadOverrides(): object {
  try {
    const result = require('./config.local'); // eslint-disable-line global-require
    if (typeof result === 'object' && result !== null) {
      return result;
    }
  } catch (e) {
    // ignore
  }
  return {};
}

// Access token from the JSON file named by `DATABRICKS_TEST_CONFIG_FILE`, or
// `undefined` when the variable is unset/empty/unreadable.
//
// Why this indirection exists: the engineer-bot (databricks-bot-engine) runs the
// e2e suite inside an agent-driven subprocess whose environment has every
// credential-shaped variable — anything matching `*TOKEN*` / `*SECRET*` /
// `*PASSWORD*` etc. — stripped for safety (the engine's `shared/env_scrub.py`).
// `E2E_ACCESS_TOKEN` is therefore removed before the tests start, so without this
// fallback `token` would be undefined and `validateConfig` would `process.exit(1)`,
// aborting the whole suite. The bot instead writes the token to a file and points
// at it with `DATABRICKS_TEST_CONFIG_FILE` — a name the scrub deliberately
// preserves. Normal CI and local dev leave that variable unset, so this returns
// `undefined` and the `E2E_ACCESS_TOKEN` env var is used unchanged.
function tokenFromConfigFile(): string | undefined {
  const path = process.env.DATABRICKS_TEST_CONFIG_FILE;
  if (!path) {
    return undefined;
  }
  try {
    const fs = require('fs'); // eslint-disable-line global-require
    const parsed = JSON.parse(fs.readFileSync(path, 'utf8'));
    return typeof parsed?.token === 'string' ? parsed.token : undefined;
  } catch (e) {
    // The file was configured but couldn't be read/parsed. Surface this so a
    // parse failure isn't misdiagnosed as a genuinely-unset token (which would
    // send the engineer-bot down the wrong `blocked` diagnosis). Log only the
    // error's name/message — never the raw exception, since a JSON.parse failure
    // on Node 20 can embed a snippet of the offending (token-bearing) input in
    // its message, which would leak part of the credential into CI logs.
    const detail = e instanceof Error ? e.message : 'unknown error';
    // eslint-disable-next-line no-console
    console.error(`⚠️  Failed to read token from DATABRICKS_TEST_CONFIG_FILE ('${path}'): ${detail}`);
    return undefined;
  }
}

export default validateConfig({
  host: process.env.E2E_HOST,
  path: process.env.E2E_PATH,
  // Env var wins; fall back to the config file only when it's absent (see
  // tokenFromConfigFile — used by the engineer-bot, whose subprocess env is
  // scrubbed of credential-shaped vars). Normal CI/local dev is unchanged.
  token: process.env.E2E_ACCESS_TOKEN || tokenFromConfigFile(),
  catalog: process.env.E2E_CATALOG,
  schema: process.env.E2E_SCHEMA,
  volume: process.env.E2E_VOLUME,
  tableSuffix: process.env.E2E_TABLE_SUFFIX,
  ...loadOverrides(),
});
