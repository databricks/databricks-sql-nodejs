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

export default validateConfig({
  host: process.env.E2E_HOST,
  path: process.env.E2E_PATH,
  token: process.env.E2E_ACCESS_TOKEN,
  catalog: process.env.E2E_CATALOG,
  schema: process.env.E2E_SCHEMA,
  volume: process.env.E2E_VOLUME,
  tableSuffix: process.env.E2E_TABLE_SUFFIX,
  ...loadOverrides(),
});
