let overrides = {};
try {
  overrides = require('./config.local');
} catch (e) {}

const catalog = process.env.E2E_CATALOG || undefined;
const schema = process.env.E2E_SCHEMA || undefined;

// Create file named `config.local.js` in the same directory and override config there
module.exports = {
  // Where to log: CONSOLE, FILE, QUIET
  logger: 'CONSOLE',
  // Host, like ****.cloud.databricks.com
  host: process.env.E2E_HOST,
  // API path: /sql/2.0/warehouses/****************
  path: process.env.E2E_PATH,
  // Access token: dapi********************************
  token: process.env.E2E_ACCESS_TOKEN,
  // Catalog and database to use for testing; specify both or leave array empty to use defaults
  database: catalog || schema ? [catalog, schema] : [],
  // Volume to use for testing
  volume: process.env.E2E_VOLUME,
  // Suffix used for tables that will be created during tests
  tableSuffix: process.env.E2E_TABLE_SUFFIX,
  ...overrides,
};
