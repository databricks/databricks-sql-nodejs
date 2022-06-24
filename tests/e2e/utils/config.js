let overrides = {};
try {
    overrides = require('./config.local');
} catch (e) {
}

// Create file named `config.local.js` in the same directory and override config there
module.exports = {
    // Where to log: CONSOLE, FILE, QUIET
    logger: 'FILE',
    // Host: like ****.cloud.databricks.com
    host: undefined,
    // API path: /sql/1.0/endpoints/****************
    path: undefined,
    // Access token: dapi********************************
    token: undefined,
    // Catalog and database to use for testing; specify both or leave array empty to use defaults
    database: [],
    ...overrides,
};
