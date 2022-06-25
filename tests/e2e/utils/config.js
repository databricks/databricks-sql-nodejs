let overrides = {};
try {
    overrides = require('./config.local');
} catch (e) {
}

// Create file named `config.local.js` in the same directory and override config there
module.exports = {
    // Where to log: CONSOLE, FILE, QUIET
    logger: 'CONSOLE',
    host: '***.databricks.com',
    path: '/sql/1.0/endpoints/5***',
    token: 'dapi***',
    // Make sure your client and credential actually have CREATE permission in the database.
    database: ['catalog', 'database'],
    ...overrides,
};
