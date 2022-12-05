const fs = require('fs');
const path = require('path');

const packageJson = require('../package.json');

const outputPath = path.join(__dirname, '../lib/version.ts');

fs.writeFileSync(outputPath, `export default ${JSON.stringify(packageJson.version)}\n`);
