#!/usr/bin/env node
// Runs every category script in order and prints a combined results table.
// Usage: node scripts/run-all.js   (from the project root, with creds sourced)
const { execFileSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const dir = __dirname;
const files = fs.readdirSync(dir)
  .filter((f) => /^\d\d-.*\.js$/.test(f))
  .sort();

const results = [];
for (const f of files) {
  let out = '';
  try {
    out = execFileSync('node', [path.join(dir, f)], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  } catch (e) {
    out = (e.stdout || '') + (e.stderr || '');
  }
  for (const line of out.split('\n')) {
    if (/^(PASS|FAIL|SKIP) \|/.test(line)) results.push(line);
  }
}

console.log('\n================ RESULTS ================');
results.forEach((r) => console.log(r));
const pass = results.filter((r) => r.startsWith('PASS')).length;
const fail = results.filter((r) => r.startsWith('FAIL')).length;
const skip = results.filter((r) => r.startsWith('SKIP')).length;
console.log(`\nTOTAL: ${pass} pass, ${fail} fail, ${skip} skip`);
process.exit(fail > 0 ? 1 : 0);
