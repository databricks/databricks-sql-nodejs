import fs from 'fs';
import path from 'path';
import os from 'os';

const productName = 'NodejsDatabricksSqlConnector';

export function definedOrError<T>(value: T | undefined): T {
  if (value === undefined) {
    throw new TypeError('Value is undefined');
  }
  return value;
}

function getPackageVersion(): string {
  const json = JSON.parse(fs.readFileSync(path.join(__dirname, '../package.json')).toString());
  return json.version;
}

function getNodeVersion(): string {
  return `Node.js ${process.versions.node}`;
}

function getOperatingSystemVersion(): string {
  return `${os.type()} ${os.release()}`;
}

export function buildUserAgentString(clientId?: string): string {
  const extra = [clientId, getNodeVersion(), getOperatingSystemVersion()].filter(Boolean);
  return `${productName}/${getPackageVersion()} (${extra.join('; ')})`;
}
