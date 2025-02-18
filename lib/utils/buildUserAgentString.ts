import os from 'os';
import packageVersion from '../version';

const productName = 'NodejsDatabricksSqlConnector';

function getNodeVersion(): string {
  return `Node.js ${process.versions.node}`;
}

function getOperatingSystemVersion(): string {
  return `${os.type()} ${os.release()}`;
}

export default function buildUserAgentString(userAgentHeader?: string): string {
  const extra = [userAgentHeader, getNodeVersion(), getOperatingSystemVersion()].filter(Boolean);
  return `${productName}/${packageVersion} (${extra.join('; ')})`;
}
