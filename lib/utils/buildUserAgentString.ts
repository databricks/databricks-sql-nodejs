import os from 'os';
import packageVersion from '../version';

const productName = 'NodejsDatabricksSqlConnector';

function getNodeVersion(): string {
  return `Node.js ${process.versions.node}`;
}

function getOperatingSystemVersion(): string {
  return `${os.type()} ${os.release()}`;
}

export default function buildUserAgentString(): string {
  const extra = [getNodeVersion(), getOperatingSystemVersion()].filter(Boolean);
  return `${productName}/${packageVersion} (${extra.join('; ')})`;
}
