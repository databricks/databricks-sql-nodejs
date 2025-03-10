import os from 'os';
import packageVersion from '../version';

const productName = 'NodejsDatabricksSqlConnector';

function getNodeVersion(): string {
  return `Node.js ${process.versions.node}`;
}

function getOperatingSystemVersion(): string {
  return `${os.type()} ${os.release()}`;
}

function redactInternalToken(userAgentEntry: string): string {
  const internalTokenPrefixes = ['dkea', 'dskea', 'dapi', 'dsapi', 'dose'];
  for (const prefix of internalTokenPrefixes) {
    if (userAgentEntry.startsWith(prefix)) {
      return '<REDACTED>';
    }
  }
  return userAgentEntry;
}

export default function buildUserAgentString(userAgentEntry?: string): string {
  if (userAgentEntry) {
    userAgentEntry = redactInternalToken(userAgentEntry);
  }

  const extra = [userAgentEntry, getNodeVersion(), getOperatingSystemVersion()].filter(Boolean);
  return `${productName}/${packageVersion} (${extra.join('; ')})`;
}
