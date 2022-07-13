import fs from 'fs';
import path from 'path';
import os from 'os';
import { execSync } from 'child_process';

function getPackageVersion(): string {
  const json = JSON.parse(fs.readFileSync(path.join(__dirname, '../package.json')).toString());
  return json.version;
}

function getNodeVersion(): string {
  return `Node.js ${process.versions.node}`;
}

function getOperatingSystemCoreVersion(): string {
  return `${os.type()} ${os.release()}`;
}

function splitLines(str: string): string[] {
  return str.split(/[\r\n]/);
}

function findValue(lines: string[], prefix: string): string {
  for (let line of lines) {
    line = line.trim();
    if (line.startsWith(prefix)) {
      return line.substring(prefix.length).trim();
    }
  }
  return '';
}

function getWindowsVersion(): string {
  try {
    const info = splitLines(
      execSync('wmic os get Caption /value', {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'ignore'],
        windowsHide: true,
      }),
    );

    const result = findValue(info, 'Caption=');
    // It may be prepended with localized `Microsoft` string - let's remove it
    const n = result.indexOf('Windows');
    return n === -1 ? result : result.substring(n);
  } catch {}
  return '';
}

function getMacOSVersion(): string {
  let name = '';
  let version = '';
  try {
    name = execSync('sw_vers -productName', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
    version = execSync('sw_vers -productVersion', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
  } catch {}
  return `${name} ${version}`.trim();
}

function getLinuxVersion(): string {
  // `/etc/os-release` is a part of systemd but also available in some other distros (e.g. Debian 8)
  try {
    const info = splitLines(execSync('cat /etc/os-release', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }));

    const prettyName = findValue(info, 'PRETTY_NAME=');
    if (prettyName) {
      return prettyName;
    }

    const name = findValue(info, 'NAME=');
    if (name) {
      return `${name} ${findValue(info, 'VERSION=')}`.trim();
    }
  } catch {}

  // `lsb_release` - part of a Linux Standard Base; available in a lot of distros by default
  try {
    const result = execSync('lsb_release -d', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
    if (result) {
      return result;
    }
  } catch {}

  return '';
}

// Tries to get a user-friendly OS name and version using some OS-specific methods
function getOperatingSystemVersion(): string {
  switch (os.platform()) {
    case 'win32':
      return getWindowsVersion();
    case 'darwin':
      return getMacOSVersion();
    case 'linux':
      return getLinuxVersion();
  }
  return '';
}

export default {
  package: getPackageVersion(),
  node: getNodeVersion(),
  osCore: getOperatingSystemCoreVersion(),
  osVersion: getOperatingSystemVersion(),
};
