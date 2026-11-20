import { execFileSync } from 'child_process';
import fs from 'fs';
import os from 'os';
import path from 'path';

// Self-signed `localhost` certificate, generated per run so no private key is committed.
// Returns `undefined` when openssl is not available.
export default function createLocalhostCertificate(): { cert: string; key: string } | undefined {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbsql-localhost-tls-'));
  try {
    execFileSync(
      'openssl',
      [
        'req',
        '-x509',
        '-newkey',
        'ec',
        '-pkeyopt',
        'ec_paramgen_curve:prime256v1',
        '-nodes',
        '-days',
        '1',
        '-subj',
        '/CN=localhost',
        '-addext',
        'subjectAltName=DNS:localhost,IP:127.0.0.1',
        '-keyout',
        path.join(dir, 'key.pem'),
        '-out',
        path.join(dir, 'cert.pem'),
      ],
      { stdio: 'ignore' },
    );
    return {
      cert: fs.readFileSync(path.join(dir, 'cert.pem'), 'utf8'),
      key: fs.readFileSync(path.join(dir, 'key.pem'), 'utf8'),
    };
  } catch {
    return undefined;
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}
