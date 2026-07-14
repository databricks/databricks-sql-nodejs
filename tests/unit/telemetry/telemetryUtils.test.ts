/**
 * Copyright (c) 2025 Databricks Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

import { expect } from 'chai';
import { buildTelemetryUrl, redactSensitive, sanitizeProcessName } from '../../../lib/telemetry/telemetryUtils';

describe('buildTelemetryUrl', () => {
  describe('valid hosts', () => {
    it('returns https URL for a bare host', () => {
      expect(buildTelemetryUrl('myws.cloud.databricks.com', '/telemetry-ext')).to.equal(
        'https://myws.cloud.databricks.com/telemetry-ext',
      );
    });

    it('strips a leading https:// prefix', () => {
      expect(buildTelemetryUrl('https://myws.cloud.databricks.com', '/telemetry-ext')).to.equal(
        'https://myws.cloud.databricks.com/telemetry-ext',
      );
    });

    it('strips a leading http:// prefix and upgrades to https', () => {
      expect(buildTelemetryUrl('http://myws.cloud.databricks.com', '/telemetry-ext')).to.equal(
        'https://myws.cloud.databricks.com/telemetry-ext',
      );
    });

    it('strips trailing slashes', () => {
      expect(buildTelemetryUrl('myws.cloud.databricks.com///', '/telemetry-ext')).to.equal(
        'https://myws.cloud.databricks.com/telemetry-ext',
      );
    });

    it('accepts an explicit default port and normalises it', () => {
      // `new URL` strips :443 for https; we rely on that normalisation.
      expect(buildTelemetryUrl('myws.cloud.databricks.com:443', '/x')).to.equal('https://myws.cloud.databricks.com/x');
    });

    it('accepts a non-default port and preserves it', () => {
      expect(buildTelemetryUrl('myws.cloud.databricks.com:8443', '/x')).to.equal(
        'https://myws.cloud.databricks.com:8443/x',
      );
    });
  });

  describe('SSRF / redirection rejections', () => {
    it('rejects protocol-relative prefix', () => {
      expect(buildTelemetryUrl('//attacker.com', '/telemetry-ext')).to.equal(null);
    });

    it('rejects zero-width space inside host', () => {
      // `legit.com\u200battacker.com` would otherwise collapse to
      // `legit.comattacker.com` inside `new URL`.
      expect(buildTelemetryUrl('legit.com\u200battacker.com', '/telemetry-ext')).to.equal(null);
    });

    it('rejects BOM inside host', () => {
      expect(buildTelemetryUrl('legit.com\ufeffattacker.com', '/telemetry-ext')).to.equal(null);
    });

    it('rejects userinfo', () => {
      expect(buildTelemetryUrl('user:pass@attacker.com', '/telemetry-ext')).to.equal(null);
    });

    it('rejects CR in host', () => {
      expect(buildTelemetryUrl('legit.com\r\nInjected: header', '/x')).to.equal(null);
    });

    it('rejects LF in host', () => {
      expect(buildTelemetryUrl('legit.com\nInjected: header', '/x')).to.equal(null);
    });

    it('rejects tab in host', () => {
      expect(buildTelemetryUrl('legit.com\tbad', '/x')).to.equal(null);
    });

    it('rejects path appended to host', () => {
      expect(buildTelemetryUrl('legit.com/evil', '/telemetry-ext')).to.equal(null);
    });

    it('rejects query appended to host', () => {
      expect(buildTelemetryUrl('legit.com?x=1', '/telemetry-ext')).to.equal(null);
    });

    it('rejects fragment appended to host', () => {
      expect(buildTelemetryUrl('legit.com#frag', '/telemetry-ext')).to.equal(null);
    });

    it('rejects backslash in host', () => {
      expect(buildTelemetryUrl('legit.com\\evil', '/x')).to.equal(null);
    });

    it('rejects at-sign in host', () => {
      expect(buildTelemetryUrl('a@b.com', '/x')).to.equal(null);
    });

    it('rejects empty host', () => {
      expect(buildTelemetryUrl('', '/x')).to.equal(null);
    });

    it('rejects only-slashes host', () => {
      expect(buildTelemetryUrl('///', '/x')).to.equal(null);
    });
  });

  describe('deny-listed hosts', () => {
    it('rejects IPv4 loopback', () => {
      expect(buildTelemetryUrl('127.0.0.1', '/telemetry-ext')).to.equal(null);
      expect(buildTelemetryUrl('127.1.2.3', '/telemetry-ext')).to.equal(null);
    });

    it('rejects 0.0.0.0', () => {
      expect(buildTelemetryUrl('0.0.0.0', '/telemetry-ext')).to.equal(null);
    });

    it('rejects RFC1918 10.0.0.0/8', () => {
      expect(buildTelemetryUrl('10.0.0.1', '/telemetry-ext')).to.equal(null);
    });

    it('rejects RFC1918 192.168/16', () => {
      expect(buildTelemetryUrl('192.168.1.1', '/telemetry-ext')).to.equal(null);
    });

    it('rejects RFC1918 172.16-31', () => {
      expect(buildTelemetryUrl('172.16.0.1', '/telemetry-ext')).to.equal(null);
      expect(buildTelemetryUrl('172.31.255.254', '/telemetry-ext')).to.equal(null);
    });

    it('accepts 172.32 (outside RFC1918)', () => {
      expect(buildTelemetryUrl('172.32.0.1', '/telemetry-ext')).to.equal('https://172.32.0.1/telemetry-ext');
    });

    it('rejects AWS IMDS', () => {
      expect(buildTelemetryUrl('169.254.169.254', '/telemetry-ext')).to.equal(null);
    });

    it('rejects GCP metadata', () => {
      expect(buildTelemetryUrl('metadata.google.internal', '/telemetry-ext')).to.equal(null);
      expect(buildTelemetryUrl('METADATA.GOOGLE.INTERNAL', '/telemetry-ext')).to.equal(null);
    });

    it('rejects Azure metadata', () => {
      expect(buildTelemetryUrl('metadata.azure.com', '/telemetry-ext')).to.equal(null);
    });

    it('rejects localhost', () => {
      expect(buildTelemetryUrl('localhost', '/telemetry-ext')).to.equal(null);
      expect(buildTelemetryUrl('LocalHost', '/telemetry-ext')).to.equal(null);
    });
  });
});

describe('redactSensitive', () => {
  it('returns empty string for undefined', () => {
    expect(redactSensitive(undefined)).to.equal('');
  });

  it('returns empty string for empty input', () => {
    expect(redactSensitive('')).to.equal('');
  });

  it('redacts Bearer tokens', () => {
    const redacted = redactSensitive('Authorization: Bearer abc.def.ghi-jkl');
    expect(redacted).to.equal('Authorization: Bearer <REDACTED>');
  });

  it('redacts multiple Bearer tokens in one string', () => {
    const redacted = redactSensitive('first Bearer abc second Bearer xyz');
    expect(redacted).to.equal('first Bearer <REDACTED> second Bearer <REDACTED>');
  });

  it('redacts Basic auth', () => {
    expect(redactSensitive('Authorization: Basic dXNlcjpwYXNz')).to.equal('Authorization: Basic <REDACTED>');
  });

  it('redacts URL-embedded credentials', () => {
    expect(redactSensitive('fetch https://user:pass@legit.com/api')).to.equal('fetch https://<REDACTED>@legit.com/api');
  });

  it('redacts Databricks PAT (dapi)', () => {
    expect(redactSensitive('token is dapi0123456789abcdef01')).to.equal('token is <REDACTED>');
  });

  it('redacts Databricks PAT (dkea, dskea, dsapi, dose)', () => {
    for (const prefix of ['dkea', 'dskea', 'dsapi', 'dose']) {
      expect(redactSensitive(`tok ${prefix}0123456789abcdef`)).to.equal('tok <REDACTED>');
    }
  });

  it('redacts realistic JWT', () => {
    // This is NOT a real token — it's a synthetic JWT-shaped string built
    // from harmless segments purely to exercise the regex. Constructed by
    // string concatenation so the assembled token never appears as a
    // source literal (otherwise pre-commit secret scanners, rightly, flag
    // the test file itself).
    const header = `${'eyJ'}hbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9`;
    const payload = `${'eyJ'}zdWIiOiJ0ZXN0LXN1YmplY3QifQ`;
    const signature = 'Ab-123_xyz456_abcDEF789';
    const jwt = `${header}.${payload}.${signature}`;
    expect(redactSensitive(`Authorization: ${jwt}`)).to.include('<REDACTED_JWT>');
  });

  it('redacts JSON-quoted access_token', () => {
    expect(redactSensitive('{"access_token":"eyJabc.def.ghi"}')).to.equal('{"access_token":"<REDACTED>"}');
  });

  it('redacts JSON-quoted client_secret', () => {
    expect(redactSensitive('body={"client_id":"abc","client_secret":"xyz"}')).to.include(
      '"client_secret":"<REDACTED>"',
    );
  });

  it('redacts JSON-quoted refresh_token, id_token, password, api_key', () => {
    for (const key of ['refresh_token', 'id_token', 'password', 'api_key', 'apikey']) {
      expect(redactSensitive(`{"${key}":"x"}`)).to.equal(`{"${key}":"<REDACTED>"}`);
    }
  });

  it('redacts form-encoded token= style secrets', () => {
    expect(redactSensitive('post body=client_secret=xyz&token=abc&password=hunter2')).to.equal(
      'post body=client_secret=<REDACTED>&token=<REDACTED>&password=<REDACTED>',
    );
  });

  it('caps long input with truncation marker', () => {
    const long = `${'x'.repeat(3000)}Bearer abc`;
    const redacted = redactSensitive(long, 2048);
    expect(redacted.length).to.be.lessThan(long.length);
    expect(redacted).to.include('…[truncated]');
  });

  it('applies redaction again after truncation', () => {
    // Secret appears in the tail; first-pass redacts, then truncation, then
    // the cap-time second pass catches anything missed.
    const input = `${'x'.repeat(3000)}Bearer leaked-token`;
    const redacted = redactSensitive(input, 50);
    expect(redacted).to.not.include('leaked-token');
  });
});

describe('sanitizeProcessName', () => {
  it('returns empty string for undefined', () => {
    expect(sanitizeProcessName(undefined)).to.equal('');
  });

  it('returns empty string for whitespace-only', () => {
    expect(sanitizeProcessName('   ')).to.equal('');
  });

  it('strips absolute path', () => {
    expect(sanitizeProcessName('/home/alice/worker.js')).to.equal('worker.js');
  });

  it('strips Windows path', () => {
    expect(sanitizeProcessName('C:\\Users\\bob\\worker.js')).to.equal('worker.js');
  });

  it('returns basename unchanged when no path', () => {
    expect(sanitizeProcessName('worker.js')).to.equal('worker.js');
  });

  it('drops argv tail (whitespace-separated)', () => {
    expect(sanitizeProcessName('node --db-password=secret app.js')).to.equal('node');
  });

  it('drops argv tail after full path', () => {
    expect(sanitizeProcessName('/usr/bin/node --token=abc app.js')).to.equal('node');
  });

  it('preserves basename-only input without spaces', () => {
    expect(sanitizeProcessName('my-worker')).to.equal('my-worker');
  });
});
