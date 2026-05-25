// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * End-to-end check that complex types (ARRAY / MAP / STRUCT and nested
 * combinations) flow through the SEA path as **native Arrow** shapes —
 * not JSON strings.
 *
 * Kernel default is `ResultConfig::complex_types_as_json = false`
 * (Arrow-native is the default). The kernel-side equivalent of this
 * test lives at
 * `tests/v0_execute_e2e.rs::complex_types_as_json_flag_stringifies_complex_columns`
 * and asserts the dual: both `false` (Arrow) and `true` (Utf8 JSON)
 * paths produce the expected shape.
 *
 * Matrix parity: this matches the NodeJS Thrift backend's behaviour
 * when `useArrowNativeTypes: true` (the default — see
 * `DBSQLSession.getArrowOptions` setting `complexTypesAsArrow: true`).
 *
 * Skipped when DATABRICKS_PECOTESTING_* env vars are absent. Pulls
 * credentials from the standard pecotesting set (see
 * `tests/e2e/sea/operation-lifecycle-e2e.test.ts` for the same gate).
 */

import { expect } from 'chai';
import { tableFromIPC } from 'apache-arrow';
import { getSeaNative } from '../../../lib/sea/SeaNativeLoader';

interface NativeBinding {
  openSession(opts: {
    hostName: string;
    httpPath: string;
    token: string;
  }): Promise<NativeConnection>;
}

interface NativeConnection {
  executeStatement(sql: string): Promise<NativeStatement>;
  close(): Promise<void>;
}

interface NativeStatement {
  fetchNextBatch(): Promise<{ ipcBytes: Buffer } | null>;
  schema(): Promise<{ ipcBytes: Buffer }>;
  cancel(): Promise<void>;
  close(): Promise<void>;
}

describe('SEA complex types — native Arrow default', function suite() {
  this.timeout(120_000);

  const hostName =
    process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME || process.env.E2E_HOST;
  const httpPath =
    process.env.DATABRICKS_PECOTESTING_HTTP_PATH || process.env.E2E_PATH;
  const token =
    process.env.DATABRICKS_PECOTESTING_TOKEN || process.env.E2E_ACCESS_TOKEN;

  before(function gate() {
    if (!hostName || !httpPath || !token) {
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  it('ARRAY / MAP / STRUCT come back as native Arrow shapes', async () => {
    const binding = getSeaNative() as unknown as NativeBinding;
    const connection = await binding.openSession({
      hostName: hostName as string,
      httpPath: httpPath as string,
      token: token as string,
    });

    let statement: NativeStatement | null = null;
    try {
      const sql = `SELECT
        ARRAY(1, 2, 3)                                AS c_arr,
        MAP('k1', 'v1', 'k2', 'v2')                   AS c_map,
        NAMED_STRUCT('a', 'foo', 'b', 1)              AS c_struct,
        ARRAY(NAMED_STRUCT('a', 'x', 'b', 1),
              NAMED_STRUCT('a', 'y', 'b', 2))         AS c_arr_struct`;

      statement = await connection.executeStatement(sql);
      const batchEnvelope = await statement.fetchNextBatch();
      expect(batchEnvelope).to.not.equal(null);

      const table = tableFromIPC(batchEnvelope!.ipcBytes);
      const schema = table.schema;

      // Each complex column should be a native Arrow nested type, not Utf8.
      const arrField = schema.fields.find((f) => f.name === 'c_arr');
      const mapField = schema.fields.find((f) => f.name === 'c_map');
      const structField = schema.fields.find((f) => f.name === 'c_struct');
      const arrStructField = schema.fields.find((f) => f.name === 'c_arr_struct');

      expect(arrField, 'c_arr field present').to.not.equal(undefined);
      expect(mapField, 'c_map field present').to.not.equal(undefined);
      expect(structField, 'c_struct field present').to.not.equal(undefined);
      expect(arrStructField, 'c_arr_struct field present').to.not.equal(undefined);

      // Arrow type ids per arrow-js — these are the structural checks
      // that distinguish "native Arrow" from "JSON Utf8". Arrow type
      // names are stable across arrow-js minor versions.
      expect(arrField!.type.toString()).to.match(/List/i, 'c_arr should be List');
      expect(mapField!.type.toString()).to.match(/Map|List/i, 'c_map should be Map (or List of Struct of key/value)');
      expect(structField!.type.toString()).to.match(/Struct/i, 'c_struct should be Struct');
      expect(arrStructField!.type.toString()).to.match(/List/i, 'c_arr_struct should be List of Struct');

      // Sanity-check: NONE of the complex columns should be Utf8 — that
      // would indicate complex_types_as_json was inadvertently enabled.
      for (const f of [arrField!, mapField!, structField!, arrStructField!]) {
        expect(f.type.toString()).to.not.match(
          /^Utf8$/,
          `${f.name} must not be a JSON string column`,
        );
      }
    } finally {
      if (statement !== null) {
        try {
          await statement.close();
        } catch (_) {
          // best-effort cleanup
        }
      }
      await connection.close();
    }
  });
});
