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
import { tableFromIPC, Type as ArrowType } from 'apache-arrow';
import { existsSync } from 'fs';
import { resolve as resolvePath } from 'path';
import { createRequire } from 'module';

// Prerequisites for the live e2e:
//   1. `DATABRICKS_PECOTESTING_*` env vars set (host, http_path, token)
//   2. `yarn build:native` has produced `native/sea/index.linux-x64-gnu.node`
//
// SeaNativeLoader.ts does a module-level `const native =
// require('../../native/sea/index.js')` which crashes file evaluation
// (MODULE_NOT_FOUND) when the `.node` artifact is absent, BEFORE mocha
// can run the `before()` skip-gate. So we (a) verify both prereqs from
// the suite's `before()` and skip-and-explain, and (b) defer the
// `getSeaNative` import to inside the test bodies via a synchronous
// import-on-demand. DA round-1 H1 fixup.

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
      return;
    }
    // Verify the native artifact exists before any test in the suite
    // attempts to import `getSeaNative` from SeaNativeLoader (whose
    // module-level require would crash the whole file load on
    // MODULE_NOT_FOUND). Skip-with-message so a developer sees the
    // actionable instruction instead of a cryptic crash.
    //
    // Use `process.cwd()` instead of `__dirname` because mocha may
    // reparse this file as ESM (MODULE_TYPELESS_PACKAGE_JSON path)
    // where `__dirname` is undefined. mocha always runs with cwd at
    // the package root, so this resolves consistently.
    const nodeArtifact = resolvePath(
      process.cwd(),
      'native/sea/index.linux-x64-gnu.node',
    );
    if (!existsSync(nodeArtifact)) {
      // eslint-disable-next-line no-console
      console.warn(
        `[sea complex-types e2e] skipping: native binary not built. ` +
          `Run \`yarn build:native\` (or set DATABRICKS_SQL_KERNEL_REPO + yarn build:native) first.`,
      );
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  // Build a `require` function from this module's URL so the call
  // works under both CJS and ESM reparse paths. mocha 11+ may reparse
  // ts-node-emitted files as ESM (MODULE_TYPELESS_PACKAGE_JSON), in
  // which case the bare `require` symbol is undefined.
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const requireFromHere = createRequire(import.meta.url);

  function loadBinding(): NativeBinding {
    return requireFromHere('../../../native/sea/index.js') as NativeBinding;
  }

  it('ARRAY / MAP / STRUCT come back as native Arrow shapes', async () => {
    const binding = loadBinding();
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

      // Strict typeId checks (DA round-1 M2 fixup — prior regex
      // assertions matched too permissively, e.g. `/List/i` would
      // accept LargeList or FixedSizeList too. Arrow `Type` is a
      // numeric enum from arrow-js; comparing typeId is stable
      // across arrow-js minor versions and a structural match.
      //
      // The server returns ARRAY columns as Arrow `List` (typeId 12),
      // MAP as Arrow `Map` (typeId 17), STRUCT as Arrow `Struct`
      // (typeId 13). Nested ARRAY<STRUCT> is `List` whose child is
      // `Struct`.
      expect(arrField!.type.typeId).to.equal(
        ArrowType.List,
        `c_arr typeId should be List(${ArrowType.List}), got ${arrField!.type.typeId} (${arrField!.type})`,
      );
      expect(mapField!.type.typeId).to.equal(
        ArrowType.Map,
        `c_map typeId should be Map(${ArrowType.Map}), got ${mapField!.type.typeId} (${mapField!.type})`,
      );
      expect(structField!.type.typeId).to.equal(
        ArrowType.Struct,
        `c_struct typeId should be Struct(${ArrowType.Struct}), got ${structField!.type.typeId} (${structField!.type})`,
      );
      expect(arrStructField!.type.typeId).to.equal(
        ArrowType.List,
        `c_arr_struct typeId should be List(${ArrowType.List}), got ${arrStructField!.type.typeId} (${arrStructField!.type})`,
      );

      // Nested-structural check: c_arr_struct should be List<Struct>.
      // Drilling into `children[0].type.typeId` catches a regression
      // where the kernel might wrap a Struct in something else (e.g.
      // FixedSizeList).
      const arrStructChildType = arrStructField!.type.children[0].type;
      expect(arrStructChildType.typeId).to.equal(
        ArrowType.Struct,
        `c_arr_struct child typeId should be Struct(${ArrowType.Struct}), got ${arrStructChildType.typeId}`,
      );

      // Negative assertion: NONE of the complex columns should be Utf8
      // — that would indicate `complex_types_as_json` was inadvertently
      // enabled on the kernel side. Using typeId here too rather than
      // a string regex.
      for (const f of [arrField!, mapField!, structField!, arrStructField!]) {
        expect(f.type.typeId).to.not.equal(
          ArrowType.Utf8,
          `${f.name} must not be a JSON string column (Utf8 typeId)`,
        );
      }

      // Row-count sanity: SELECT-of-literals yields exactly one row.
      expect(table.numRows).to.equal(1, 'literal SELECT should yield one row');
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
