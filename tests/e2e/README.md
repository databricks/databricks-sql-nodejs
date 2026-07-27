# End-to-End Tests

These tests run against a real Databricks SQL warehouse. They're invoked by `npm run e2e` and exercise the driver's HTTP/Thrift/Arrow path against live infrastructure.

## Environment

| Variable           | Used for                                                                 |
| ------------------ | ------------------------------------------------------------------------ |
| `E2E_HOST`         | Workspace hostname                                                       |
| `E2E_PATH`         | Warehouse HTTP path                                                      |
| `E2E_ACCESS_TOKEN` | PAT for auth                                                             |
| `E2E_TABLE_SUFFIX` | Suffix appended to per-test table names so concurrent runs don't collide |
| `E2E_CATALOG`      | Catalog (default: `peco`)                                                |
| `E2E_SCHEMA`       | Schema (default: `default`)                                              |
| `E2E_VOLUME`       | Volume name (default: `e2etests`)                                        |

## CI parallelism

The `e2e-test` job in `.github/workflows/main.yml` runs as a matrix across Node 20/22/24/26. All entries point at the same workspace, catalog, schema, and volume.

Per-test isolation is achieved by:

- **Tables**: all DDL in tests is templated against `${E2E_TABLE_SUFFIX}`, which in CI is `${{ github.sha }}_node${{ matrix.node-version }}`. Underscores not hyphens — SQL unquoted identifiers don't allow `-`.
- **Volume files**: `tests/e2e/staging_ingestion.test.ts` generates per-file `uuid.v4()` names. Multiple matrix entries can read/write the volume concurrently without collisions.

No test creates or drops the shared catalog/schema/volume. If you add a test that does, you'll need to suffix-unique the resource name too — verify before merging.

## Local invocation

`npm run e2e` must be run from the repo root. Some specs resolve fixture paths relative to `process.cwd()`.

## Warehouse capacity

The parallel CI matrix entries against one warehouse plus any concurrent PR runs can saturate the warehouse's session limit. If you see queue-related flakes (`session start` timeouts, request queueing delays), check the warehouse's `max_num_concurrent_runs` setting.
