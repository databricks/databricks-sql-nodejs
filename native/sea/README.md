# `native/sea/` — consumer-side directory for the Rust napi binding

**The Rust binding source lives in the kernel repo** at
`databricks-sql-kernel/napi/`, as a workspace sibling of `pyo3/`.
See `databricks-sql-kernel`'s root `Cargo.toml` `[workspace] members`.

## Why

Per the architectural decision recorded in
`sea-workflow/decisions.md` (D-006), every language binding (PyO3,
napi-rs, future cgo) is a workspace member of the kernel crate. This
keeps Arrow version pinning lockstep, the path dep clean (`path = ".."`),
and CI single (`cargo build --workspace`). The pattern matches polars,
ruff, arrow-rs.

## What lives here

- `index.d.ts` — generated TypeScript declarations consumed by `lib/sea/`
- `index.linux-x64-gnu.node` (and other platform variants) — symlinked
  or copied build artifacts from the kernel workspace at run time

## How to build the binding for local dev

```bash
# From the nodejs repo root:
npm run build:native
# which delegates to the kernel workspace:
#   cd $DATABRICKS_SQL_KERNEL_REPO/napi && napi build --release
# and copies the artifact back here
```

`$DATABRICKS_SQL_KERNEL_REPO` defaults to a path published with the
release flow; for dev it points at a local checkout of
`databricks-sql-kernel`.

## How to consume in production

At release time the kernel CI publishes `@databricks/sea-native-<triple>`
npm packages with the `.node` binaries. The nodejs driver declares them
as `optionalDependencies` in `package.json`; `SeaNativeLoader.ts`
resolves the right one at runtime.
