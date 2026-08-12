#!/usr/bin/env bash

set -euo pipefail

driver_repo=$(pwd)
kernel_repo=${DATABRICKS_SQL_KERNEL_REPO:-../../databricks-sql-kernel}
napi_dir="${kernel_repo}/napi"
napi_major=$(
  cargo metadata --format-version 1 --locked --manifest-path "${napi_dir}/Cargo.toml" |
    node -e '
      const metadata = JSON.parse(require("fs").readFileSync(0, "utf8"));
      const rootId = metadata.resolve.root ?? metadata.workspace_members[0];
      const root = metadata.resolve.nodes.find(({ id }) => id === rootId);
      const napiId = root?.deps.find(({ name }) => name === "napi")?.pkg;
      const version = metadata.packages.find(({ id }) => id === napiId)?.version;
      process.stdout.write(version?.split(".")[0] ?? "");
    '
)

# napi-rs v2 and v3 derive macros expect different CLI environment variables.
case "${napi_major}" in
  2)
    cli=(npx --yes @napi-rs/cli@2.18.4)
    ;;
  3)
    cli=(npx --yes --package @napi-rs/cli@3.8.2 napi)
    ;;
  *)
    echo "Unsupported napi-rs major version: ${napi_major:-unknown}" >&2
    exit 1
    ;;
esac

cd "${napi_dir}"
"${cli[@]}" build --platform "${BUILD_PROFILE:---release}"
cp index.* "${driver_repo}/native/kernel/"
