#!/usr/bin/env bash

set -euo pipefail

driver_repo=$(pwd)
kernel_repo=${DATABRICKS_SQL_KERNEL_REPO:-../../databricks-sql-kernel}
napi_dir="${kernel_repo}/napi"
kernel_package_version=$(
  node -e '
    const { optionalDependencies = {} } = require(process.argv[1]);
    const versions = [...new Set(
      Object.entries(optionalDependencies)
        .filter(([name]) => name.startsWith("@databricks/databricks-sql-kernel-"))
        .map(([, version]) => version),
    )];

    if (versions.length !== 1) {
      throw new Error(
        `Expected one pinned kernel package version, found: ${versions.join(", ") || "none"}`,
      );
    }

    process.stdout.write(versions[0]);
  ' "${driver_repo}/package.json"
)
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

build_profile=${BUILD_PROFILE---release}

cd "${napi_dir}"
# napi-rs normally embeds the kernel source manifest version in index.js. The
# source can advance before its native packages are published, so generate the
# loader guard from the version the driver actually installs instead.
if [[ -n "${build_profile//[[:space:]]/}" ]]; then
  read -r -a build_profile_args <<< "${build_profile}"
  npm_new_version="${kernel_package_version}" "${cli[@]}" build --platform "${build_profile_args[@]}"
else
  npm_new_version="${kernel_package_version}" "${cli[@]}" build --platform
fi
cp index.* "${driver_repo}/native/kernel/"
