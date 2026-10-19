#!/usr/bin/env bash
# Verifies the packed packages the way a consumer uses them: restores them from the local feed
# into eng/PackageSmoke and runs a task end to end, including the generator from the analyzer
# package. Fails when a package is missing, its dependencies do not resolve, or its analyzer
# assets are not applied.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
packages_dir="${1:-artifacts/packages}"

if [ ! -d "${packages_dir}" ]; then
  echo "Package directory '${packages_dir}' does not exist." >&2
  exit 1
fi

packages_dir="$(cd "${packages_dir}" && pwd)"
core_package="$(find "${packages_dir}" -maxdepth 1 -name 'DotCelery.Core.*.nupkg' -print -quit)"
if [ -z "${core_package}" ]; then
  echo "No DotCelery.Core package found in '${packages_dir}'." >&2
  exit 1
fi

version="$(basename "${core_package}" | sed -E 's/^DotCelery\.Core\.(.*)\.nupkg$/\1/')"
if [ -z "${version}" ] || [ "${version}" = "$(basename "${core_package}")" ]; then
  echo "Cannot derive the package version from '${core_package}'." >&2
  exit 1
fi

expected="$(grep -vE '^#|^[[:space:]]*$' "${repo_root}/eng/packages.txt" | sort)"
actual="$(
  find "${packages_dir}" -maxdepth 1 -name '*.nupkg' -print | while IFS= read -r package; do
    name="$(basename "${package}")"
    printf '%s\n' "${name%.${version}.nupkg}"
  done | sort
)"

if [ "${expected}" != "${actual}" ]; then
  echo "The packed package set does not match eng/packages.txt:" >&2
  diff <(printf '%s\n' "${expected}") <(printf '%s\n' "${actual}") >&2 || true
  exit 1
fi

echo "Verifying DotCelery ${version} from ${packages_dir}"

cd "${repo_root}"

dotnet restore eng/PackageSmoke/PackageSmoke.csproj \
  -p:DotCeleryVersion="${version}" \
  --source "${packages_dir}" \
  --source "https://api.nuget.org/v3/index.json"

dotnet run \
  --project eng/PackageSmoke/PackageSmoke.csproj \
  --configuration Release \
  --no-restore \
  -p:DotCeleryVersion="${version}"
