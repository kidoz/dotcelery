#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
rid="${1:-$(dotnet --info | awk '$1 == "RID:" { print $2 }')}"
if [ -z "${rid}" ]; then
  echo "Cannot determine the host runtime identifier. Pass it as the first argument." >&2
  exit 1
fi

project="samples/DotCelery.AotSmoke"
executable="DotCelery.AotSmoke"
if [[ "${rid}" == win-* ]]; then
  executable+=".exe"
fi
for framework in net10.0 net11.0; do
  dotnet publish "${project}" --configuration Release --framework "${framework}" --runtime "${rid}"
  "${project}/bin/Release/${framework}/${rid}/publish/${executable}"
done
