#!/bin/bash

# This scripts runs Renovate locally to validate renovate.json5 configuration file
# It generates the folloing files:
# - `renovate.out` With the raw debug output of the run.
# - `update-proposals.json` with the list of proposed updates but INCLUDING DISABLED ones
# - `final-updated-packages.txt` with the list of packages with updates in the resulting PRs (considering disabled/enabled).
# - `final-updated-packages-details.json` the final list of proposed changes with details.
# WARNING: If the script is terminated before restoring the extension source (step 2), make sure you revert
#          the changes to renovate.json5 manually.

if [ -f "renovate.json5" ]; then
  echo "Found renovate.json5, proceeding..."
else
  echo "renovate.json5 not found, please run this script from the root of the repository."
  exit 1
fi

## (1) Check config syntax
npx --yes --package renovate -- renovate-config-validator renovate.json5 || exit 1

## (2) Temporarily change the renovate.json5 to point to the local extension source
sed -i.tmp 's/local>PeerDB-io/github>PeerDB-io/' renovate.json5 && rm -f renovate.json5.tmp

function cleanup {
  echo "Restoring renovate.json5 to point to the GitHub extension source"
  sed -i.tmp 's/github>PeerDB-io/local>PeerDB-io/' renovate.json5 && rm -f renovate.json5.tmp
}
trap cleanup EXIT

## (3) Run Renovate locally
echo "Running Renovate locally..."
LOG_LEVEL=debug npx --yes renovate --platform=local --require-config=optional --repository-cache=reset > renovate.out
renovate_exit=$?
if [ $renovate_exit -ne 0 ]; then
  echo "Renovate run failed (exit code $renovate_exit), last lines of renovate.out:"
  tail -n 100 renovate.out
  exit $renovate_exit
fi

## (4) Extract update proposals
cat renovate.out | sed '1,/packageFiles with updates/d' | sed 's/"config": {/{/' | sed '/DEBUG/,$d' | jq '.gomod[].deps[]' > update-proposals.json

## (5) Extract final updated packages
cat renovate.out | grep 'flattened updates found' | tr ',' '\n' | tr ':' '\n' > final-updated-packages.txt

## (6) Combine final updated packages details
FINAL_UPDATES_FILE="final-updated-packages-details.json"
: > ${FINAL_UPDATES_FILE}
for dep in $(cat final-updated-packages.txt | tail -n+2 | sort -u | awk '{print $1}'); do
  jq --compact-output --arg dep "$dep" '. | select(.depName == $dep)' update-proposals.json >> ${FINAL_UPDATES_FILE}
done

echo "List of packages with proposed updates:"
echo "----------------------------------------"
cat final-updated-packages.txt
