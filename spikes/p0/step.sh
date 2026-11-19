#!/bin/sh
# Fill the placeholders of a spike manifest and apply it ("-" reads the manifest from stdin).
#   RUN_ID=p0 ./step.sh anchor.yaml
#   RUN_ID=p0 ./step.sh step-membench.yaml <node name>
#   RUN_ID=p0 TASK=membench ./step.sh step-flush.yaml <node name>
set -eu
file=${1:?usage: step.sh MANIFEST|- [NODE_NAME]}
node=${2:-}
manifest=$(cat "$file")
if echo "$manifest" | grep -q __NODE_NAME__ && [ -z "$node" ]; then
  echo "step.sh: this manifest needs a node name" >&2
  exit 1
fi
echo "$manifest" |
  sed -e "s/__RUN_ID__/${RUN_ID:-p0}/g" -e "s/__NODE_NAME__/$node/g" -e "s/__TASK__/${TASK:-}/g" |
  kubectl apply -f -
