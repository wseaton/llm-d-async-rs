#!/bin/sh
# Usage: mode.sh true|false. Sets the processor queue's `resumable` and restarts it.
set -e
case "$1" in true|false) ;; *) echo "usage: $0 true|false" >&2; exit 2 ;; esac
K="kubectl --context ${KUBE_CONTEXT:-coreweave-waldorf} -n ${NAMESPACE:-weaton-dev}"
$K get cm evict-processor -o json \
  | python3 -c "import json,re,sys; c=json.load(sys.stdin); c['data']['transport.json']=re.sub(r'\"resumable\": (true|false)', '\"resumable\": $1', c['data']['transport.json']); print(json.dumps(c))" \
  | $K apply -f -
$K rollout restart deploy/evict-processor
$K rollout status deploy/evict-processor --timeout=120s
until curl -sf -m2 localhost:18080/v1/queues >/dev/null; do sleep 1; done
