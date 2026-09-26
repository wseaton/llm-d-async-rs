#!/bin/sh
# Local ports scenario.py expects: vLLM 18000, gateway 18081, processor API 18080 and metrics 19090, EPP metrics 19091.
# kubectl port-forward outlives a replaced pod, so each forward is restarted when its health URL stops answering.
K="kubectl --context ${KUBE_CONTEXT:-coreweave-waldorf} -n ${NAMESPACE:-weaton-dev}"
# forward <health url> <target> <ports...>
forward() {
  health=$1
  shift
  while true; do
    $K port-forward "$@" >/dev/null 2>&1 &
    pid=$!
    sleep 3
    while kill -0 $pid 2>/dev/null && curl -s -m3 -o /dev/null "$health"; do
      sleep 5
    done
    kill $pid 2>/dev/null
    wait $pid 2>/dev/null
    sleep 1
  done
}
forward http://localhost:18000/health svc/evict-vllm 18000:8000 &
forward http://localhost:19091/metrics svc/evict-epp 18081:80 19091:9090 &
forward http://localhost:19090/metrics deploy/evict-processor 18080:8080 19090:9090 &
trap 'kill 0' INT TERM
wait
