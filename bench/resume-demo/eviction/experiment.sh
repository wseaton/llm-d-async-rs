#!/bin/sh
# Runs the same load profile under each arm:
#   noevict        flow control, no eviction
#   holdback-50/75 flow control, no eviction, priority-holdback-policy capping
#                  batch at minCeiling (2 or 3 of 4 slots)
#   restart        eviction, processor restarts evicted requests
#   resumable      eviction, processor resumes evicted requests
#   tiered         resumable, and jobs with looser deadlines name
#                  lower-priority objectives so eviction takes them first
#   affinity-none, affinity-session, affinity-precise
#                  resumable with epp/<none|session|precise>.yaml as the EPP
#                  config; the scenario sends x-session-id on every request
#                  and a per-run cache_salt so every run starts with cold
#                  prefix caches
# Needs forward.sh running and CHART pointing at llm-d-router v0.10.0
# config/charts/llm-d-router-standalone.
# SLOTS (default 4) sets EPP maxConcurrency; vLLM --max-num-seqs must match
# (stack.yaml's SLOTS placeholder). LOAD overrides the scenario arguments and
# RUNS (default runs) the directory reports go to. DRIVER names a pod
# (driver.yaml) that runs every scenario in-cluster against Service DNS
# names; reports are copied back to RUNS afterwards. MODEL fills the MODEL
# placeholder in epp/*.yaml and must match the model stack.yaml was applied with.
# Usage: CHART=... [SLOTS=32] [LOAD="..."] ./experiment.sh [reps] [arm...]
set -e
: "${CHART:?set CHART to the llm-d-router-standalone chart directory}"
REPS=${1:-3}
[ $# -gt 0 ] && shift
ARMS=${*:-floor noevict holdback-50 holdback-75 restart resumable}
SLOTS=${SLOTS:-4}
MODEL=${MODEL:-Qwen/Qwen2.5-1.5B-Instruct}
RUNS=${RUNS:-runs}
CTX=${KUBE_CONTEXT:-coreweave-waldorf}
NS=${NAMESPACE:-weaton-dev}
LOAD=${LOAD:-"--batch 20 --max-tokens 2000 --warmup 5 --bursts 4 --burst-size 6 --burst-gap 8"}
FLOOR=${FLOOR:-"--batch 0 --warmup 0 --bursts 4 --burst-size 6 --burst-gap 8"}

# router <enableEviction> [holdback minCeiling]
router() {
  values=$RUNS/values-$SLOTS-$1-${2:-static}.yaml
  sed -e "s/enableEviction: true/enableEviction: $1/" -e "s/maxConcurrency: 4$/maxConcurrency: $SLOTS/" \
    router-values.yaml > "$values"
  if [ -n "${2:-}" ]; then
    sed -i '' \
      -e "s/^    flags:$/    flags:\\
      allow-experimental-plugins: true/" \
      -e "s/^        plugins:$/        plugins:\\
        - type: priority-holdback-policy\\
          name: holdback\\
          parameters: {domain: rank, minCeiling: $2, maxCeiling: 1.0}/" \
      -e "s/^        flowControl:$/        flowControl:\\
          usageLimitPolicyPluginRef: holdback/" "$values"
  fi
  helm --kube-context "$CTX" -n "$NS" upgrade evict "$CHART" -f "$values" \
    --set-string router.epp.podAnnotations.values-sha="$(shasum "$values" | cut -c1-12)" >/dev/null
  kubectl --context "$CTX" -n "$NS" rollout status deploy/evict-epp --timeout=300s >/dev/null
  loaded=$(kubectl --context "$CTX" -n "$NS" logs deploy/evict-epp -c epp | grep -E 'Loaded raw configuration|EPP config after phase two')
  for want in "EnableEviction:$1" "\\\"maxConcurrency\\\":$SLOTS" ${2:+"\\\"minCeiling\\\":$2"}; do
    case $loaded in *"$want"*) ;; *) echo "EPP did not load $want" >&2; exit 1 ;; esac
  done
  until curl -sf -m3 localhost:19091/metrics >/dev/null && curl -sf -m3 localhost:18081/health >/dev/null; do sleep 2; done
}

# run <label> [scenario args...]
# epp_config <name> <plugin the EPP must load> [extra helm args...]
epp_config() {
  file=$RUNS/epp-$1.yaml
  sed "s#MODEL#$MODEL#g" "epp/$1.yaml" > "$file"
  want=$2
  shift 2
  helm --kube-context "$CTX" -n "$NS" upgrade evict "$CHART" -f router-values.yaml \
    --set-file router.epp.pluginsCustomConfig.eviction\\.yaml="$file" \
    --set-string router.epp.podAnnotations.values-sha="$(shasum "$file" | cut -c1-12)" "$@" >/dev/null
  kubectl --context "$CTX" -n "$NS" rollout status deploy/evict-epp --timeout=300s >/dev/null
  loaded=$(kubectl --context "$CTX" -n "$NS" logs deploy/evict-epp -c epp | grep -E 'Loaded raw configuration|EPP config after phase two')
  for w in "EnableEviction:true" "Type: $want"; do
    case $loaded in *"$w"*) ;; *) echo "EPP did not load $w" >&2; exit 1 ;; esac
  done
  until curl -sf -m3 localhost:19091/metrics >/dev/null && curl -sf -m3 localhost:18081/health >/dev/null; do sleep 2; done
}

IN_CLUSTER="--gateway http://evict-epp:80 --processor http://evict-processor:8080
  --processor-metrics http://evict-processor:9090 --epp-metrics http://evict-epp:9090
  --vllm-pods-dns evict-vllm-pods"

# run <label> [scenario args...]
run() {
  label=$1
  shift
  i=0
  while [ $i -lt "$REPS" ]; do
    i=$((i + 1))
    if [ -n "${DRIVER:-}" ]; then
      kubectl --context "$CTX" -n "$NS" cp scenario.py "$DRIVER:/work/scenario.py" >/dev/null
      kubectl --context "$CTX" -n "$NS" exec "$DRIVER" -- \
        python scenario.py --label "$label" --out "$RUNS" $IN_CLUSTER $LOAD "$@" >/dev/null 2>&1 || true
      kubectl --context "$CTX" -n "$NS" cp "$DRIVER:/work/$RUNS" "$RUNS" >/dev/null
    else
      ./scenario.py --label "$label" --out "$RUNS" $LOAD "$@" >/dev/null || true
    fi
  done
}

mkdir -p "$RUNS"
for arm in $ARMS; do
  echo "arm $arm"
  case $arm in
    floor) ./scenario.py --label floor --out "$RUNS" $FLOOR >/dev/null ;;
    noevict) router false; ./mode.sh true >/dev/null; run noevict ;;
    holdback-50) router false 0.5; ./mode.sh true >/dev/null; run holdback-50 ;;
    holdback-75) router false 0.75; ./mode.sh true >/dev/null; run holdback-75 ;;
    restart) router true; ./mode.sh false >/dev/null; run restart ;;
    resumable) router true; ./mode.sh true >/dev/null; run resumable ;;
    tiered) router true; ./mode.sh true >/dev/null; run tiered --tiered ;;
    affinity-none) epp_config none queue-scorer; ./mode.sh true >/dev/null; run affinity-none --session-id --cache-salt ;;
    affinity-session) epp_config session session-affinity-scorer --set router.epp.flags.allow-experimental-plugins=true
      ./mode.sh true >/dev/null; run affinity-session --session-id --cache-salt ;;
    affinity-precise) epp_config precise precise-prefix-cache-producer; ./mode.sh true >/dev/null; run affinity-precise --session-id --cache-salt ;;
    *) echo "unknown arm $arm" >&2; exit 2 ;;
  esac
done
./summarize.py "$RUNS"
