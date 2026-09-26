# Codex through Praxis AI and llm-d-async on the cluster

```text
codex (laptop) ──port-forward──► agents-praxis ─────────────────────────► agents-epp (Envoy + EPP) ──► agents-vllm
                                  Responses API  ──background──► agents-processor   flow control, eviction,        GLM-4.7-Flash
                                  background mode                queue, resume       precise prefix routing         render/derender
```

- Model: `zai-org/GLM-4.7-Flash` (30B-A3B MoE, reasoning and tool calls with
  vLLM's `glm47` parsers) on vLLM 0.30.0, one H200, 64k context, 16 slots.
- Router: llm-d-router v0.10.0 standalone chart, flow control with eviction,
  precise prefix routing from vLLM KV events (`router-values.yaml`).
  Objectives: `interactive` (priority 10) and `batch` (priority -1, evictable).
- Processor: resumable queues rendering and derendering on the vLLM pods:
  `agents` (`interactive`) straight to the EPP gateway, and `agents-coord`
  (`batch`) through the coordinator.
- Praxis AI with both patches from `../praxis-ai/`: foreground Responses
  calls go straight to the EPP gateway as `interactive`; `background: true`
  calls go through the processor's request API as `batch`.
- The llm-d coordinator (`coordinator.yaml`): `agents-coord-gw` routes requests
  with an `EPP-Profile` header to the same EPP and everything else to a
  decode-only coordinator, as llm-d-router's coordinator e2e does. The
  processor's `agents-coord` queue (`batch`) dispatches through it.

## Deploy

```sh
CTX="--context coreweave-waldorf -n weaton-dev"
# images (remote builds on waldorf)
TAG=codex-$(date +%Y%m%d%H%M)
PROCESSOR=$(buildit build quay.io/wseaton/llm-d-async-rs:$TAG -n weaton-dev \
  --request cpu=16 --request memory=32Gi | tail -1)
PRAXIS=$(cd <praxis-ai with both patches> && buildit build quay.io/wseaton/praxis-ai:$TAG -n weaton-dev \
  --build-arg PRAXIS_AI_FEATURES=full,store-sqlite --request cpu=16 --request memory=32Gi | tail -1)

# pull secret from the quay.io entry of ~/.docker/config.json only
python3 -c 'import json,os;c=json.load(open(os.path.expanduser("~/.docker/config.json")));print(json.dumps({"auths":{"quay.io":c["auths"]["quay.io"]}}))' > /tmp/quay.json
kubectl $CTX create secret generic praxis-agents-quay --type=kubernetes.io/dockerconfigjson \
  --from-file=.dockerconfigjson=/tmp/quay.json && rm /tmp/quay.json
kubectl $CTX label secret praxis-agents-quay app.kubernetes.io/part-of=praxis-agents

# router chart from llm-d-router v0.10.0
git -C ~/git/llm-d-router archive v0.10.0 config/charts | tar -x -C /tmp/router
helm dependency build /tmp/router/config/charts/llm-d-router-standalone
helm --kube-context coreweave-waldorf -n weaton-dev install agents \
  /tmp/router/config/charts/llm-d-router-standalone -f router-values.yaml

kubectl $CTX apply -f coordinator.yaml
sed -e "s#PROCESSOR_IMAGE#$PROCESSOR#" -e "s#PRAXIS_IMAGE#$PRAXIS#" stack.yaml | kubectl $CTX apply -f -
kubectl $CTX port-forward svc/agents-praxis 18125:8080
```

## Run Codex

```sh
CODEX_HOME=$PWD/../codex PRAXIS_API_KEY=x codex exec --skip-git-repo-check "<task>"
```

`../codex/config.toml` points Codex at the port-forward with the Responses wire
API and disables the hosted web search tool.

## Evict through the coordinator

From a pod in the namespace: 16 long `batch` generations through the
coordinator queue, then 8 `interactive` requests straight to the EPP gateway.

```sh
BATCH='{"model":"zai-org/GLM-4.7-Flash","messages":[{"role":"user","content":"Write a long story."}],"max_tokens":6000,"ignore_eos":true}'
DEADLINE=$(( $(date +%s) + 1200 ))
REQS=$(for i in $(seq 16); do
  printf '{"id":"evict-%s","deadline":%s,"request_queue_name":"agents-coord","endpoint":"/v1/chat/completions","payload":%s}\n' \
    "$i" "$DEADLINE" "$BATCH"
done | paste -sd, -)
curl -s -H 'content-type: application/json' -d "[$REQS]" http://agents-processor:8080/v1/requests/batch
sleep 8
for i in $(seq 8); do
  curl -s -H 'content-type: application/json' -H 'x-llm-d-inference-objective: interactive' \
    -d '{"model":"zai-org/GLM-4.7-Flash","messages":[{"role":"user","content":"Name three colors."}],"max_tokens":200}' \
    http://agents-epp:80/v1/chat/completions &
done
wait
for i in $(seq 16); do
  curl -s -X POST 'http://agents-processor:8080/v1/results/result-list/pop?wait_ms=600000'
done
```

Eviction shows as `Request evicted by flow control` in the EPP log and
`resuming interrupted generation` in the processor's; every batch response
still reports 6,000 completion tokens in its payload.

## Teardown

```sh
helm --kube-context coreweave-waldorf -n weaton-dev uninstall agents
kubectl --context coreweave-waldorf -n weaton-dev delete deploy,svc,configmap,secret \
  -l app.kubernetes.io/part-of=praxis-agents
```
