# Codex through Praxis AI and llm-d-async on the cluster

```text
codex (laptop) ──port-forward──► agents-praxis ──► agents-processor ──► agents-epp (Envoy + EPP) ──► agents-vllm
                                  Responses API      queue, resume         flow control, eviction,        GLM-4.7-Flash
                                  background mode                          precise prefix routing         render/derender
```

- Model: `zai-org/GLM-4.7-Flash` (30B-A3B MoE, reasoning and tool calls with
  vLLM's `glm47` parsers) on vLLM 0.30.0, one H200, 64k context, 16 slots.
- Router: llm-d-router v0.10.0 standalone chart, flow control with eviction,
  precise prefix routing from vLLM KV events (`router-values.yaml`).
  Objectives: `interactive` (priority 10) and `batch` (priority -1, evictable).
- Processor: one resumable queue, `agents`, rendering and derendering on the
  vLLM pods.
- Praxis AI with both patches from `../praxis-ai/`: foreground Responses
  calls go to the processor's `/v1/chat/completions` as `interactive`;
  `background: true` calls go through the processor's request API as `batch`.

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

sed -e "s#PROCESSOR_IMAGE#$PROCESSOR#" -e "s#PRAXIS_IMAGE#$PRAXIS#" stack.yaml | kubectl $CTX apply -f -
kubectl $CTX port-forward svc/agents-praxis 18125:8080
```

## Run Codex

```sh
CODEX_HOME=$PWD/../codex PRAXIS_API_KEY=x codex exec --skip-git-repo-check "<task>"
```

`../codex/config.toml` points Codex at the port-forward with the Responses wire
API and disables the hosted web search tool.

## Teardown

```sh
helm --kube-context coreweave-waldorf -n weaton-dev uninstall agents
kubectl --context coreweave-waldorf -n weaton-dev delete deploy,svc,configmap,secret \
  -l app.kubernetes.io/part-of=praxis-agents
```
