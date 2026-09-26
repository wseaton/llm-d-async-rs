# Flow-control eviction demo

llm-d-router v0.10.0 evicts in-flight `batch` (priority -1) requests when
`interactive` (priority 10) requests queue behind a saturated pool. The
processor resumes each evicted batch generation from its saved tokens. The
A/B flips the queue's `resumable` flag and runs the same scenario.

```
scenario.py ──batch──> processor ──x-gateway-inference-objective: batch──┐
            ──interactive (x-llm-d-inference-objective)──────────────────┤
                                                                         v
                                            Envoy ──ext_proc──> EPP (flow control, eviction)
                                              │
                                              v
                                   vLLM Qwen2.5-1.5B, --max-num-seqs 4
```

EPP config (`router-values.yaml`): `featureGates: [flowControl]`,
`flowControl.enableEviction: true`, `concurrency-detector` with
`maxConcurrency: 4` (the same as vLLM `--max-num-seqs`), and request TTL 900s.
Priority comes only from InferenceObjective CRDs, so the EPP runs with
`--pool-name` against a real InferencePool. File discovery and
`--endpoint-selector` pin every request to priority 0, and priority 0 is
never evicted. vLLM runs with `VLLM_BATCH_INVARIANT=1`, so greedy output
does not depend on batch composition and every batch result can be compared
token for token against a direct run.

## Run

```sh
# router chart from llm-d-router v0.10.0
git -C ~/git/llm-d-router archive v0.10.0 config/charts | tar -x -C /tmp/router
helm dependency build /tmp/router/config/charts/llm-d-router-standalone
helm --kube-context coreweave-waldorf -n weaton-dev install evict \
  /tmp/router/config/charts/llm-d-router-standalone -f router-values.yaml

# pull secret resume-poc-quay (quay.io entry only), then vLLM and processor
sed 's#IMAGE#quay.io/wseaton/llm-d-async-rs@sha256:<digest>#' stack.yaml \
  | kubectl --context coreweave-waldorf -n weaton-dev apply -f -

./forward.sh &   # 18000 vLLM, 18081 gateway, 18080/19090 processor, 19091 EPP metrics
CHART=/tmp/router/config/charts/llm-d-router-standalone ./experiment.sh 3
```

`experiment.sh [reps] [arm...]` runs each arm with the same load, switching
the router config with `helm upgrade` and the processor queue with
`mode.sh`, and then prints `summarize.py`'s table. `scenario.py` runs one
scenario. It computes direct baselines once and caches them in
`runs/baselines.json`. It then submits the batch, fires the interactive
bursts at the gateway, claims every result, compares each one with its
baseline, and writes `runs/<run>.json`. `batch_redone` is vLLM
`generation_tokens_total` minus interactive tokens minus
`batch × max_tokens`.

Teardown:

```sh
helm --kube-context coreweave-waldorf -n weaton-dev uninstall evict
kubectl --context coreweave-waldorf -n weaton-dev delete deploy,svc,configmap,secret \
  -l app.kubernetes.io/part-of=resume-poc
```

## Results (waldorf H200, vLLM 0.30.0, 2026-09-25)

20 batch × 2000 tokens (`ignore_eos`) through the processor. 4 bursts of 6
interactive × 256 tokens straight at the gateway, 8 s apart. 3 runs per
arm, mean (min to max).

| arm | TTFT p50 s | TTFT p95 s | batch makespan s | batch p50 s | decode redone | evictions |
|---|---|---|---|---|---|---|
| floor (no batch) | 0.146 | 1.34 | | | | |
| no eviction | 13.3 (5.6-17.3) | 19.6 (10.4-24.4) | 79.3 (75.4-81.4) | 52.2 | 0 | 0 |
| holdback, 2 of 4 slots reserved | 2.81 (1.17-3.78) | 8.34 (4.75-10.1) | 125 (120-136) | 67.6 | 0 | 0 |
| holdback, 1 of 4 slots reserved | 14.1 (8.63-17.6) | 19.6 (10.7-24.8) | 80.0 (76.9-82.8) | 51.9 | 0 | 0 |
| eviction + restart | 0.173 (0.158-0.189) | 1.34 (1.24-1.51) | 99.9 (98.9-100.4) | 71.9 | 14,100 | 19.7 |
| eviction + resume | 0.161 (0.160-0.162) | 1.26 (1.16-1.33) | 78.2 (75.2-82.7) | 60.4 | 2.7 | 16.7 |

Every batch output in every arm was token-identical to its direct run.
The 1-slot holdback row comes from a rerun. The first attempt followed
the 2-slot arm with only `minCeiling` changed, and the chart does not roll
the EPP when only `pluginsCustomConfig` changes. That attempt ran with
0.5, and its makespan (119 s) matched the 2-slot arm. `experiment.sh` now
stamps a hash of the values into a pod annotation and checks the EPP's
loaded config before each arm. The superseded runs are in `runs/suspect/`. The holdback arms use `priority-holdback-policy` (`domain: rank`,
`minCeiling` 0.5 and 0.75, eviction off). It is Alpha, so the EPP needs
`--allow-experimental-plugins`.

- Eviction is the only arm that keeps interactive TTFT at the no-load floor.
  Without it, a burst waits for batch requests to finish: 5 to 17 s at p50.
  Reserving 2 slots gets TTFT down to 2.8 s p50, but the batch takes 58%
  longer because those slots sit idle between bursts. Reserving 1 slot
  keeps batch time about the same as no eviction, and TTFT stays nearly
  as bad (14 s p50).
- With resume, batch finishes as fast as it does with no eviction (78 s vs
  79 s). Restart redoes about 14k tokens (35% of the batch's decode) and
  takes 21 s longer. The batch p50 goes from 52 s to 60 s with resume
  because evicted requests finish later, but the batch as a whole does not.
- Both eviction paths recover. A request evicted after response headers has
  its stream cut, and the processor resumes it. A request evicted before
  headers gets a 429, and the processor retries it. The EPP source sets
  `x-llm-d-request-dropped-reason: evicted` on that 429 and sends no
  `Retry-After`. Non-streamed requests on the restart queue only ever
  see the 429.
- vLLM stops within about one token of a cut: 2 to 3 tokens decoded after
  17 evictions.
- Continuations hit the prefix cache: 14,544 of 15,121 prompt tokens were
  cached on the first resumable run.
- The floor's p95 and the eviction arms' p95 are both about 1.3 s because a
  burst of 6 is larger than the 4 slots, so 2 requests wait for another
  interactive request to finish.

## Scaled: 32 slots, three jobs with deadlines

vLLM runs with `--max-num-seqs 32` and the EPP with `maxConcurrency: 32`.
The batch is three jobs of 64 requests × 1500 tokens, all submitted at
once with deadlines of 45, 75 and 100 s after submit. The processor sends
the earliest deadline first and fails a request when its deadline passes.
Interactive traffic is 15 bursts of 32 × 256 tokens, 6 s apart, which is
about a quarter of the pool's capacity. Uncontended, the jobs finish at
28, 45 and 63 s. `tiered` sends job a as `batch` (-1), b as `batch-2` (-2)
and c as `batch-3` (-3), so eviction takes the looser jobs first.

```sh
RUNS=runs-32 SLOTS=32 \
LOAD="--max-tokens 1500 --job a:64:45 --job b:64:75 --job c:64:100 --warmup 3 --bursts 15 --burst-size 32 --burst-gap 6" \
FLOOR="--batch 0 --warmup 0 --bursts 15 --burst-size 32 --burst-gap 6" \
CHART=... ./experiment.sh 3 floor noevict holdback-50 holdback-75 restart resumable tiered
```

3 runs per arm:

| arm | TTFT p50 s | TTFT p95 s | job a met | job b met | job c met | batch done s | decode redone | evictions |
|---|---|---|---|---|---|---|---|---|
| floor (no batch) | 0.159 | 0.223 | | | | | | |
| no eviction | 1.50 | 6.53 | 100% | 100% | 100% | 69.6 | 0 | 0 |
| holdback, 16 of 32 reserved | 1.12 | 4.18 | 100% | 100% | 100% | 83.9 | 0 | 0 |
| holdback, 8 of 32 reserved | 1.44 | 5.97 | 100% | 100% | 100% | 77.9 | 0 | 0 |
| eviction + restart | 0.293 | 0.451 | 0.5% | 1% | 50% | 99.3 | ~138,000 | 481 |
| eviction + resume | 0.241 | 0.439 | 99.5% | 100% | 100% | 74.0 | 92 | 355 |
| eviction + resume, tiered | 0.253 | 0.436 | 100% | 100% | 100% | 74.7 | 121 | 402 |

- Eviction is the only arm near the interactive floor, and p95 is about
  15× lower than without eviction. At this scale holdback barely improves
  TTFT and delays the batch by 8 to 14 s.
- Restart collapses. Almost every interactive request evicts a batch
  request, the restarts redo about 138k tokens (48% of the batch's
  decode), and jobs a and b almost all fail at their deadlines.
- Resume costs the batch about 4 s against no eviction. Every completed
  output was identical in every arm.
- Resume missed one job-a deadline in one run, with a result backlog of 1,
  so the claim loop did not cause it. A likely cause: a resumed request is
  dispatched again and becomes the newest in flight, and newest-first
  eviction takes it again. Tiered keeps job a out of reach while a looser
  job is in flight. Its job a p50 is 23.0 s against 26.6 s untiered, and it
  missed nothing.
- These deadlines are loose enough for no eviction and holdback to meet
  too. The tighter variant below separates them.

### Tighter deadlines: 30, 55 and 80 s

Same load, `--job a:64:30 --job b:64:55 --job c:64:80`, `RUNS=runs-32-tight`.
Restart was left out. 3 runs per arm:

| arm | TTFT p50 s | TTFT p95 s | job a met | job b met | job c met | batch done s | decode redone | evictions |
|---|---|---|---|---|---|---|---|---|
| no eviction | 1.42 | 6.48 | 100% | 100% | 100% | 69.6 | 0 | 0 |
| holdback, 16 of 32 reserved | 1.06 | 3.77 | 77.6% | 90.1% | 75% | 79.1 | n/a | 0 |
| eviction + resume | 0.239 | 0.434 | 80.7% | 85.4% | 100% | 73.5 | n/a | 343 |
| eviction + resume, tiered | 0.241 | 0.438 | 99.5% | 100% | 100% | 74.4 | 117 | 405 |

- Tiered resume is the only arm with both interactive TTFT at the floor and
  deadline hits on par with no eviction. It missed one request in 576.
- Untiered resume misses 15 to 20% of the two tighter jobs. With one batch
  band, eviction takes the newest dispatch. The processor dispatches the
  earliest deadline first, so the newest dispatch is often the most urgent
  request. The EPP cannot order by the processor's deadline:
  `edf-ordering-policy` exists, but at v0.10.0 every request's TTL is the
  default (`InitialEffectiveTTL` returns 0,
  `pkg/epp/requestcontrol/admission.go:219`). Priority tiers are the only
  way to pass urgency down.
- Holdback misses deadlines because the reserved half of the pool is idle
  between bursts.
- Across both 32-slot sets there were no wrong outputs. Every
  non-identical result was a `deadline exceeded` failure. Decode redone is
  shown only for runs where every request completed, since failed requests
  never decode their full budget.

Two harness fixes came with the scale-up. Results are claimed by 32
parallel claimers, and the report records the deepest result backlog
(`max_result_backlog`). A single sequential claimer lagged tens of seconds
behind with 192 results, which delayed recorded completion times. Batch
makespan is now the last batch result, not the end of the interactive
bursts. The clock starts before the submit POST rather than after it, and
the report records the submit's duration (`submit_s`).

## Scaled out: 4 replicas and prefix affinity for continuations

There are 4 vLLM replicas at 32 slots each (one per node), and every
replica publishes KV-cache events (`--kv-events-config`, ZMQ on :5557) and
serves `/render` (`--enable-scale-out`). The processor runs with
`--concurrency 256`. All arms use eviction and resume. The EPP config
comes from `epp/<arm>.yaml`:

- `none`: queue (2) and kv-cache-utilization (2) scorers.
- `session`: adds `session-affinity-scorer` (3, Alpha), with
  `strategy: session_id` keyed on `x-session-id`. The scenario sets that
  header to each request's ID, and the processor forwards it on every
  continuation.
- `precise`: adds `precise-prefix-cache-producer` fed by vLLM KV events,
  `prefix-cache-scorer` (3), and a `token-producer` that renders text
  prompts through vLLM so they hash like the token-ID prompts of
  continuations.

The load is 768 batch requests × 1500 tokens (3 jobs of 256, loose
deadlines) plus 15 bursts of 128 interactive requests, 6 s apart. Every
request carries `cache_salt` set to the run ID, so each run starts with
cold prefix caches.

```sh
RUNS=runs-affinity \
LOAD="--max-tokens 1500 --job a:256:1800 --job b:256:1800 --job c:256:1800 --warmup 3 --bursts 15 --burst-size 128 --burst-gap 6 --vllm http://localhost:18081 --baseline-concurrency 128" \
CHART=... ./experiment.sh 3 affinity-none affinity-session affinity-precise
```

3 runs per arm. All 768 batch outputs were identical and all 1920
interactive requests succeeded in every run.

| arm | TTFT p50 s | TTFT p95 s | batch makespan s | batch p50 s | resumes | prefill tokens computed | prefix hit % | decode per pod, max/min |
|---|---|---|---|---|---|---|---|---|
| none | 0.776 | 1.73 | 86.2 (83.1-91.0) | 67.9 | ~1300 | 850,000 | 34.4 | 1.03-1.16 |
| session | 0.787 | 1.61 | 98.8 (89.5-109) | 67.6 | ~1320 | 45,700 | 96.4 | 1.07-1.10 |
| precise | 0.626 | 1.40 | 112 (86.2-130) | 68.3 | ~1210 | 42,100 | 96.2 | 1.11-1.72 |

- Affinity works. Continuations go back to the replica that holds their
  prefix: prefill recomputed drops 95% (850k to about 44k tokens), and the
  hit rate goes from 34% to 96%. Session affinity and precise prefix
  routing do equally well on continuations.
- Neither speeds up the batch at this model size. Prefill of a
  1500-token prefix on a 1.5B model is cheap, and affinity costs load
  balance. With precise routing, one replica decoded up to 1.7× the tokens
  of another. The 128-request interactive bursts share 24 prompts and pile
  onto the replicas that cache them, which gives the best interactive p50
  (0.63 s) and the longest batch tail. Batch p50 is the same in every arm.
  Where prefill is expensive (bigger models, long prompts), the 95% saving
  should outweigh that.
- Without session headers or rendering, v0.10.0's default approximate
  prefix scorer cannot match a continuation to its original. It packs text
  prompts 4 bytes per pseudo-token but passes token-ID prompts through, so
  the two never hash alike. It also indexes only prompt blocks (64-token
  minimum), so it never sees the generated tokens.

Two setup findings from this round:

- The per-endpoint cap (`concurrency-detector` listed as a filter in the
  scheduling profile) rejects requests that flow control has just
  released: 628 `ResourceExhausted - failed to find target endpoint`
  errors, returned as 429s to 7% of interactive requests (`priority=10`)
  and to batch requests. Flow control releases a request when the pool
  aggregate is under the limit, but in-flight counts only rise after
  scheduling, so a burst gets released against a stale count and the late
  arrivals find every endpoint full. The arms above run without the
  filter, so an affinity-heavy replica can exceed 32 and queue inside
  vLLM. The filtered run is in `runs-affinity-filter/`.
- Without `cache_salt`, identical greedy outputs across runs keep every
  replica's prefix cache warm for later runs. No-affinity prefill
  recomputed fell from 265k to 87k tokens over three runs. Those runs are
  in `runs-affinity-warm/`.

## 14B with long distinct prompts

The pool is 4 × Qwen2.5-14B-Instruct at 32 slots each, with 541k tokens of KV
cache per replica. Each batch request gets a distinct pseudo-document of
about 4,800 prompt tokens (`--context-tokens 4000`) and decodes 2000
tokens. Each interactive request gets a distinct 620-token prompt and
decodes 128 tokens. The load is 3 jobs × 128 batch requests plus 18
bursts of 32 interactive requests, 10 s apart. The scenario runs inside
the cluster (`driver.yaml`, `DRIVER=evict-driver`). At this scale, 128
concurrent streams over `kubectl port-forward` dropped connections and
made makespans vary from 45 to 300 s; those runs are in
`runs-14b-portforward/`.

```sh
DRIVER=evict-driver MODEL=Qwen/Qwen2.5-14B-Instruct RUNS=runs-14b \
LOAD="--model Qwen/Qwen2.5-14B-Instruct --context-tokens 4000 --interactive-context-tokens 500 --interactive-max-tokens 128 --max-tokens 2000 --job a:128:3600 --job b:128:3600 --job c:128:3600 --warmup 10 --bursts 18 --burst-size 32 --burst-gap 10 --vllm http://evict-epp:80 --baseline-concurrency 128" \
CHART=... ./experiment.sh 3 affinity-none affinity-session affinity-precise
```

3 runs per arm. Every batch request completed and every interactive
request succeeded.

| arm | TTFT p50 s | TTFT p95 s | batch makespan s | batch p50 s | evictions | resumes | prefill tokens computed | prefix hit % |
|---|---|---|---|---|---|---|---|---|
| none | 0.96 | 6.79 | 351 (346-356) | 241 | 564 | ~390 | 3.86M | 6.9 |
| session | 0.78 | 5.13 | 484 (444-541) | 240 | 568 | ~260 | 2.72M | 20.3 |
| precise | 0.84 | 6.77 | 361 (349-375) | 230 | 654 | ~330 | 2.62M | 30.1 |

- Eviction takes the newest dispatch, so a resumed request has little
  output saved (about 70 tokens on average). What a continuation costs is
  re-prefilling its ~4,900-token prompt. Without affinity, that is about
  half of all prefill.
- Precise prefix routing cuts prefill by 32%, keeps the batch makespan
  within 3% of no affinity, and gives the best batch p50 (230 s vs 241 s).
- Session affinity cuts prefill by 30%, but the batch tail is 38% longer
  (484 s vs 351 s). A pinned continuation waits for its replica even when
  that replica is the busy one, while precise routing still weighs load
  against the cache hit.
- Hit rates stay low (at most 38%) because each replica's KV cache holds
  only about 110 prompts' worth, and many evicted prefixes are gone before
  their continuation returns.
- "identical" in the report is not meaningful on 14B. Each vLLM process
  picks slightly different long-prompt kernels at startup, and the choice
  survives `VLLM_TRITON_FORCE_FIRST_CONFIG=1`, Inductor max-autotune off
  and combo-kernel benchmarking off. The same prompt gives two different
  greedy outputs across replicas, so outputs cannot be compared with a
  baseline taken on another replica. Output identity for resume is shown
  on 1.5B above.
