# Demo: background agents evicted under live load

Research agents run every model turn as an OpenAI Responses background
response (`background: true`) through Praxis AI, so each turn is `batch` work
in llm-d-async. guidellm sends `interactive` chat traffic straight to the EPP
until the pool saturates, flow control evicts the agents' generations, and
llm-d-async resumes each one from its saved tokens. The agents only see a
longer `in_progress`.

```text
┌─ guidellm · interactive ────┬─ background agents · batch ─────────┐
│ constant-rate chat, never   │ one row per agent: turn, status,    │
│ queued or evicted           │ tool calls, resumes and their saved │
│                             │ token counts                        │
├─────────────────────────────┴─────────────────────────────────────┤
│ EPP evictions and processor resumes, from stern                   │
└───────────────────────────────────────────────────────────────────┘
```

- `agents.py`: the agents. Each one investigates a question about this
  repository with read-only tools run locally (`list_dir`, `read_file`,
  `grep`), continues with `previous_response_id`, and writes a Markdown report.
  The board tags an agent when the processor logs
  `resuming interrupted generation` for one of its response IDs (Praxis
  submits each background response under its own ID). Runs standalone too:
  `./agents.py --help`.
- The line above the board names the model, vLLM version, GPUs, sequence
  slots, context length and router version, which `demo.sh` reads from the
  running stack (`nvidia-smi`, vLLM's `/version` and `/v1/models`, the vLLM and
  EPP deployments) and passes as `AGENTS_STACK`.
- `ticker.py`: formats `stern --output json` from the EPP and the processor
  into one line per eviction and resume.
- `demo.sh`: port-forwards Praxis (18125) and the EPP (18126), builds the tmux
  session, and opens it in a new Ghostty tab through Ghostty's AppleScript
  (elsewhere it prints the `tmux attach` command).

## Run

Deploy `../cluster` first, then:

```sh
./demo.sh          # F12 in the new tab advances each beat
AUTO=1 ./demo.sh   # timed beats, for recording
AUTO=1 REC=$PWD/agents.cast ./demo.sh   # the tab records itself with asciinema
agg --speed 2 agents.cast agents.gif    # optional GIF
./demo.sh down     # session, port-forwards and the F12 binding
```

`AGENTS` (8), `RATE` (mean guidellm requests per second, Poisson arrivals, 3), `LOAD_TOKENS` (guidellm output tokens per request, 128) and `DURATION`
(guidellm seconds, 150) tune the run. Reports and `summary.json` land in
`$TMPDIR/agents-demo/reports`.
