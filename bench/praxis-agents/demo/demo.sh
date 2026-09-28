#!/usr/bin/env bash
# Background research agents evicted and resumed under live interactive load,
# in a tmux session opened in a new Ghostty tab.
#
#   ┌─ background agents · batch ───────────────────────────────────────┐
#   │ agents.py: every turn a Responses background response via Praxis  │
#   ├─ interactive load ────────────────────────────────────────────────┤
#   │ guidellm's live stats; the band is short so its setup scrolls off │
#   ├─ evictions and resumes ───────────────────────────────────────────┤
#   │ EPP evictions and processor resumes and requeues, from stern      │
#   └───────────────────────────────────────────────────────────────────┘
#
# Usage: demo.sh [up|down]. F12 in the demo tab advances to the next beat;
# AUTO=1 advances on timers instead. Needs the stack in ../cluster deployed.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
CTX=${CTX:-coreweave-waldorf}
NS=${NS:-weaton-dev}
SESSION=${SESSION:-agents-demo}
AGENTS=${AGENTS:-8}
RATE=${RATE:-3}
LOAD_TOKENS=${LOAD_TOKENS:-128}
DURATION=${DURATION:-150}
RUN=${RUN:-${TMPDIR:-/tmp}/agents-demo}
PRAXIS_PORT=18125
EPP_PORT=18126
K=(kubectl --context "$CTX" -n "$NS")

caption() {
    tmux set -t "$SESSION" status-left "#[bold,fg=colour16,bg=colour220] $1 #[default] "
}

beat() {
    if [[ -n ${AUTO:-} ]]; then
        caption "$1"
        sleep "$2"
    else
        caption "$1 · F12"
        tmux wait-for demo-beat
    fi
}

type_into() {
    tmux send-keys -t "$1" -l "$2"
    tmux send-keys -t "$1" Enter
}

# One line naming the model, engine, hardware and router the demo runs on,
# read from the live stack.
stack_info() {
    local gpus model version args seqs len router
    gpus=$("${K[@]}" exec deploy/agents-vllm -- nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits)
    model=$("${K[@]}" exec deploy/agents-vllm -- curl -s localhost:8000/v1/models | jq -r '.data[0].id')
    version=$("${K[@]}" exec deploy/agents-vllm -- curl -s localhost:8000/version | jq -r .version)
    args=$("${K[@]}" get deploy/agents-vllm -o 'jsonpath={.spec.template.spec.containers[0].args}')
    seqs=$(jq -r 'index("--max-num-seqs") as $i | .[$i + 1]' <<< "$args")
    len=$(jq -r 'index("--max-model-len") as $i | .[$i + 1] | tonumber / 1024 | floor' <<< "$args")
    router=$("${K[@]}" get deploy/agents-epp -o 'jsonpath={.spec.template.spec.containers[*].image}' | tr ' ' '\n' | sed -n 's/.*endpoint-picker:\(.*\)/\1/p')
    printf '%s · vLLM %s · %s × %s %s GiB · %s sequence slots · %sk context · llm-d-router %s' \
        "$model" "$version" "$(wc -l <<< "$gpus" | tr -d ' ')" "${gpus%%,*}" "$(( ${gpus##*, } / 1024 ))" "$seqs" "$len" "$router"
}

# The configuration that decides who gets evicted, read from the cluster:
# Praxis's routing of background and foreground calls, the objectives'
# priorities, and the EPP's flow control.
config_info() {
    local praxis epp
    praxis=$("${K[@]}" get cm agents-praxis -o 'jsonpath={.data.praxis\.yaml}')
    epp=$("${K[@]}" get cm agents-epp -o 'jsonpath={.data.agents\.yaml}')
    echo "# Praxis AI: background: true → llm-d-async"
    yq '{"background": (.filter_chains[0].filters[] | select(.filter == "openai_response_store") | .background | pick(["processor_url", "queue", "objective", "deadline_secs"]))}' <<< "$praxis"
    echo
    echo "# Praxis AI: foreground calls → EPP"
    yq '{"request_set": [.. | select(.filter? == "headers") | .request_set[]]}' <<< "$praxis"
    echo
    echo "# llm-d-router: InferenceObjectives"
    "${K[@]}" get inferenceobjectives.llm-d.ai -o json | jq '[.items[] | {(.metadata.name): {priority: .spec.priority}}] | add' | yq -P
    echo
    echo "# llm-d-router EPP: flow control"
    yq '{"flowControl": .flowControl, "saturation": (.plugins[] | select(.type == "concurrency-detector") | .parameters)}' <<< "$epp"
}

forward() {
    "${K[@]}" port-forward "svc/$1" "$2" > "$RUN/forward-$1.log" 2>&1 &
    echo $! >> "$RUN/pids"
}

up() {
    mkdir -p "$RUN"
    rm -rf "$RUN/reports"
    : > "$RUN/pids"
    forward agents-praxis "$PRAXIS_PORT:8080"
    forward agents-epp "$EPP_PORT:80"

    local shell=(bash --noprofile --norc)
    tmux kill-session -t "$SESSION" 2>/dev/null || true
    tmux new-session -d -s "$SESSION" -n background -x 240 -y 60 -c "$RUN" "${shell[@]}"
    tmux set -t "$SESSION" status-style bg=colour235,fg=colour250
    tmux set -t "$SESSION" status-left-length 200
    tmux set -t "$SESSION" status-right " llm-d-async · Praxis AI · llm-d-router "
    tmux set -t "$SESSION" pane-border-status top
    tmux set -t "$SESSION" pane-border-format " #[bold]#{pane_title} "
    tmux set -t "$SESSION" pane-border-style fg=colour244
    tmux set -t "$SESSION" pane-active-border-style fg=colour244
    tmux set -t "$SESSION" mouse on
    tmux bind -n F12 wait-for -S demo-beat

    local board=$SESSION:background.0
    local load ticker config
    load=$(tmux split-window -v -t "$board" -c "$RUN" -P -F '#{pane_id}' "${shell[@]}")
    ticker=$(tmux split-window -v -t "$load" -c "$HERE" -P -F '#{pane_id}' "${shell[@]}")
    config=$(tmux split-window -h -l 38% -t "$ticker" -c "$RUN" -P -F '#{pane_id}' "${shell[@]}")
    board=$(tmux display -p -t "$board" '#{pane_id}')
    tmux select-pane -t "$board" -T "background agents · every model turn a Responses background: true call → Praxis AI → llm-d-async (batch)"
    tmux select-pane -t "$load" -T "interactive load · guidellm, Poisson $RATE req/s straight to the EPP, never queued in llm-d-async"
    tmux select-pane -t "$ticker" -T "flow control evictions → llm-d-async resumes"
    tmux select-pane -t "$config" -T "configuration, read from the cluster"
    config_info > "$RUN/config.yaml"
    local stack
    stack=$(stack_info)
    for p in "$load" "$board" "$ticker"; do
        tmux send-keys -t "$p" "export PS1='$ ' PATH='$HERE':\$PATH AGENTS_STACK='$stack' AGENTS_RESPONSES='$RUN/reports/responses.tsv'; clear" Enter
    done

    tmux send-keys -t "$ticker" "clear; stern --context $CTX -n $NS 'agents-(epp|processor)' --since 1s --output json --include 'evicted by flow control|resuming interrupted|retrying request|escalating request' 2>/dev/null | ticker.py" Enter

    tmux new-window -d -t "$SESSION:9" -n driver "$HERE/demo.sh drive $board $load $config; exec bash"
    for w in 0 9; do
        tmux set -w -t "$SESSION:$w" window-status-format ""
        tmux set -w -t "$SESSION:$w" window-status-current-format ""
    done

    local attach
    attach="$(command -v tmux) attach -t $SESSION"
    if [[ -n ${REC:-} ]]; then
        attach="$(command -v asciinema) rec --overwrite --idle-time-limit 2 --title 'Background agents on llm-d' --command '$attach' '$REC'"
    fi
    printf '#!/usr/bin/env bash\nexec %s\n' "$attach" > "$RUN/attach.sh"
    chmod +x "$RUN/attach.sh"

    if [[ ${TERM_PROGRAM:-} == ghostty ]]; then
        osascript -e "tell application \"Ghostty\"
            set cfg to new surface configuration
            set command of cfg to \"$RUN/attach.sh\"
            set wait after command of cfg to true
            ${FONT_SIZE:+set font size of cfg to $FONT_SIZE}
            new tab in front window with configuration cfg
        end tell" > /dev/null
    else
        echo "attach with: $RUN/attach.sh"
    fi
}

drive() {
    local board=$1 load=$2 config=$3
    until [[ -n $(tmux list-clients -t "$SESSION") ]]; do sleep 0.2; done
    tmux resize-pane -t "$board" -y 17
    tmux resize-pane -t "$load" -y 7
    tmux respawn-pane -k -t "$config" "bat --language yaml --style plain --color always --paging never '$RUN/config.yaml'; tput civis; exec sleep 86400"
    beat "background agents on llm-d: interactive traffic evicts their generations, llm-d-async resumes them" 5
    type_into "$board" "agents.py --agents $AGENTS --out reports"
    beat "each turn queues in llm-d-async as batch work · next: interactive load" 20
    type_into "$load" "uvx guidellm run --backend kind=openai_http,target=http://127.0.0.1:$EPP_PORT,model=zai-org/GLM-4.7-Flash,extras.headers.x-llm-d-inference-objective=interactive --tokenizer kind=huggingface_auto,model=facebook/opt-125m --data kind=synthetic_text,prompt_tokens=512,output_tokens=$LOAD_TOKENS --profile kind=poisson,rate=$RATE --constraint kind=max_duration,seconds=$DURATION"
    beat "interactive bursts saturate the pool: flow control evicts batch generations" 60
    caption "llm-d-async resumes each evicted generation from its saved tokens · the agents never notice"
    sleep 2
    local announced=
    while [[ $(tmux display -p -t "$board" '#{pane_current_command}') != bash ]]; do
        if [[ -z $announced && $(tmux display -p -t "$load" '#{pane_current_command}') == bash ]]; then
            caption "interactive load over after ${DURATION}s · the remaining agents finish"
            announced=1
        fi
        sleep 2
    done
    caption "every agent finished: evicted generations resumed, not redone"
    sleep 12
    if [[ -n ${AUTO:-} ]]; then
        tmux detach-client -s "$SESSION"
    fi
}

down() {
    tmux unbind -n F12 2>/dev/null || true
    tmux kill-session -t "$SESSION" 2>/dev/null || true
    [[ -f $RUN/pids ]] && xargs kill < "$RUN/pids" 2>/dev/null || true
    rm -f "$RUN/pids"
}

case ${1:-up} in
    up) up ;;
    drive) shift; drive "$@" ;;
    down) down ;;
    *) echo "usage: $0 [up|down]" >&2; exit 2 ;;
esac
