#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["openai>=1.0"]
# ///
"""Drives the OpenAI Responses API through Praxis AI with the official OpenAI
SDK and prints what the client saw as JSON.

Argument: a JSON object with `base_url`, `scenario`, and `request` (the
create() arguments). Scenarios:

- `foreground`: one create call.
- `background`: create with background=True, then poll retrieve() until the
  response leaves queued and in_progress, recording every status seen.
- `cancel`: create with background=True, cancel it twice, retrieve it.
- `follow_up`: foreground create, then a second turn that answers its first
  function call through previous_response_id.
- `background_chain`: the same two turns in background mode, then a third,
  foreground turn continuing the second.
- `stream`: a streamed create, recording the event types, the reasoning and
  text deltas, and the final response.
"""

import json
import sys
import time

import openai


def summary(r) -> dict:
    return {
        "id": r.id,
        "status": r.status,
        "background": r.background,
        "text": r.output_text,
        "output_types": [o.type for o in r.output],
        "function_calls": [
            {"name": o.name, "arguments": o.arguments, "call_id": o.call_id}
            for o in r.output
            if o.type == "function_call"
        ],
        "error": r.error.model_dump() if r.error else None,
    }


def wait(client, r):
    deadline = time.time() + 120
    while r.status in {"queued", "in_progress"} and time.time() < deadline:
        time.sleep(0.1)
        r = client.responses.retrieve(r.id)
    return r


def answer_first_call(r) -> list:
    call = next(o for o in r.output if o.type == "function_call")
    return [{"type": "function_call_output", "call_id": call.call_id, "output": '{"temp_c": 17}'}]


def main() -> None:
    args = json.loads(sys.argv[1])
    client = openai.OpenAI(base_url=args["base_url"] + "/v1", api_key="unused", max_retries=0, timeout=60)
    request = args["request"]
    scenario = args["scenario"]
    try:
        if scenario == "foreground":
            out = summary(client.responses.create(**request))
        elif scenario == "background":
            r = client.responses.create(background=True, **request)
            seen = [r.status]
            deadline = time.time() + 120
            while r.status in {"queued", "in_progress"} and time.time() < deadline:
                time.sleep(0.1)
                r = client.responses.retrieve(r.id)
                if r.status != seen[-1]:
                    seen.append(r.status)
            out = {**summary(r), "seen": seen, "retrieved_again": summary(client.responses.retrieve(r.id))}
        elif scenario == "cancel":
            r = client.responses.create(background=True, **request)
            first = client.responses.cancel(r.id)
            second = client.responses.cancel(r.id)
            out = {
                "created": r.status,
                "cancelled": first.status,
                "cancelled_again": second.status,
                "retrieved": client.responses.retrieve(r.id).status,
            }
        elif scenario == "follow_up":
            r = client.responses.create(**request)
            call = next(o for o in r.output if o.type == "function_call")
            follow = client.responses.create(
                model=request["model"],
                previous_response_id=r.id,
                tools=request.get("tools", []),
                max_output_tokens=8,
                input=[{"type": "function_call_output", "call_id": call.call_id, "output": '{"temp_c": 17}'}],
            )
            out = {"first": summary(r), "follow_up": summary(follow)}
        elif scenario == "background_chain":
            first = wait(client, client.responses.create(background=True, **request))
            second = wait(
                client,
                client.responses.create(
                    background=True,
                    model=request["model"],
                    previous_response_id=first.id,
                    instructions=request.get("instructions"),
                    tools=request.get("tools", []),
                    max_output_tokens=8,
                    input=answer_first_call(first),
                ),
            )
            third = client.responses.create(
                model=request["model"], previous_response_id=second.id, max_output_tokens=8, input="Thanks."
            )
            out = {"first": summary(first), "second": summary(second), "third": summary(third)}
        elif scenario == "stream":
            kinds, reasoning, text, final = [], "", "", None
            for event in client.responses.create(stream=True, **request):
                kinds.append(event.type)
                if event.type == "response.reasoning_text.delta":
                    reasoning += event.delta
                elif event.type == "response.output_text.delta":
                    text += event.delta
                elif event.type in {"response.completed", "response.failed", "response.incomplete"}:
                    final = summary(event.response)
            out = {"events": kinds, "reasoning": reasoning, "text": text, "final": final}
        else:
            raise SystemExit(f"unknown scenario {scenario}")
    except openai.APIStatusError as e:
        out = {"status_code": e.status_code, "body": e.body}
    print(json.dumps(out))


if __name__ == "__main__":
    main()
