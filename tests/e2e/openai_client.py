#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["openai>=1.0"]
# ///
"""Calls the processor's OpenAI-compatible routes with the official OpenAI
SDK, as an unmodified client would, and prints what the client got as JSON.

Argument: a JSON object with `base_url`, `api` (`chat` or `completions`),
`request` (the create() arguments), and optional `headers`.
"""

import json
import sys

import openai


def assembled(chunks) -> dict:
    """What a client builds from a stream: text per field, tool calls by
    index, the finish reason and the usage."""
    text: dict[str, str] = {}
    calls: dict[int, dict] = {}
    finish = None
    usage = None
    ids = set()
    for chunk in chunks:
        ids.add(chunk.id)
        if chunk.usage is not None:
            usage = chunk.usage.model_dump()
        for choice in chunk.choices:
            finish = choice.finish_reason or finish
            if hasattr(choice, "delta"):
                delta = choice.delta
                fields = {"content": delta.content, **(delta.model_extra or {})}
                for call in delta.tool_calls or []:
                    entry = calls.setdefault(call.index, {"id": None, "name": "", "arguments": ""})
                    entry["id"] = call.id or entry["id"]
                    if call.function is not None:
                        entry["name"] += call.function.name or ""
                        entry["arguments"] += call.function.arguments or ""
            else:
                fields = {"text": choice.text}
            for name, value in fields.items():
                if isinstance(value, str):
                    text[name] = text.get(name, "") + value
    return {
        "ids": sorted(ids),
        "text": text,
        "tool_calls": [calls[i] for i in sorted(calls)],
        "finish_reason": finish,
        "usage": usage,
    }


def main() -> None:
    args = json.loads(sys.argv[1])
    client = openai.OpenAI(
        base_url=args["base_url"] + "/v1",
        api_key="unused",
        max_retries=0,
        timeout=600,
        default_headers=args.get("headers", {}),
    )
    resource = client.chat.completions if args["api"] == "chat" else client.completions
    try:
        if args["request"].get("stream"):
            out = {"stream": assembled(resource.create(**args["request"]))}
        else:
            raw = resource.with_raw_response.create(**args["request"])
            raw.parse()
            out = {"response": raw.http_response.json()}
    except openai.APIStatusError as e:
        out = {"status": e.status_code, "body": e.body}
    except openai.APIError as e:
        out = {"error": type(e).__name__, "message": str(e), "body": getattr(e, "body", None)}
    print(json.dumps(out))


if __name__ == "__main__":
    main()
