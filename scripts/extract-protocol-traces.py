#!/usr/bin/env python3
"""Extract and segment Antfly protocol traces from one or more process logs."""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any


FAMILIES = (
    "placement-readiness",
    "index-lifecycle",
    "derived-replay",
    "enrichment-lease",
)
PROTOCOL_TAGS = frozenset(
    ("antfly-protocol-trace", *(f"{family}-trace" for family in FAMILIES))
)
PROTOCOL_TAG_CLAIM = re.compile(
    r'"tag"\s*:\s*"(?:antfly-protocol-trace|(?:'
    + "|".join(re.escape(family) for family in FAMILIES)
    + r')-trace)'
)


def parse_line(raw: str, source: Path, line_number: int) -> dict[str, Any] | None:
    claims_protocol_trace = PROTOCOL_TAG_CLAIM.search(raw) is not None
    start = raw.find("{")
    if start < 0:
        if claims_protocol_trace:
            raise ValueError(f"{source}:{line_number}: protocol trace has no JSON object")
        return None
    try:
        value = json.loads(raw[start:])
    except json.JSONDecodeError as error:
        if claims_protocol_trace:
            raise ValueError(
                f"{source}:{line_number}: malformed protocol trace JSON: {error.msg}"
            ) from error
        return None
    if not isinstance(value, dict):
        if claims_protocol_trace:
            raise ValueError(f"{source}:{line_number}: protocol trace must be a JSON object")
        return None

    tag = value.get("tag")
    if tag not in PROTOCOL_TAGS:
        return None
    family = value.get("family")
    if not isinstance(family, str) or not family:
        raise ValueError(f"{source}:{line_number}: protocol trace requires string family")
    if tag != "antfly-protocol-trace" and tag != f"{family}-trace":
        raise ValueError(f"{source}:{line_number}: protocol trace tag/family mismatch")

    trace_id = value.get("traceId")
    if not isinstance(trace_id, str) or not trace_id:
        raise ValueError(f"{source}:{line_number}: protocol trace requires string traceId")
    seq = value.get("seq")
    if isinstance(seq, bool) or not isinstance(seq, int):
        raise ValueError(f"{source}:{line_number}: protocol trace requires integer seq")
    event = value.get("event")
    if not isinstance(event, dict):
        raise ValueError(f"{source}:{line_number}: protocol trace requires object event")
    if not isinstance(event.get("name"), str) or not event["name"]:
        raise ValueError(f"{source}:{line_number}: protocol trace requires string event.name")
    if not isinstance(event.get("facts"), dict):
        raise ValueError(f"{source}:{line_number}: protocol trace requires object event.facts")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("family", choices=FAMILIES)
    parser.add_argument("outdir", type=Path)
    parser.add_argument("logs", nargs="+", type=Path)
    args = parser.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)
    written = 0
    for source in args.logs:
        segments: dict[str, list[dict[str, Any]]] = defaultdict(list)
        last_seq: dict[tuple[str, str], int] = {}
        for line_number, raw in enumerate(
            source.read_text(errors="replace").splitlines(), start=1
        ):
            try:
                obj = parse_line(raw, source, line_number)
            except ValueError as error:
                parser.error(str(error))
            if obj is None:
                continue

            trace_id = obj["traceId"]
            seq = obj["seq"]
            order_key = (obj["family"], trace_id)
            previous = last_seq.get(order_key)
            if previous is not None and seq <= previous:
                parser.error(
                    f"{source}:{line_number}: {obj['family']} trace {trace_id!r} "
                    f"sequence {seq} is duplicate or decreasing after {previous}"
                )
            last_seq[order_key] = seq
            if obj["family"] != args.family:
                continue

            event = obj["event"]
            facts = event["facts"]
            if args.family == "index-lifecycle":
                # Ordinary index creation is useful raw telemetry but is not a
                # durable-generation lifecycle behavior.
                if (
                    event["name"] == "RequestGeneration"
                    and facts.get("durableWork") is False
                ):
                    continue
            segments[trace_id].append(obj)

        for trace_id, events in segments.items():
            safe_id = "".join(
                c if c.isalnum() or c in "-_" else "_" for c in trace_id
            )
            path = args.outdir / f"{args.family}-{source.stem}-{safe_id}.ndjson"
            path.write_text(
                "".join(
                    json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n"
                    for event in events
                )
            )
            written += 1

    if written == 0:
        raise SystemExit(f"no {args.family} protocol traces found")
    print(f"wrote {written} {args.family} trace segment(s) to {args.outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
