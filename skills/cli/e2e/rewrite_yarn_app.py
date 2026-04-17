"""Rewrite a Spark event log to use a YARN-style app ID and attempt ID."""

import argparse
import io
import json
import zstandard


def rewrite(src: str, dest: str, app_id: str, attempt_id: str) -> None:
    dctx = zstandard.ZstdDecompressor()
    with open(src, "rb") as f:
        reader = dctx.stream_reader(f)
        data = io.TextIOWrapper(reader, encoding="utf-8").read()

    lines = []
    for line in data.splitlines():
        event = json.loads(line)
        ev = event.get("Event")
        if ev == "SparkListenerApplicationStart":
            event["App ID"] = app_id
            event["App Attempt ID"] = attempt_id
        elif ev == "SparkListenerEnvironmentUpdate":
            props = event.get("Spark Properties", {})
            if "spark.app.id" in props:
                props["spark.app.id"] = app_id
        lines.append(json.dumps(event, separators=(",", ":")))

    cctx = zstandard.ZstdCompressor()
    with open(dest, "wb") as f:
        f.write(cctx.compress("\n".join(lines).encode()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("src")
    p.add_argument("dest")
    p.add_argument("--app-id", required=True)
    p.add_argument("--attempt-id", required=True)
    rewrite(**vars(p.parse_args()))
