#!/usr/bin/env python3
import json
from pathlib import Path

from shared_benchmark import EVIDENCE, run_scenario


CASES = [
    (
        "shared-gc-default-4",
        "v2ray-sspanel-shared-pprof",
        "shared-server-4.json",
        "shared-client-4.json",
        4,
        True,
        20,
        {},
    ),
    (
        "shared-gc-gogc75-4",
        "v2ray-sspanel-shared-pprof",
        "shared-server-4.json",
        "shared-client-4.json",
        4,
        True,
        21,
        {"GOGC": "75"},
    ),
    (
        "shared-gc-memlimit1200-4",
        "v2ray-sspanel-shared-pprof",
        "shared-server-4.json",
        "shared-client-4.json",
        4,
        True,
        22,
        {"GOMEMLIMIT": "1200MiB"},
    ),
]


def main():
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    results = []
    for case in CASES:
        print(f"gc-ab={case[0]}", flush=True)
        scenario, binary, server_cfg, client_cfg, nodes, profile, index, extra_env = case
        results.append(
            run_scenario(
                scenario,
                binary,
                server_cfg,
                client_cfg,
                nodes,
                profile,
                index,
                extra_env,
            )
        )
    (EVIDENCE / "shared-gc-ab.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print("gc_ab=PASS")


if __name__ == "__main__":
    main()
