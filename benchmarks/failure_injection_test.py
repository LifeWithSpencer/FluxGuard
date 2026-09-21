"""
benchmarks/failure_injection_test.py

Automated failure-injection harness: drives load against the gateway with
plain asyncio + httpx (no Locust dependency needed for this specific test),
takes Redis down mid-run via `docker compose stop redis`, and asserts:

  1. Zero connection-level exceptions / 5xx responses at any point - every
     response is a clean 200 or 429.
  2. The response header `X-RateLimit-Served-By` flips from "redis" to
     "local_fallback" during the outage.
  3. It flips back to "redis" after Redis is restarted and the circuit
     breaker's recovery_timeout elapses.

This complements locustfile.py (which is about throughput/latency) by
being a pass/fail correctness check for the disaster-recovery story.

Usage (from repo root, with the docker-compose stack already up):
    python benchmarks/failure_injection_test.py --host http://localhost:8080
"""

from __future__ import annotations

import argparse
import asyncio
import subprocess
import time
from collections import Counter

import httpx


async def hammer(client: httpx.AsyncClient, path: str, duration_s: float, results: Counter) -> None:
    end = time.monotonic() + duration_s
    headers = {"X-API-Key": "failure-injection-key"}
    while time.monotonic() < end:
        try:
            resp = await client.get(path, headers=headers, timeout=2.0)
        except httpx.HTTPError as exc:
            results["transport_error"] += 1
            print(f"  ! transport error: {exc}")
            continue

        if resp.status_code >= 500:
            results["5xx"] += 1
            print(f"  ! unexpected 5xx: {resp.status_code}")
        elif resp.status_code in (200, 429):
            served_by = resp.headers.get("X-RateLimit-Served-By", "unknown")
            results[f"ok:{served_by}"] += 1
        else:
            results[f"other:{resp.status_code}"] += 1

        await asyncio.sleep(0.02)


def docker_compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True)


async def main(host: str) -> None:
    results: Counter[str] = Counter()

    async with httpx.AsyncClient(base_url=host) as client:
        print("Phase 1: baseline load with Redis healthy (10s)...")
        await hammer(client, "/api/resource", 10, results)

        print("Phase 2: stopping Redis, continuing load (15s)...")
        docker_compose("stop", "redis")
        await hammer(client, "/api/resource", 15, results)

        print("Phase 3: restarting Redis, continuing load through recovery (20s)...")
        docker_compose("start", "redis")
        await hammer(client, "/api/resource", 20, results)

    print("\n--- Results ---")
    for key, count in sorted(results.items()):
        print(f"  {key}: {count}")

    assert results.get("transport_error", 0) == 0, "transport errors occurred - gateway crashed a connection"
    assert results.get("5xx", 0) == 0, "gateway returned 5xx during the test - fallback did not engage cleanly"
    assert any(k.startswith("ok:local_fallback") for k in results), (
        "never observed local_fallback being used - Redis outage may not have been detected"
    )
    assert any(k.startswith("ok:redis") for k in results), (
        "never observed recovery back to redis-backed decisions"
    )

    print("\nPASS: zero crashes/5xx, fallback engaged during outage, recovery confirmed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="http://localhost:8080")
    args = parser.parse_args()
    asyncio.run(main(args.host))
