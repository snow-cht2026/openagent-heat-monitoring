#!/usr/bin/env python3
"""Probe GitHub endpoints per configured repository and report access status.

Useful for diagnosing 401/403/404 responses (e.g. stargazers restrictions)
before running a full collection.

    python scripts/diagnose.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402

from app.config import load_settings  # noqa: E402

ENDPOINTS = [
    ("repo", "/repos/{o}/{r}", None),
    ("stargazers", "/repos/{o}/{r}/stargazers", "application/vnd.github.star+json"),
    ("forks", "/repos/{o}/{r}/forks", None),
    ("issues", "/repos/{o}/{r}/issues", None),
    ("pulls", "/repos/{o}/{r}/pulls", None),
    ("commits", "/repos/{o}/{r}/commits", None),
    ("releases", "/repos/{o}/{r}/releases", None),
    ("contributors", "/repos/{o}/{r}/contributors", None),
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose GitHub API access")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    settings = load_settings(args.config)
    token = settings.github_token
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "openagent-heat-monitoring-diagnose",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    print(f"token: {'yes' if token else 'anonymous (60 req/h)'}\n")

    with httpx.Client(base_url="https://api.github.com", headers=headers, timeout=20) as client:
        for repo in settings.repositories:
            print(f"=== {repo.full_name} ===")
            for label, path, accept in ENDPOINTS:
                request_headers = {"Accept": accept} if accept else None
                try:
                    response = client.get(
                        path.format(o=repo.owner, r=repo.name),
                        params={"page": 1, "per_page": 1},
                        headers=request_headers,
                    )
                    detail = ""
                    if response.status_code >= 400:
                        detail = response.json().get("message", "")[:70]
                    scopes = response.headers.get("X-OAuth-Scopes", "")
                    if label == "repo" and scopes:
                        detail = f"scopes={scopes}"
                    print(f"  {response.status_code:>3}  {label:<14} {detail}")
                except httpx.HTTPError as exc:
                    print(f"  ERR  {label:<14} {exc}")
            print()


if __name__ == "__main__":
    raise SystemExit(main())
