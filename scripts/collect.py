#!/usr/bin/env python3
"""CLI: backfill history and capture a daily snapshot.

Usage:
    python scripts/collect.py                 # sync every configured repo
    python scripts/collect.py --repo owner/name
    python scripts/collect.py --init-only     # create DB + repo rows, no API calls
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.collector import Collector, ensure_tables  # noqa: E402
from app.config import load_settings  # noqa: E402
from app.database import build_engine, init_db, make_session_factory  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect GitHub repository metrics")
    parser.add_argument("--config", default=None, help="path to config.yaml")
    parser.add_argument("--repo", default=None, help="only sync owner/name")
    parser.add_argument("--init-only", action="store_true", help="initialize DB without API calls")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    settings = load_settings(args.config)
    if args.repo:
        wanted = args.repo.lower()
        settings = settings.__class__(
            **{**settings.__dict__, "repositories": [
                r for r in settings.repositories if r.full_name.lower() == wanted
            ]}
        )
        if not settings.repositories:
            print(f"repository {args.repo} is not in config.yaml", file=sys.stderr)
            return 2

    engine = build_engine(settings.database_url)
    init_db(engine)
    session_factory = make_session_factory(engine)
    ensure_tables(settings, session_factory)

    if args.init_only:
        print(f"initialized database at {settings.database_url}")
        return 0

    token_state = "token" if settings.github_token else "anonymous (60 req/h)"
    print(f"collecting {len(settings.repositories)} repo(s) using {token_state}")

    collector = Collector(settings, session_factory)
    try:
        results = collector.run()
    finally:
        collector.close()

    print(json.dumps(results, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
