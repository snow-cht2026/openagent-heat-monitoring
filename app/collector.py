"""Backfill historical activity and capture daily snapshots."""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from .config import RepositoryConfig, Settings
from .database import Contributor, Event, Repository, Snapshot, SyncState, session_scope
from .github_client import (
    AuthenticationRequired,
    GitHubClient,
    PermissionDenied,
    RateLimitExceeded,
)

logger = logging.getLogger(__name__)

EVENT_STREAMS = ["stars", "forks", "issues", "pulls", "commits", "releases"]
ALL_STREAMS = EVENT_STREAMS + ["contributors"]
ASCENDING_STREAMS = {"stars", "forks", "issues", "pulls"}


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class Collector:
    def __init__(
        self,
        settings: Settings,
        session_factory: sessionmaker[Session],
        client: GitHubClient | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.client = client or GitHubClient(
            token=settings.github_token,
            per_page=settings.per_page,
            timeout=settings.timeout,
            rate_limit_floor=settings.rate_limit_floor,
        )

    def close(self) -> None:
        self.client.close()

    def run(self) -> list[dict[str, Any]]:
        results = []
        for repo_config in self.settings.repositories:
            results.append(self.sync_repository(repo_config))
        return results

    def sync_repository(self, repo_config: RepositoryConfig) -> dict[str, Any]:
        with session_scope(self.session_factory) as session:
            repo = self._get_or_create_repo(session, repo_config)

        streams: dict[str, str] = {}
        rate_limited = False
        for stream in ALL_STREAMS:
            if rate_limited:
                streams[stream] = "skipped"
                continue
            try:
                streams[stream] = self._sync_stream(repo.id, stream)
            except RateLimitExceeded as exc:
                logger.warning("rate limited during %s: %s", stream, exc)
                streams[stream] = "rate_limited"
                rate_limited = True
            except AuthenticationRequired as exc:
                logger.warning("skipping %s: %s", stream, exc)
                streams[stream] = "auth_required"
            except PermissionDenied as exc:
                logger.warning("skipping %s: %s", stream, exc)
                streams[stream] = "permission_denied"

        snapshot = None
        if not rate_limited:
            try:
                snapshot = self.capture_snapshot(repo.id)
            except (RateLimitExceeded, AuthenticationRequired, PermissionDenied, FileNotFoundError) as exc:
                logger.warning("could not capture snapshot: %s", exc)

        return {
            "repository": repo_config.full_name,
            "streams": streams,
            "snapshot": snapshot,
            "rate_limit_remaining": self.client.rate_limit.remaining,
        }

    # -- repository ----------------------------------------------------------
    def _get_or_create_repo(self, session: Session, config: RepositoryConfig) -> Repository:
        repo = session.scalar(
            select(Repository).where(Repository.full_name == config.full_name)
        )
        if repo is None:
            repo = Repository(owner=config.owner, name=config.name, full_name=config.full_name)
            session.add(repo)
            session.flush()
        return repo

    def _get_state(self, session: Session, repo_id: int, stream: str) -> SyncState:
        state = session.scalar(
            select(SyncState).where(
                SyncState.repository_id == repo_id, SyncState.stream == stream
            )
        )
        if state is None:
            state = SyncState(repository_id=repo_id, stream=stream, next_page=1)
            session.add(state)
            session.flush()
        return state

    # -- streams -------------------------------------------------------------
    def _sync_stream(self, repo_id: int, stream: str) -> str:
        if stream == "contributors":
            return self._sync_contributors(repo_id)

        with session_scope(self.session_factory) as session:
            repo = session.get(Repository, repo_id)
            state = self._get_state(session, repo_id, stream)
            owner, name = repo.owner, repo.name
            is_backfill = not state.is_complete

            if is_backfill:
                start_page = max(1, state.next_page)
            elif stream in ASCENDING_STREAMS:
                # Ascending streams append new items after the last known page.
                start_page = max(1, state.next_page)
            else:
                # Newest-first streams: new items always appear on page 1.
                start_page = 1

            max_pages = self.settings.max_pages_per_run
            pages_fetched = 0
            new_events = 0
            last_page = start_page
            has_next = False
            saw_events = False

            for page, events, has_next in self.client.iter_stream(
                stream, owner, name, start_page=start_page
            ):
                pages_fetched += 1
                last_page = page
                if events:
                    saw_events = True
                added, all_known = self._upsert_events(session, repo_id, stream, events)
                new_events += added
                session.flush()

                if not is_backfill and all_known and events:
                    break
                if has_next and pages_fetched >= max_pages:
                    break
                if not has_next:
                    state.is_complete = True
                    break

            if has_next or saw_events:
                state.next_page = last_page + 1
            state.last_synced_at = datetime.now(timezone.utc)
            state.last_error = None

            if state.is_complete and stream in ("issues", "pulls"):
                self._refresh_recent_states(session, repo_id, stream, owner, name)

            if new_events == 0 and state.is_complete:
                return "up_to_date"
            if state.is_complete:
                return "complete"
            return f"backfilling(page={state.next_page})"

    def _refresh_recent_states(
        self, session: Session, repo_id: int, stream: str, owner: str, name: str
    ) -> None:
        """Rescan recently-updated issues/PRs to capture close/merge transitions."""
        for page in range(1, self.settings.refresh_recent_pages + 1):
            try:
                events, has_next = self.client.fetch_recent_updated(stream, owner, name, page)
            except RateLimitExceeded as exc:
                logger.warning("stopping state refresh for %s: %s", stream, exc)
                return
            if not events:
                return
            ids = [event["external_id"] for event in events]
            rows = session.scalars(
                select(Event).where(
                    Event.repository_id == repo_id,
                    Event.event_type == stream,
                    Event.external_id.in_(ids),
                )
            ).all()
            known = {row.external_id: row for row in rows}
            for event in events:
                row = known.get(event["external_id"])
                if row is None:
                    continue
                row.closed_at = parse_dt(event.get("closed_at")) or row.closed_at
                row.merged_at = parse_dt(event.get("merged_at")) or row.merged_at
                row.state = event.get("state") or row.state
            session.flush()
            if not has_next:
                break

    def _upsert_events(
        self, session: Session, repo_id: int, stream: str, events: list[dict[str, Any]]
    ) -> tuple[int, bool]:
        if not events:
            return 0, True

        ids = [e["external_id"] for e in events]
        existing_rows = session.scalars(
            select(Event).where(
                Event.repository_id == repo_id,
                Event.event_type == stream,
                Event.external_id.in_(ids),
            )
        ).all()
        existing = {row.external_id: row for row in existing_rows}

        added = 0
        for event in events:
            occurred_at = parse_dt(event.get("occurred_at"))
            if occurred_at is None:
                continue
            row = existing.get(event["external_id"])
            if row is None:
                session.add(
                    Event(
                        repository_id=repo_id,
                        event_type=stream,
                        external_id=event["external_id"],
                        occurred_at=occurred_at,
                        closed_at=parse_dt(event.get("closed_at")),
                        merged_at=parse_dt(event.get("merged_at")),
                        actor=event.get("actor"),
                        state=event.get("state"),
                        title=event.get("title"),
                    )
                )
                added += 1
            else:
                # Refresh mutable state (issues/PRs can close or merge later).
                row.closed_at = parse_dt(event.get("closed_at")) or row.closed_at
                row.merged_at = parse_dt(event.get("merged_at")) or row.merged_at
                row.state = event.get("state") or row.state

        all_known = added == 0 and all(e["external_id"] in existing for e in events)
        return added, all_known

    def _sync_contributors(self, repo_id: int) -> str:
        with session_scope(self.session_factory) as session:
            repo = session.get(Repository, repo_id)
            state = self._get_state(session, repo_id, "contributors")
            max_pages = self.settings.max_pages_per_run
            start_page = max(1, state.next_page) if not state.is_complete else 1

            pages_fetched = 0
            last_page = start_page
            changed = 0
            has_next = False
            saw_events = False
            for page, events, has_next in self.client.iter_stream(
                "contributors", repo.owner, repo.name, start_page=start_page
            ):
                pages_fetched += 1
                last_page = page
                if events:
                    saw_events = True
                for event in events:
                    login = event.get("actor") or event["external_id"]
                    row = session.scalar(
                        select(Contributor).where(
                            Contributor.repository_id == repo_id,
                            Contributor.login == login,
                        )
                    )
                    if row is None:
                        session.add(
                            Contributor(
                                repository_id=repo_id,
                                login=login,
                                contributions=int(event.get("contributions", 0)),
                                avatar_url=event.get("avatar_url"),
                            )
                        )
                        changed += 1
                    elif row.contributions != int(event.get("contributions", 0)):
                        row.contributions = int(event.get("contributions", 0))
                        changed += 1
                session.flush()
                if has_next and pages_fetched >= max_pages:
                    break
                if not has_next:
                    state.is_complete = True
                    break

            if has_next or saw_events:
                state.next_page = last_page + 1
            state.last_synced_at = datetime.now(timezone.utc)
            return "complete" if state.is_complete else f"backfilling(page={state.next_page})"

    # -- snapshots -----------------------------------------------------------
    def capture_snapshot(self, repo_id: int) -> dict[str, Any]:
        with session_scope(self.session_factory) as session:
            repo = session.get(Repository, repo_id)
            data = self.client.get_repository(repo.owner, repo.name)

            repo.description = data.get("description")
            repo.default_branch = data.get("default_branch")
            repo.github_created_at = parse_dt(data.get("created_at"))

            open_prs = session.scalar(
                select(func.count(Event.id)).where(
                    Event.repository_id == repo_id,
                    Event.event_type == "pulls",
                    Event.state == "open",
                    Event.merged_at.is_(None),
                )
            ) or 0
            total_releases = session.scalar(
                select(func.count(Event.id)).where(
                    Event.repository_id == repo_id, Event.event_type == "releases"
                )
            ) or 0
            total_contributors = session.scalar(
                select(func.count(Contributor.id)).where(Contributor.repository_id == repo_id)
            ) or 0

            open_issues_total = int(data.get("open_issues_count") or 0)
            open_issues = max(open_issues_total - open_prs, 0)

            today = datetime.now(timezone.utc).date()
            snapshot = session.scalar(
                select(Snapshot).where(
                    Snapshot.repository_id == repo_id, Snapshot.snapshot_date == today
                )
            )
            if snapshot is None:
                snapshot = Snapshot(repository_id=repo_id, snapshot_date=today)
                session.add(snapshot)

            snapshot.stars = int(data.get("stargazers_count") or 0)
            snapshot.forks = int(data.get("forks_count") or 0)
            snapshot.watchers = int(data.get("subscribers_count") or 0)
            snapshot.open_issues = open_issues
            snapshot.open_prs = int(open_prs)
            snapshot.total_releases = int(total_releases)
            snapshot.total_contributors = int(total_contributors)
            snapshot.collected_at = datetime.now(timezone.utc)

            session.flush()
            return {
                "date": today.isoformat(),
                "stars": snapshot.stars,
                "forks": snapshot.forks,
                "watchers": snapshot.watchers,
                "open_issues": snapshot.open_issues,
                "open_prs": snapshot.open_prs,
                "total_releases": snapshot.total_releases,
                "total_contributors": snapshot.total_contributors,
            }


def ensure_tables(settings: Settings, session_factory: sessionmaker[Session]) -> None:
    """Create repository rows for every configured repo (no API calls)."""
    with session_scope(session_factory) as session:
        for config in settings.repositories:
            existing = session.scalar(
                select(Repository).where(Repository.full_name == config.full_name)
            )
            if existing is None:
                session.add(
                    Repository(owner=config.owner, name=config.name, full_name=config.full_name)
                )
