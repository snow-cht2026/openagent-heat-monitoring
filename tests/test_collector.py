from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.collector import Collector, ensure_tables
from app.config import RepositoryConfig, Settings
from app.database import Base, Event, Repository, SyncState, build_engine, make_session_factory


class FakeClient:
    """Replays canned pages; never touches the network."""

    def __init__(self, pages, recent=None, errors=None):
        self.pages = pages
        self.recent = recent or {}
        self.errors = errors or {}
        self.rate_limit = SimpleNamespace(remaining=100, limit=100)

    def iter_stream(self, stream, owner, name, start_page=1):
        if stream in self.errors:
            raise self.errors[stream]
        sequence = self.pages.get(stream, [])
        for index in range(start_page - 1, len(sequence)):
            items, has_next = sequence[index]
            yield index + 1, items, has_next

    def fetch_recent_updated(self, stream, owner, name, page=1):
        return self.recent.get(stream, ([], False))

    def get_repository(self, owner, name):
        return {
            "stargazers_count": 10,
            "forks_count": 4,
            "subscribers_count": 3,
            "open_issues_count": 5,
            "description": "demo",
            "default_branch": "main",
            "created_at": "2020-01-01T00:00:00Z",
        }

    def close(self):
        pass


def _star(login):
    return {"external_id": login, "occurred_at": "2024-01-01T00:00:00Z", "actor": login}


def _make_settings(max_pages):
    return Settings(
        database_url="sqlite://",
        github_token=None,
        per_page=100,
        timeout=30,
        rate_limit_floor=2,
        max_pages_per_run=max_pages,
        refresh_recent_pages=1,
        repositories=[RepositoryConfig(owner="o", name="n")],
    )


@pytest.fixture()
def factory(tmp_path):
    engine = build_engine(f"sqlite:///{tmp_path/'c.db'}")
    Base.metadata.create_all(engine)
    return make_session_factory(engine)


def _state(factory, stream):
    with factory() as session:
        return session.scalar(
            select(SyncState).join(Repository).where(SyncState.stream == stream)
        )


def test_backfill_resumes_across_runs(factory):
    pages = {"stars": [([_star("a"), _star("b")], True), ([_star("c")], True), ([_star("d")], False)]}
    settings = _make_settings(max_pages=1)
    collector = Collector(settings, factory, client=FakeClient(pages))

    collector.sync_repository(settings.repositories[0])
    assert _state(factory, "stars").next_page == 2
    assert _state(factory, "stars").is_complete is False

    collector.sync_repository(settings.repositories[0])
    assert _state(factory, "stars").next_page == 3

    collector.sync_repository(settings.repositories[0])
    state = _state(factory, "stars")
    assert state.is_complete is True

    with factory() as session:
        count = session.query(Event).filter(Event.event_type == "stars").count()
    assert count == 4


def test_incremental_picks_up_appended_events(factory):
    pages = {"stars": [([_star("a")], True), ([_star("b")], False)]}
    settings = _make_settings(max_pages=5)
    collector = Collector(settings, factory, client=FakeClient(pages))
    collector.sync_repository(settings.repositories[0])
    assert _state(factory, "stars").is_complete is True

    # New star arrives; ascending stream appends it after the last known page.
    pages["stars"].append(([_star("c")], False))
    collector.sync_repository(settings.repositories[0])

    with factory() as session:
        count = session.query(Event).filter(Event.event_type == "stars").count()
    assert count == 3


def test_snapshot_captured(factory):
    settings = _make_settings(max_pages=5)
    collector = Collector(settings, factory, client=FakeClient({}))
    result = collector.sync_repository(settings.repositories[0])
    assert result["snapshot"]["stars"] == 10
    assert result["snapshot"]["forks"] == 4
    assert result["snapshot"]["open_issues"] == 5


def test_refresh_captures_closed_state(factory):
    issue_open = {
        "external_id": "1",
        "occurred_at": "2024-01-01T00:00:00Z",
        "state": "open",
        "actor": "a",
    }
    issue_closed = {**issue_open, "closed_at": "2024-01-05T00:00:00Z", "state": "closed"}
    client = FakeClient({"issues": [([issue_open], False)]}, recent={"issues": ([issue_closed], False)})
    settings = _make_settings(max_pages=5)
    collector = Collector(settings, factory, client=client)
    collector.sync_repository(settings.repositories[0])

    with factory() as session:
        row = session.scalar(select(Event).where(Event.event_type == "issues"))
        assert row.state == "closed"
        assert row.closed_at is not None


def test_permission_denied_skips_only_that_stream(factory):
    from app.github_client import PermissionDenied

    client = FakeClient(
        {"forks": [([{"external_id": "1", "occurred_at": "2024-01-01T00:00:00Z", "actor": "a"}], False)]},
        errors={"stars": PermissionDenied("token lacks access to /stargazers")},
    )
    settings = _make_settings(max_pages=5)
    collector = Collector(settings, factory, client=client)
    result = collector.sync_repository(settings.repositories[0])

    assert result["streams"]["stars"] == "permission_denied"
    assert result["streams"]["forks"] == "complete"
    assert result["snapshot"]["stars"] == 10


def test_ensure_tables_is_idempotent(factory):
    settings = _make_settings(max_pages=5)
    ensure_tables(settings, factory)
    ensure_tables(settings, factory)
    with factory() as session:
        assert session.query(Repository).count() == 1
