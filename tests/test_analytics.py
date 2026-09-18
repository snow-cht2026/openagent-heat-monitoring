from datetime import date, datetime, timezone

import pytest
from sqlalchemy.orm import Session

from app.analytics import build_series, summarize
from app.database import Base, Event, Repository, build_engine, make_session_factory


@pytest.fixture()
def session(tmp_path):
    engine = build_engine(f"sqlite:///{tmp_path/'test.db'}")
    Base.metadata.create_all(engine)
    factory = make_session_factory(engine)
    with Session(engine) as s:
        yield s
    engine.dispose()


def _dt(day: int) -> datetime:
    return datetime(2024, 1, day, 12, 0, tzinfo=timezone.utc)


@pytest.fixture()
def repo(session: Session) -> Repository:
    r = Repository(owner="o", name="n", full_name="o/n")
    session.add(r)
    session.commit()
    return r


def _add(session, repo, event_type, external_id, occurred, **kwargs):
    session.add(
        Event(
            repository_id=repo.id,
            event_type=event_type,
            external_id=external_id,
            occurred_at=occurred,
            **kwargs,
        )
    )


def _seed(session: Session, repo: Repository) -> None:
    _add(session, repo, "stars", "s1", _dt(1))
    _add(session, repo, "stars", "s2", _dt(1))
    _add(session, repo, "stars", "s3", _dt(3))
    _add(session, repo, "forks", "f1", _dt(2))
    _add(session, repo, "issues", "i1", _dt(1), closed_at=_dt(2), state="closed")
    _add(session, repo, "issues", "i2", _dt(3), state="open")
    _add(session, repo, "pulls", "p1", _dt(1), merged_at=_dt(2), closed_at=_dt(2), state="closed")
    _add(session, repo, "commits", "c1", _dt(1), actor="alice")
    _add(session, repo, "commits", "c2", _dt(2), actor="bob")
    _add(session, repo, "commits", "c3", _dt(3), actor="alice")
    _add(session, repo, "releases", "r1", _dt(2))
    session.commit()


def _values(series, metric):
    return [p["value"] for p in series[metric]]


def test_build_series_daily(session: Session, repo: Repository):
    _seed(session, repo)
    series = build_series(_load(session, repo), 0, date(2024, 1, 1), date(2024, 1, 4), "daily")

    assert _values(series, "stars_new") == [2, 0, 1, 0]
    assert _values(series, "stars_total") == [2, 2, 3, 3]
    assert _values(series, "forks_total") == [0, 1, 1, 1]
    assert _values(series, "issues_opened") == [1, 0, 1, 0]
    assert _values(series, "issues_closed") == [0, 1, 0, 0]
    assert _values(series, "issues_open") == [1, 0, 1, 1]
    assert _values(series, "prs_merged") == [0, 1, 0, 0]
    assert _values(series, "prs_open") == [1, 0, 0, 0]
    assert _values(series, "commits") == [1, 1, 1, 0]
    assert _values(series, "releases_total") == [0, 1, 1, 1]
    assert _values(series, "contributors_total") == [1, 2, 2, 2]


def test_build_series_weekly(session: Session, repo: Repository):
    _seed(session, repo)
    series = build_series(_load(session, repo), 0, date(2024, 1, 1), date(2024, 1, 29), "weekly")
    # Weekly buckets anchored on Mondays: Jan 1, Jan 8, Jan 15, Jan 22, Jan 29
    assert _values(series, "stars_new")[0] == 3
    assert _values(series, "commits")[0] == 3


def test_summarize(session: Session, repo: Repository):
    _seed(session, repo)
    summary = summarize(session, repo.id, date(2024, 1, 1), date(2024, 1, 4))
    assert summary["stars_total"]["delta"] == 1
    assert summary["commits"]["total"] == 3


def _load(session: Session, repo: Repository):
    from app.analytics import load_events

    return load_events(session, repo.id)
