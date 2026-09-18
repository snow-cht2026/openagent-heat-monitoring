"""FastAPI application: JSON API + ECharts dashboard."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from .analytics import build_series, load_events, load_snapshots, snapshot_series, summarize
from .collector import Collector, ensure_tables
from .config import Settings, load_settings
from .database import (
    Contributor,
    Event,
    Repository,
    build_engine,
    init_db,
    make_session_factory,
)

logger = logging.getLogger(__name__)
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

_collect_status: dict[str, Any] = {"running": False, "last_result": None, "last_error": None}


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings: Settings = load_settings()
    engine = build_engine(settings.database_url)
    init_db(engine)
    session_factory = make_session_factory(engine)
    ensure_tables(settings, session_factory)
    app.state.settings = settings
    app.state.engine = engine
    app.state.session_factory = session_factory
    yield
    engine.dispose()


app = FastAPI(title="OpenAgent Heat Monitoring", version="1.0.0", lifespan=lifespan)


def get_session(request: Request) -> Iterator[Session]:
    factory = request.app.state.session_factory
    session = factory()
    try:
        yield session
    finally:
        session.close()


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _resolve_repo(session: Session, owner: str, name: str) -> Repository:
    repo = session.scalar(
        select(Repository).where(Repository.full_name == f"{owner}/{name}")
    )
    if repo is None:
        raise HTTPException(status_code=404, detail=f"repository {owner}/{name} not tracked")
    return repo


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, session: Session = Depends(get_session)):
    repos = session.scalars(select(Repository).order_by(Repository.full_name)).all()
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "repos": [{"full_name": r.full_name, "owner": r.owner, "name": r.name} for r in repos],
        },
    )


@app.get("/api/repos")
def list_repos(session: Session = Depends(get_session)):
    repos = session.scalars(select(Repository).order_by(Repository.full_name)).all()
    return [
        {
            "owner": repo.owner,
            "name": repo.name,
            "full_name": repo.full_name,
            "description": repo.description,
            "event_counts": dict(
                session.execute(
                    select(Event.event_type, func.count(Event.id))
                    .where(Event.repository_id == repo.id)
                    .group_by(Event.event_type)
                ).all()
            ),
        }
        for repo in repos
    ]


@app.get("/api/repos/{owner}/{name}/summary")
def repo_summary(
    owner: str,
    name: str,
    start: date | None = Query(None),
    end: date | None = Query(None),
    session: Session = Depends(get_session),
):
    repo = _resolve_repo(session, owner, name)
    return {"repository": repo.full_name, "summary": summarize(session, repo.id, start, end)}


@app.get("/api/repos/{owner}/{name}/timeseries")
def repo_timeseries(
    owner: str,
    name: str,
    start: date | None = Query(None),
    end: date | None = Query(None),
    granularity: str = Query("daily", pattern="^(daily|weekly|monthly)$"),
    metrics: str | None = Query(None, description="comma separated metric names"),
    session: Session = Depends(get_session),
):
    repo = _resolve_repo(session, owner, name)
    events = load_events(session, repo.id)
    snapshots = load_snapshots(session, repo.id)
    contributors_count = (
        session.query(Contributor).filter(Contributor.repository_id == repo.id).count()
    )
    series = build_series(events, contributors_count, start, end, granularity, snapshots)
    if metrics:
        wanted = {m.strip() for m in metrics.split(",") if m.strip()}
        series = {k: v for k, v in series.items() if k in wanted}
    return {"repository": repo.full_name, "granularity": granularity, "series": series}


@app.get("/api/repos/{owner}/{name}/snapshots")
def repo_snapshots(
    owner: str,
    name: str,
    start: date | None = Query(None),
    end: date | None = Query(None),
    session: Session = Depends(get_session),
):
    repo = _resolve_repo(session, owner, name)
    return {"repository": repo.full_name, "snapshots": snapshot_series(session, repo.id, start, end)}


@app.get("/api/repos/{owner}/{name}/contributors")
def repo_contributors(
    owner: str,
    name: str,
    limit: int = Query(20, ge=1, le=200),
    session: Session = Depends(get_session),
):
    repo = _resolve_repo(session, owner, name)
    rows = session.scalars(
        select(Contributor)
        .where(Contributor.repository_id == repo.id)
        .order_by(desc(Contributor.contributions))
        .limit(limit)
    ).all()
    return {
        "repository": repo.full_name,
        "contributors": [
            {"login": r.login, "contributions": r.contributions, "avatar_url": r.avatar_url}
            for r in rows
        ],
    }


@app.get("/api/repos/{owner}/{name}/events/recent")
def repo_recent_events(
    owner: str,
    name: str,
    limit: int = Query(30, ge=1, le=200),
    session: Session = Depends(get_session),
):
    repo = _resolve_repo(session, owner, name)
    rows = session.scalars(
        select(Event)
        .where(Event.repository_id == repo.id)
        .order_by(desc(Event.occurred_at))
        .limit(limit)
    ).all()
    return {
        "repository": repo.full_name,
        "events": [
            {
                "type": r.event_type,
                "occurred_at": _iso_utc(r.occurred_at),
                "actor": r.actor,
                "state": r.state,
                "title": r.title,
            }
            for r in rows
        ],
    }


def _run_collection(request: Request) -> None:
    _collect_status["running"] = True
    _collect_status["last_error"] = None
    collector = Collector(request.app.state.settings, request.app.state.session_factory)
    try:
        _collect_status["last_result"] = collector.run()
    except Exception as exc:  # pragma: no cover - surfaced through API
        logger.exception("collection failed")
        _collect_status["last_error"] = str(exc)
    finally:
        collector.close()
        _collect_status["running"] = False


@app.post("/api/collect")
def trigger_collection(request: Request, background: BackgroundTasks):
    if _collect_status["running"]:
        return {"status": "already_running", **_collect_status}
    background.add_task(_run_collection, request)
    return {"status": "started"}


@app.get("/api/collect/status")
def collection_status():
    return _collect_status


@app.get("/api/rate-limit")
def rate_limit(request: Request):
    from .github_client import GitHubClient

    settings: Settings = request.app.state.settings
    client = GitHubClient(token=settings.github_token)
    try:
        status = client.fetch_rate_limit()
        return {"remaining": status.remaining, "limit": status.limit, "reset_at": status.reset_at}
    finally:
        client.close()


@app.get("/health")
def health():
    return {"status": "ok"}
