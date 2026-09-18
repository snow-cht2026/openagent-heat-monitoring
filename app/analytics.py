"""Pandas-based aggregation of stored events into chartable time series."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from .database import Contributor, Event, Snapshot

GRANULARITIES = ("daily", "weekly", "monthly")

NEW_METRICS = {
    "stars_new": ("stars", "occurred_at"),
    "forks_new": ("forks", "occurred_at"),
    "issues_opened": ("issues", "occurred_at"),
    "issues_closed": ("issues", "closed_at"),
    "prs_opened": ("pulls", "occurred_at"),
    "prs_closed": ("pulls", "closed_at"),
    "prs_merged": ("pulls", "merged_at"),
    "commits": ("commits", "occurred_at"),
    "releases_new": ("releases", "occurred_at"),
}

SNAPSHOT_COLUMNS = [
    "snapshot_date",
    "stars",
    "forks",
    "watchers",
    "open_issues",
    "open_prs",
    "total_releases",
    "total_contributors",
]


def _bucket_start(ts: pd.Timestamp, granularity: str) -> pd.Timestamp:
    """Normalize a timestamp to the start of its bucket (timezone preserved)."""
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    if granularity == "daily":
        return ts.normalize()
    if granularity == "weekly":
        return (ts - pd.Timedelta(days=int(ts.weekday()))).normalize()
    if granularity == "monthly":
        return ts.replace(day=1).normalize()
    raise ValueError(f"granularity must be one of {GRANULARITIES}")


def _date_index(start: date | None, end: date | None, granularity: str) -> pd.DatetimeIndex:
    end_ts = pd.Timestamp(end or datetime.now(timezone.utc).date(), tz="UTC")
    start_ts = pd.Timestamp(start, tz="UTC") if start else end_ts - pd.Timedelta(days=89)
    if start_ts > end_ts:
        start_ts = end_ts
    daily = pd.date_range(start_ts.normalize(), end_ts.normalize(), freq="D", tz="UTC")
    return pd.DatetimeIndex(sorted({_bucket_start(ts, granularity) for ts in daily}))


def _to_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def load_events(session: Session, repository_id: int) -> pd.DataFrame:
    rows = session.execute(
        select(
            Event.event_type,
            Event.occurred_at,
            Event.closed_at,
            Event.merged_at,
            Event.actor,
        ).where(Event.repository_id == repository_id)
    ).all()
    frame = pd.DataFrame(
        rows, columns=["event_type", "occurred_at", "closed_at", "merged_at", "actor"]
    )
    for column in ("occurred_at", "closed_at", "merged_at"):
        frame[column] = (
            _to_utc(frame[column])
            if not frame.empty
            else pd.Series(dtype="datetime64[ns, UTC]")
        )
    return frame


def load_snapshots(session: Session, repository_id: int) -> pd.DataFrame:
    rows = session.execute(
        select(
            Snapshot.snapshot_date,
            Snapshot.stars,
            Snapshot.forks,
            Snapshot.watchers,
            Snapshot.open_issues,
            Snapshot.open_prs,
            Snapshot.total_releases,
            Snapshot.total_contributors,
        ).where(Snapshot.repository_id == repository_id)
    ).all()
    frame = pd.DataFrame(rows, columns=SNAPSHOT_COLUMNS)
    if not frame.empty:
        frame["snapshot_date"] = _to_utc(frame["snapshot_date"])
        frame = frame.sort_values("snapshot_date")
    return frame


def _counts_in_buckets(
    frame: pd.DataFrame, event_type: str, column: str, index: pd.DatetimeIndex, granularity: str
) -> pd.Series:
    """New events per bucket, restricted to the requested window."""
    empty = pd.Series(0.0, index=index)
    if frame.empty:
        return empty
    subset = frame[frame["event_type"] == event_type].dropna(subset=[column])
    if subset.empty:
        return empty
    keys = subset[column].map(lambda ts: _bucket_start(ts, granularity))
    grouped = keys.value_counts().sort_index()
    grouped.index = pd.DatetimeIndex(grouped.index)
    return grouped.reindex(index, fill_value=0).astype(float)


def _cumulative_all_history(
    frame: pd.DataFrame, event_type: str, column: str, granularity: str
) -> pd.Series:
    """Cumulative count over the full stored history (seeds windows correctly)."""
    if frame.empty:
        return pd.Series(dtype="float64")
    subset = frame[frame["event_type"] == event_type].dropna(subset=[column])
    if subset.empty:
        return pd.Series(dtype="float64")
    keys = subset[column].map(lambda ts: _bucket_start(ts, granularity))
    counts = keys.value_counts().sort_index()
    counts.index = pd.DatetimeIndex(counts.index)
    return counts.cumsum().astype(float)


def _align(series: pd.Series, index: pd.DatetimeIndex) -> pd.Series:
    if series.empty:
        return pd.Series(0.0, index=index)
    return series.reindex(index, method="ffill").fillna(0.0)


def _open_series(opened: pd.Series, closed: pd.Series, index: pd.DatetimeIndex) -> pd.Series:
    if opened.empty and closed.empty:
        return pd.Series(0.0, index=index)
    union = opened.index.union(closed.index)
    open_counts = (
        opened.reindex(union).ffill().fillna(0.0) - closed.reindex(union).ffill().fillna(0.0)
    ).clip(lower=0)
    return open_counts.reindex(index, method="ffill").fillna(0.0)


def _snapshot_series(
    snapshots: pd.DataFrame | None, column: str, index: pd.DatetimeIndex
) -> pd.Series | None:
    if snapshots is None or snapshots.empty or column not in snapshots.columns:
        return None
    points = (
        snapshots.dropna(subset=[column])
        .set_index("snapshot_date")[column]
        .astype(float)
        .sort_index()
    )
    if points.empty:
        return None
    daily = pd.date_range(points.index.min(), max(points.index.max(), index[-1]), freq="D", tz="UTC")
    daily_values = points.reindex(daily).ffill().bfill()
    return daily_values.reindex(index, method="ffill").fillna(daily_values.iloc[0])


def _contributor_growth(
    frame: pd.DataFrame, index: pd.DatetimeIndex, granularity: str
) -> pd.Series:
    if frame.empty:
        return pd.Series(0.0, index=index)
    commits = frame[(frame["event_type"] == "commits") & frame["actor"].notna()]
    if commits.empty:
        return pd.Series(0.0, index=index)
    first_seen = commits.groupby("actor")["occurred_at"].min()
    keys = first_seen.map(lambda ts: _bucket_start(ts, granularity))
    starts = keys.value_counts().sort_index()
    cumulative = starts.cumsum()
    cumulative.index = pd.DatetimeIndex(cumulative.index)
    return cumulative.reindex(index, method="ffill").fillna(0.0).astype(float)


def build_series(
    events: pd.DataFrame,
    contributors_count: int,
    start: date | None,
    end: date | None,
    granularity: str = "daily",
    snapshots: pd.DataFrame | None = None,
) -> dict[str, list[dict[str, Any]]]:
    if granularity not in GRANULARITIES:
        raise ValueError(f"granularity must be one of {list(GRANULARITIES)}")
    index = _date_index(start, end, granularity)

    new_series = {
        metric: _counts_in_buckets(events, event_type, column, index, granularity)
        for metric, (event_type, column) in NEW_METRICS.items()
    }

    # Cumulative totals prefer full event history; fall back to daily snapshots
    # (needed for stars without a token, since /stargazers now requires auth).
    stars_cum = _cumulative_all_history(events, "stars", "occurred_at", granularity)
    forks_cum = _cumulative_all_history(events, "forks", "occurred_at", granularity)
    releases_cum = _cumulative_all_history(events, "releases", "occurred_at", granularity)
    issues_opened = _cumulative_all_history(events, "issues", "occurred_at", granularity)
    issues_closed = _cumulative_all_history(events, "issues", "closed_at", granularity)
    prs_opened = _cumulative_all_history(events, "pulls", "occurred_at", granularity)
    prs_closed = _cumulative_all_history(events, "pulls", "closed_at", granularity)

    def calibrated_total(event_series: pd.Series, column: str) -> pd.Series:
        """Anchor the event-derived series to the authoritative snapshot total.

        total(t) = latest_snapshot - (events after t). This keeps the current
        absolute value correct even while a backfill is still incomplete, while
        still using events for the shape of the change.
        """
        snapshot = _snapshot_series(snapshots, column, index)
        if event_series.empty:
            return snapshot if snapshot is not None else pd.Series(0.0, index=index)
        aligned = _align(event_series, index)
        if snapshot is None:
            return aligned
        offset = float(snapshot.iloc[-1] - aligned.iloc[-1])
        return (aligned + offset).clip(lower=0.0)

    totals: dict[str, pd.Series] = {
        "stars_total": calibrated_total(stars_cum, "stars"),
        "forks_total": calibrated_total(forks_cum, "forks"),
        "releases_total": calibrated_total(releases_cum, "total_releases"),
        "issues_open": calibrated_total(_open_series(issues_opened, issues_closed, index), "open_issues"),
        "prs_open": calibrated_total(_open_series(prs_opened, prs_closed, index), "open_prs"),
        "contributors_total": calibrated_total(
            _contributor_growth(events, index, granularity), "total_contributors"
        ),
    }

    if totals["contributors_total"].max() == 0 and contributors_count:
        totals["contributors_total"] = pd.Series(float(contributors_count), index=index)

    def serialize(series: pd.Series) -> list[dict[str, Any]]:
        return [
            {"date": ts.date().isoformat(), "value": float(value)} for ts, value in series.items()
        ]

    return {metric: serialize(series) for metric, series in {**new_series, **totals}.items()}


def summarize(
    session: Session, repository_id: int, start: date | None, end: date | None
) -> dict[str, Any]:
    events = load_events(session, repository_id)
    snapshots = load_snapshots(session, repository_id)
    contributors_count = (
        session.query(Contributor).filter(Contributor.repository_id == repository_id).count()
    )
    series = build_series(events, contributors_count, start, end, "daily", snapshots)

    summary: dict[str, Any] = {}
    for metric, points in series.items():
        if not points:
            continue
        values = [p["value"] for p in points]
        summary[metric] = {
            "start": values[0],
            "end": values[-1],
            "delta": round(values[-1] - values[0], 2),
            "total": round(sum(values), 2),
        }
    return summary


def snapshot_series(session: Session, repository_id: int, start: date | None, end: date | None):
    query = select(Snapshot).where(Snapshot.repository_id == repository_id)
    if start:
        query = query.where(Snapshot.snapshot_date >= start)
    if end:
        query = query.where(Snapshot.snapshot_date <= end)
    query = query.order_by(Snapshot.snapshot_date)
    rows = session.scalars(query).all()
    return [
        {
            "date": row.snapshot_date.isoformat(),
            "stars": row.stars,
            "forks": row.forks,
            "watchers": row.watchers,
            "open_issues": row.open_issues,
            "open_prs": row.open_prs,
            "total_releases": row.total_releases,
            "total_contributors": row.total_contributors,
        }
        for row in rows
    ]
