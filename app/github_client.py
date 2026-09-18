"""Thin GitHub REST API client with rate-limit awareness and pagination."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator

import httpx

API_ROOT = "https://api.github.com"
STAR_ACCEPT = "application/vnd.github.star+json"


class RateLimitExceeded(RuntimeError):
    """Raised when the remaining quota drops below the configured floor."""

    def __init__(self, message: str, reset_at: datetime | None = None) -> None:
        super().__init__(message)
        self.reset_at = reset_at


class AuthenticationRequired(RuntimeError):
    """Raised when an endpoint requires a token (e.g. stargazers star+json)."""


class PermissionDenied(RuntimeError):
    """Raised when the token is valid but lacks access to an endpoint."""


@dataclass
class RateLimitStatus:
    remaining: int
    limit: int
    reset_at: datetime

    def __str__(self) -> str:  # pragma: no cover - debug helper
        return f"{self.remaining}/{self.limit} until {self.reset_at.isoformat()}"


class GitHubClient:
    def __init__(
        self,
        token: str | None = None,
        per_page: int = 100,
        timeout: int = 30,
        rate_limit_floor: int = 2,
        user_agent: str = "openagent-heat-monitoring",
    ) -> None:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": user_agent,
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._client = httpx.Client(
            base_url=API_ROOT, headers=headers, timeout=timeout, follow_redirects=True
        )
        self.per_page = per_page
        self.rate_limit_floor = rate_limit_floor
        self._rate_limit = RateLimitStatus(remaining=60, limit=60, reset_at=datetime.now(timezone.utc))

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "GitHubClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def rate_limit(self) -> RateLimitStatus:
        return self._rate_limit

    def _update_rate_limit(self, response: httpx.Response) -> None:
        remaining = response.headers.get("X-RateLimit-Remaining")
        limit = response.headers.get("X-RateLimit-Limit")
        reset = response.headers.get("X-RateLimit-Reset")
        if remaining is None:
            return
        reset_at = (
            datetime.fromtimestamp(int(reset), tz=timezone.utc)
            if reset
            else datetime.now(timezone.utc)
        )
        self._rate_limit = RateLimitStatus(
            remaining=int(remaining),
            limit=int(limit) if limit else self._rate_limit.limit,
            reset_at=reset_at,
        )

    def _request(self, path: str, params: dict[str, Any] | None = None, accept: str | None = None):
        if self._rate_limit.remaining <= self.rate_limit_floor:
            raise RateLimitExceeded(
                f"rate limit floor reached ({self._rate_limit.remaining} left)",
                reset_at=self._rate_limit.reset_at,
            )
        headers = {"Accept": accept} if accept else None
        response = self._client.get(path, params=params, headers=headers)
        self._update_rate_limit(response)
        if response.status_code == 401:
            raise AuthenticationRequired(
                f"GitHub requires authentication for {path}; set GITHUB_TOKEN"
            )
        if response.status_code == 403 and "rate limit" in response.text.lower():
            raise RateLimitExceeded("secondary rate limit hit", reset_at=self._rate_limit.reset_at)
        if response.status_code == 403:
            raise PermissionDenied(f"token lacks access to {path}: {response.text[:160]}")
        if response.status_code == 404:
            raise FileNotFoundError(f"GitHub resource not found: {path}")
        response.raise_for_status()
        return response

    # -- repository metadata -------------------------------------------------
    def get_repository(self, owner: str, name: str) -> dict[str, Any]:
        return self._request(f"/repos/{owner}/{name}").json()

    def fetch_rate_limit(self) -> RateLimitStatus:
        """Read the core rate limit without consuming quota."""
        response = self._client.get("/rate_limit")
        response.raise_for_status()
        core = response.json().get("resources", {}).get("core", {})
        status = RateLimitStatus(
            remaining=int(core.get("remaining", 0)),
            limit=int(core.get("limit", 0)),
            reset_at=datetime.fromtimestamp(int(core.get("reset", 0)), tz=timezone.utc),
        )
        self._rate_limit = status
        return status

    def fetch_recent_updated(self, stream: str, owner: str, name: str, page: int = 1):
        """Newest-updated issues/PRs, used to refresh close/merge state."""
        if stream not in ("issues", "pulls"):
            raise ValueError(f"stream {stream} does not support updated refresh")
        path = f"/repos/{owner}/{name}/{stream}"
        params = {"state": "all", "sort": "updated", "direction": "desc", "page": page, "per_page": self.per_page}
        response = self._request(path, params=params)
        raw = response.json()
        if not isinstance(raw, list):
            raw = []
        events = [e for e in (self._normalize(stream, item) for item in raw) if e]
        return events, "next" in response.links

    # -- stream paging (page-by-page, for resumable backfill) ----------------
    def fetch_page(self, stream: str, owner: str, name: str, page: int) -> tuple[list[dict], bool]:
        """Fetch a single page of a stream. Returns (normalized_events, has_next)."""
        path, params, accept = self._stream_request(stream, owner, name)
        query = dict(params)
        query.update({"page": page, "per_page": self.per_page})
        response = self._request(path, params=query, accept=accept)
        raw = response.json()
        if not isinstance(raw, list):
            raw = []
        events = [e for e in (self._normalize(stream, item) for item in raw) if e]
        return events, "next" in response.links

    def iter_stream(
        self, stream: str, owner: str, name: str, start_page: int = 1
    ) -> Iterator[tuple[int, list[dict], bool]]:
        path, params, accept = self._stream_request(stream, owner, name)
        page = start_page
        while True:
            query = dict(params)
            query.update({"page": page, "per_page": self.per_page})
            response = self._request(path, params=query, accept=accept)
            raw = response.json()
            if not isinstance(raw, list):
                raw = []
            events = [e for e in (self._normalize(stream, item) for item in raw) if e]
            yield page, events, "next" in response.links
            if "next" not in response.links:
                break
            page += 1

    @staticmethod
    def _stream_request(stream: str, owner: str, name: str) -> tuple[str, dict, str | None]:
        base = f"/repos/{owner}/{name}"
        if stream == "stars":
            return f"{base}/stargazers", {}, STAR_ACCEPT
        if stream == "forks":
            return f"{base}/forks", {"sort": "oldest"}, None
        if stream == "issues":
            return f"{base}/issues", {"state": "all", "sort": "created", "direction": "asc"}, None
        if stream == "pulls":
            return f"{base}/pulls", {"state": "all", "sort": "created", "direction": "asc"}, None
        if stream == "commits":
            return f"{base}/commits", {}, None
        if stream == "releases":
            return f"{base}/releases", {}, None
        if stream == "contributors":
            return f"{base}/contributors", {"anon": "false"}, None
        raise ValueError(f"unknown stream: {stream}")

    @staticmethod
    def _normalize(stream: str, item: dict[str, Any]) -> dict[str, Any] | None:
        if stream == "stars":
            user = item.get("user") or {}
            return {
                "external_id": str(user.get("login") or user.get("id")),
                "occurred_at": item.get("starred_at"),
                "actor": user.get("login"),
            }
        if stream == "forks":
            owner = item.get("owner") or {}
            return {
                "external_id": str(item.get("id")),
                "occurred_at": item.get("created_at"),
                "actor": owner.get("login"),
            }
        if stream == "issues":
            if item.get("pull_request"):
                return None  # issues endpoint also returns PRs
            return {
                "external_id": str(item.get("id")),
                "occurred_at": item.get("created_at"),
                "closed_at": item.get("closed_at"),
                "state": item.get("state"),
                "actor": (item.get("user") or {}).get("login"),
                "title": item.get("title"),
            }
        if stream == "pulls":
            return {
                "external_id": str(item.get("id")),
                "occurred_at": item.get("created_at"),
                "closed_at": item.get("closed_at"),
                "merged_at": item.get("merged_at"),
                "state": item.get("state"),
                "actor": (item.get("user") or {}).get("login"),
                "title": item.get("title"),
            }
        if stream == "commits":
            commit = item.get("commit") or {}
            author = item.get("author") or {}
            commit_author = commit.get("author") or {}
            return {
                "external_id": str(item.get("sha")),
                "occurred_at": commit_author.get("date") or commit.get("author", {}).get("date"),
                "actor": author.get("login") or commit_author.get("name"),
            }
        if stream == "releases":
            author = item.get("author") or {}
            return {
                "external_id": str(item.get("id")),
                "occurred_at": item.get("published_at") or item.get("created_at"),
                "actor": author.get("login"),
                "title": item.get("name") or item.get("tag_name"),
            }
        if stream == "contributors":
            return {
                "external_id": str(item.get("login") or item.get("id")),
                "occurred_at": None,
                "actor": item.get("login"),
                "contributions": item.get("contributions", 0),
                "avatar_url": item.get("avatar_url"),
            }
        return None
