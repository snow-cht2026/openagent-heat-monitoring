from app.collector import parse_dt
from app.github_client import GitHubClient


def test_normalize_star():
    event = GitHubClient._normalize("stars", {"user": {"login": "alice"}, "starred_at": "2024-01-01T00:00:00Z"})
    assert event["external_id"] == "alice"
    assert event["actor"] == "alice"


def test_normalize_issue_skips_pull_requests():
    assert GitHubClient._normalize("issues", {"pull_request": {"url": "x"}}) is None
    event = GitHubClient._normalize(
        "issues",
        {"id": 5, "created_at": "2024-01-01T00:00:00Z", "state": "open", "user": {"login": "bob"}},
    )
    assert event["external_id"] == "5"
    assert event["state"] == "open"


def test_normalize_commit_uses_sha_and_date():
    event = GitHubClient._normalize(
        "commits",
        {"sha": "abc", "commit": {"author": {"date": "2024-02-02T10:00:00Z", "name": "Ann"}}, "author": None},
    )
    assert event["external_id"] == "abc"
    assert event["actor"] == "Ann"


def test_parse_dt_handles_zulu_and_naive():
    assert parse_dt("2024-01-01T00:00:00Z").tzinfo is not None
    assert parse_dt("2024-01-01T00:00:00").tzinfo is not None
    assert parse_dt(None) is None
    assert parse_dt("not-a-date") is None
