"""Tests for crawl orchestration and graceful deadline handling."""
import asyncio

from src.config import Config
from src.main import fetch_due_companies
from src.models import Company


class FakeAdapter:
    def __init__(self, delay=0, error=None):
        self.delay = delay
        self.error = error
        self.cancelled = False

    async def fetch(self, session, company, **kwargs):
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        if self.error:
            raise self.error
        return []


def test_fetch_due_companies_returns_successes_errors_and_timed_out_boards(monkeypatch):
    import src.main as main

    fast = FakeAdapter()
    slow = FakeAdapter(delay=1)
    failed = FakeAdapter(error=RuntimeError("fixture failure"))
    monkeypatch.setattr(
        main,
        "ADAPTERS",
        {
            "greenhouse": fast,
            "lever": slow,
            "ashby": failed,
        },
    )
    companies = [
        Company("Fast", "greenhouse", "fast", 1),
        Company("Slow", "lever", "slow", 2),
        Company("Failing", "ashby", "failing", 3),
        Company("Unknown", "unknown", "unknown", 3),
    ]

    async def run():
        return await fetch_due_companies(
            session=None,
            companies=companies,
            cfg=Config(),
            budget_seconds=0.05,
        )

    results, skipped = asyncio.run(run())
    assert results[companies[0]] == []
    assert isinstance(results[companies[2]], RuntimeError)
    assert companies[1] in skipped
    assert companies[3] in skipped
    assert slow.cancelled is True


def test_run_exits_1_and_keeps_jobs_pending_when_email_fails(tmp_path, monkeypatch, capsys):
    """A failed send must not mark jobs alerted, and must make the run exit 1."""
    import argparse
    from pathlib import Path

    import src.main as main
    from src.filters import FilterResult
    from src.models import Job
    from src.store import Store

    repo = Path(main.__file__).resolve().parents[1]
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    companies_csv = tmp_path / "companies.csv"
    companies_csv.write_text("name,ats,slug,tier\nAcme,greenhouse,acme,1\n", encoding="utf-8")

    company = Company("Acme", "greenhouse", "acme", 1)
    match = FilterResult(True, country="India", min_exp=0.0, exp_signal="title_entry")

    def job(native_id, title):
        return Job(
            job_id=f"greenhouse:acme:{native_id}", company_key=company.key, company="Acme",
            ats="greenhouse", title=title, location="Bengaluru, India",
            url=f"https://example.com/acme/{native_id}", description="", posted_at=None,
            salary=None, remote=None,
        )

    store = Store("data/jobs.db")
    store.sync_companies([company])
    store.apply_crawl(company, [(job("seed", "Accountant"), FilterResult(False))], "2026-10-01T00:00:00Z")
    store.apply_crawl(
        company,
        [(job("seed", "Accountant"), FilterResult(False)), (job("new", "SDE Intern"), match)],
        "2026-10-02T00:00:00Z",
    )
    assert len(store.pending_alerts()) == 1
    store.close()

    async def no_fetch(session, companies, cfg, budget_seconds):
        return {}, []

    def failing_send(*args, **kwargs):
        raise OSError("fixture SMTP failure")

    monkeypatch.setattr(main, "fetch_due_companies", no_fetch)
    monkeypatch.setattr(main, "send_email", failing_send)
    monkeypatch.setattr(main, "load_env_file", lambda *a, **k: None)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")

    args = argparse.Namespace(
        config=str(repo / "config" / "config.yaml"),
        companies=str(companies_csv),
        company_meta=str(tmp_path / "missing_meta.csv"),
        dry_run=False, no_email=False, tier=None, company=None, limit=None,
    )
    assert asyncio.run(main.run_crawl(args)) == 1

    out = capsys.readouterr().out
    assert "run ok |" in out
    assert "::error title=Email not sent::Alert email failed: OSError: fixture SMTP failure" in out

    store = Store("data/jobs.db")
    assert len(store.pending_alerts()) == 1
    store.close()
