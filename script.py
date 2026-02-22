#!/usr/bin/env python3
"""
Install + run:
    pip install playwright
    playwright install
    python script.py input.txt --concurrency 5

Blastup-only Instagram follower checker, built for reliability at 500+ usernames.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import random
import re
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from playwright.async_api import Browser, Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

BLASTUP_URL = "https://blastup.com/instagram-follower-count?{username}"
OUTPUT_CSV = "followers.csv"

URL_RE = re.compile(r"https?://(?:www\.)?instagram\.com/([A-Za-z0-9._]{1,30})(?:/|\b)", re.IGNORECASE)
MENTION_RE = re.compile(r"(?<![A-Za-z0-9._])@([A-Za-z0-9._]{1,30})(?![A-Za-z0-9._])")
TOKEN_RE = re.compile(r"(?<![A-Za-z0-9._])([A-Za-z0-9._]{1,30})(?![A-Za-z0-9._])")
VALID_USERNAME_RE = re.compile(r"^[A-Za-z0-9._]{1,30}$")
FOLLOWER_TEXT_RE = re.compile(
    r"(?i)([0-9][0-9,]*(?:\.[0-9]+)?\s*[KMB]?)\s*(?:followers?|follower\s*count)"
)
RAW_NUMBER_RE = re.compile(r"(?i)^\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*([KMB]?)\s*$")


@dataclass
class RowResult:
    username: str
    followers_raw: str
    followers_number: int | None
    status: str
    error: str


class ProgressTracker:
    """Prints clean periodic progress updates suitable for Windows CMD."""

    def __init__(self, total: int, progress_interval: int) -> None:
        self.total = total
        self.progress_interval = max(1, progress_interval)
        self.started_at = time.monotonic()
        self.last_print_at = self.started_at

        self.completed = 0
        self.ok_count = 0
        self.failed_count = 0
        self._completion_times: deque[float] = deque(maxlen=50)

    def print_starting(self, concurrency: int) -> None:
        print(
            f"Starting... total usernames: {self.total}, concurrency: {concurrency}",
            flush=True,
        )

    def _format_eta(self, eta_seconds: float) -> str:
        eta_seconds = max(0, int(eta_seconds))
        hours, rem = divmod(eta_seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:02d}:{seconds:02d}"

    def _rolling_seconds_per_item(self) -> float | None:
        if len(self._completion_times) < 2:
            return None
        window_elapsed = self._completion_times[-1] - self._completion_times[0]
        window_items = len(self._completion_times) - 1
        if window_elapsed <= 0 or window_items <= 0:
            return None
        return window_elapsed / window_items

    def maybe_print(self, row: RowResult, force: bool = False) -> None:
        self.completed += 1
        if row.status == "ok":
            self.ok_count += 1
        else:
            self.failed_count += 1

        now = time.monotonic()
        self._completion_times.append(now)

        due_to_count = self.completed % self.progress_interval == 0
        due_to_time = (now - self.last_print_at) >= 30
        finished = self.completed == self.total
        if not (force or due_to_count or due_to_time or finished):
            return

        elapsed = now - self.started_at
        avg_speed = self.completed / elapsed if elapsed > 0 else 0.0
        sec_per_item = self._rolling_seconds_per_item()
        if sec_per_item is None:
            sec_per_item = elapsed / self.completed if self.completed else 0.0

        remaining = self.total - self.completed
        eta = sec_per_item * remaining
        percent = (self.completed / self.total) * 100 if self.total else 100.0

        print(
            "Progress "
            f"{self.completed}/{self.total} "
            f"({percent:.1f}%) | "
            f"ok: {self.ok_count} | "
            f"failed: {self.failed_count} | "
            f"speed: {avg_speed:.3f} acc/s | "
            f"ETA: {self._format_eta(eta)}",
            flush=True,
        )
        self.last_print_at = now


class AdaptiveController:
    """Controls effective concurrency and delay scaling based on recent failures."""

    def __init__(self, max_concurrency: int, min_delay: float, max_delay: float) -> None:
        self.max_concurrency = max(1, max_concurrency)
        self.target_concurrency = self.max_concurrency
        self._active = 0
        self._cond = asyncio.Condition()

        self.base_min_delay = min_delay
        self.base_max_delay = max_delay
        self.delay_scale = 1.0
        self._recent: deque[bool] = deque(maxlen=25)

    async def acquire_slot(self) -> None:
        async with self._cond:
            while self._active >= self.target_concurrency:
                await self._cond.wait()
            self._active += 1

    async def release_slot(self) -> None:
        async with self._cond:
            self._active -= 1
            self._cond.notify_all()

    async def adaptive_pause(self) -> None:
        min_d = self.base_min_delay * self.delay_scale
        max_d = self.base_max_delay * self.delay_scale
        await asyncio.sleep(random.uniform(min_d, max_d))

    async def record_outcome(self, ok: bool) -> None:
        async with self._cond:
            self._recent.append(ok)
            if len(self._recent) < 6:
                return

            failure_rate = 1 - (sum(self._recent) / len(self._recent))

            # If failures spike, increase pacing and reduce effective concurrency temporarily.
            if failure_rate >= 0.40:
                self.delay_scale = min(3.0, self.delay_scale * 1.25)
                if self.target_concurrency > 1:
                    self.target_concurrency -= 1
                self._cond.notify_all()
                return

            # If conditions stabilize, gradually restore normal speed.
            if failure_rate <= 0.20:
                self.delay_scale = max(1.0, self.delay_scale * 0.92)
                if self.target_concurrency < self.max_concurrency:
                    self.target_concurrency += 1
                self._cond.notify_all()


def clean_token(token: str) -> str:
    """Normalize punctuation around tokens and strip leading @."""
    token = token.strip().strip("\"'`()[]{}<>,;:!?|\\")
    if token.startswith("@"):
        token = token[1:]
    return token.strip().rstrip("/.")


def normalize_candidates(raw_text: str) -> list[str]:
    """
    Extract usernames from messy mixed input while preserving order + uniqueness.

    Supports:
      - @username
      - username
      - instagram URLs
      - messy text containing one/more usernames
    """
    seen: set[str] = set()
    normalized: list[str] = []

    lines = raw_text.splitlines()
    for line in lines:
        line_hits: list[str] = []
        stripped = line.strip()
        if not stripped:
            continue

        for m in URL_RE.finditer(stripped):
            line_hits.append(m.group(1))

        for m in MENTION_RE.finditer(stripped):
            line_hits.append(m.group(1))

        if not line_hits:
            tokenized = [clean_token(t) for t in stripped.split()]
            for tok in tokenized:
                if VALID_USERNAME_RE.fullmatch(tok):
                    line_hits.append(tok)

        if not line_hits:
            for m in TOKEN_RE.finditer(stripped):
                line_hits.append(m.group(1))

        for candidate in line_hits:
            user = clean_token(candidate)
            if not VALID_USERNAME_RE.fullmatch(user):
                continue
            lowered = user.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            normalized.append(user)

    return normalized


def parse_follower_number(raw_value: str) -> int | None:
    match = RAW_NUMBER_RE.match(raw_value)
    if not match:
        return None

    value = float(match.group(1).replace(",", ""))
    suffix = match.group(2).upper()
    multiplier = 1
    if suffix == "K":
        multiplier = 1_000
    elif suffix == "M":
        multiplier = 1_000_000
    elif suffix == "B":
        multiplier = 1_000_000_000

    return int(value * multiplier)


async def extract_followers_from_page(page) -> tuple[str | None, int | None, str | None]:
    """Try multiple extraction strategies so DOM changes are less likely to break runs."""
    selectors = [
        "text=/followers?/i",
        "[class*='follower' i]",
        "[id*='follower' i]",
    ]

    for selector in selectors:
        try:
            loc = page.locator(selector).first
            if await loc.count() > 0:
                txt = (await loc.inner_text(timeout=1500)).strip()
                m = FOLLOWER_TEXT_RE.search(txt)
                if m:
                    raw = m.group(1).replace(" ", "")
                    return raw, parse_follower_number(raw), None
        except PlaywrightError:
            pass

    full_text = await page.locator("body").inner_text(timeout=3000)

    m = FOLLOWER_TEXT_RE.search(full_text)
    if m:
        raw = m.group(1).replace(" ", "")
        return raw, parse_follower_number(raw), None

    near_followers_re = re.compile(
        r"(?i)(?:followers?|follower\s*count)[^\d]{0,25}([0-9][0-9,]*(?:\.[0-9]+)?\s*[KMB]?)"
    )
    m2 = near_followers_re.search(full_text)
    if m2:
        raw = m2.group(1).replace(" ", "")
        return raw, parse_follower_number(raw), None

    return None, None, "Follower count not found in rendered page text"


async def fetch_one(
    page,
    username: str,
    retries: int,
    nav_timeout_ms: int,
    selector_timeout_ms: int,
    controller: AdaptiveController,
) -> RowResult:
    url = BLASTUP_URL.format(username=username)

    for attempt in range(1, retries + 1):
        await controller.adaptive_pause()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
            await page.wait_for_load_state("networkidle", timeout=selector_timeout_ms)

            raw, number, extract_err = await extract_followers_from_page(page)
            if raw is not None and number is not None:
                await controller.record_outcome(ok=True)
                return RowResult(username, raw, number, "ok", "")

            error_msg = extract_err or "Unable to parse follower count"
            if attempt == retries:
                await controller.record_outcome(ok=False)
                return RowResult(username, raw or "", None, "failed", error_msg)

        except (PlaywrightTimeoutError, PlaywrightError) as exc:
            if attempt == retries:
                await controller.record_outcome(ok=False)
                return RowResult(username, "", None, "failed", str(exc))

        backoff = (2 ** (attempt - 1)) + random.uniform(0.2, 1.2)
        await asyncio.sleep(backoff)

    await controller.record_outcome(ok=False)
    return RowResult(username, "", None, "failed", "Unknown failure")


async def run_scrape(
    usernames: list[str],
    concurrency: int,
    min_delay: float,
    max_delay: float,
    retries: int,
    headed: bool,
    nav_timeout_ms: int,
    selector_timeout_ms: int,
    progress_interval: int,
) -> list[RowResult]:
    results: list[RowResult] = []
    results_lock = asyncio.Lock()
    queue: asyncio.Queue[str] = asyncio.Queue()

    # Cache guard in case duplicates slip through future input changes.
    fetched_cache: dict[str, RowResult] = {}

    for username in usernames:
        await queue.put(username)

    tracker = ProgressTracker(total=len(usernames), progress_interval=progress_interval)
    tracker.print_starting(concurrency=concurrency)

    controller = AdaptiveController(concurrency, min_delay, max_delay)

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(headless=not headed)

        contexts = []
        pages = []
        for _ in range(concurrency):
            ctx = await browser.new_context()
            page = await ctx.new_page()
            contexts.append(ctx)
            pages.append(page)

        async def worker(page_index: int) -> None:
            page = pages[page_index]
            while True:
                try:
                    username = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return

                await controller.acquire_slot()
                try:
                    if username in fetched_cache:
                        row = fetched_cache[username]
                    else:
                        row = await fetch_one(
                            page=page,
                            username=username,
                            retries=retries,
                            nav_timeout_ms=nav_timeout_ms,
                            selector_timeout_ms=selector_timeout_ms,
                            controller=controller,
                        )
                        fetched_cache[username] = row

                    async with results_lock:
                        results.append(row)
                        tracker.maybe_print(row)
                finally:
                    await controller.release_slot()
                    queue.task_done()

        workers = [asyncio.create_task(worker(i)) for i in range(concurrency)]
        await asyncio.gather(*workers)

        for page in pages:
            await page.close()
        for ctx in contexts:
            await ctx.close()
        await browser.close()

    by_username = {r.username.lower(): r for r in results}
    return [by_username[u.lower()] for u in usernames]


def write_csv(rows: Iterable[RowResult], output_file: Path) -> None:
    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["username", "followers_raw", "followers_number", "status", "error"])
        for row in rows:
            writer.writerow(
                [
                    row.username,
                    row.followers_raw,
                    "" if row.followers_number is None else row.followers_number,
                    row.status,
                    row.error,
                ]
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Blastup follower checker for many usernames")
    parser.add_argument("input", help="Path to input.txt")
    parser.add_argument("--concurrency", type=int, default=4, help="Parallel pages (default: 4)")
    parser.add_argument("--min-delay", type=float, default=1.2, help="Minimum jitter delay seconds")
    parser.add_argument("--max-delay", type=float, default=2.8, help="Maximum jitter delay seconds")
    parser.add_argument("--retries", type=int, default=3, help="Retries per username")
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=10,
        help="Print progress every N completions (default: 10)",
    )
    parser.add_argument("--headed", action="store_true", help="Run browser in headed mode")
    parser.add_argument("--nav-timeout-ms", type=int, default=45_000, help="Navigation timeout")
    parser.add_argument(
        "--selector-timeout-ms", type=int, default=18_000, help="Selector/network wait timeout"
    )
    return parser


async def async_main(args: argparse.Namespace) -> int:
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 1

    raw_text = input_path.read_text(encoding="utf-8", errors="ignore")
    usernames = normalize_candidates(raw_text)

    if not usernames:
        print("No valid Instagram usernames found in input file.")
        return 1

    start = time.time()
    rows = await run_scrape(
        usernames=usernames,
        concurrency=max(1, args.concurrency),
        min_delay=max(0.0, args.min_delay),
        max_delay=max(args.min_delay, args.max_delay),
        retries=max(1, args.retries),
        headed=args.headed,
        nav_timeout_ms=max(5_000, args.nav_timeout_ms),
        selector_timeout_ms=max(3_000, args.selector_timeout_ms),
        progress_interval=max(1, args.progress_interval),
    )
    elapsed = time.time() - start

    output_path = Path(OUTPUT_CSV)
    write_csv(rows, output_path)

    ok_rows = [r for r in rows if r.status == "ok" and r.followers_number is not None]
    failed_rows = [r for r in rows if r.status != "ok"]
    total_followers = sum(r.followers_number for r in ok_rows if r.followers_number is not None)

    print(f"Output CSV path: {output_path.resolve()}")
    print(f"Total usernames processed: {len(rows)}")
    print(f"OK count: {len(ok_rows)}")
    print(f"Failed count: {len(failed_rows)}")
    print(f"Total followers sum: {total_followers}")
    print(f"Elapsed seconds: {elapsed:.2f}")

    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
