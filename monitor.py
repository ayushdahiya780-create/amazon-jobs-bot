"""
Amazon Jobs Monitor
Polls amazon.jobs for warehouse/FC postings in Toronto/Mississauga area
and sends Telegram alerts with apply links.
"""

import asyncio
import logging
import json
import hashlib
from datetime import datetime
import httpx
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# Amazon Jobs search API (used by their own site)
SEARCH_URL = "https://www.amazon.jobs/en/search.json"

# All Ontario cities with confirmed Amazon warehouse/FC presence
LOCATIONS = [
    # GTA — multiple FCs (YYZ1, YYZ2, YTO3, YTO4, sortation + delivery stations)
    "Mississauga, Ontario, Canada",
    "Brampton, Ontario, Canada",
    "Toronto, Ontario, Canada",
    "Etobicoke, Ontario, Canada",
    "Scarborough, Ontario, Canada",
    "North York, Ontario, Canada",
    "Vaughan, Ontario, Canada",
    "Markham, Ontario, Canada",
    "Richmond Hill, Ontario, Canada",
    "Ajax, Ontario, Canada",
    "Pickering, Ontario, Canada",
    "Oakville, Ontario, Canada",
    "Burlington, Ontario, Canada",
    "Milton, Ontario, Canada",
    # Ottawa — 4 existing FCs + massive new one under construction
    "Ottawa, Ontario, Canada",
    "Barrhaven, Ontario, Canada",
    "Nepean, Ontario, Canada",
    "Gloucester, Ontario, Canada",
    # Hamilton area
    "Hamilton, Ontario, Canada",
    "Stoney Creek, Ontario, Canada",
    # Kitchener-Waterloo
    "Kitchener, Ontario, Canada",
    "Cambridge, Ontario, Canada",
    # London
    "London, Ontario, Canada",
    # Windsor
    "Windsor, Ontario, Canada",
]

# Warehouse job categories on amazon.jobs
WAREHOUSE_KEYWORDS = [
    "warehouse",
    "fulfillment",
    "sortation",
    "delivery station",
    "picker",
    "packer",
    "stower",
    "associate",
    "FC associate",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Accept": "application/json, text/html",
    "Accept-Language": "en-CA,en;q=0.9",
    "Referer": "https://www.amazon.jobs/",
}


class AmazonJobsMonitor:
    def __init__(self, config: dict, chat_id: int, bot):
        self.config = config
        self.chat_id = chat_id
        self.bot = bot
        self.running = False
        self.seen_job_ids: set = set()
        self.stats = {
            "checks": 0,
            "jobs_found": 0,
            "new_alerts_sent": 0,
            "errors": 0,
            "last_check": "never"
        }

    async def fetch_jobs(self) -> list[dict]:
        """Fetch warehouse jobs for GTA area from amazon.jobs API."""
        all_jobs = []

        async with httpx.AsyncClient(headers=HEADERS, timeout=30, follow_redirects=True) as client:
            for location in LOCATIONS:
                try:
                    params = {
                        "query": "warehouse associate",
                        "location[]": location,
                        "radius": "24km",
                        "facets[]": ["normalized_country_code", "normalized_state_name", "normalized_city_name", "normalized_job_family", "schedule_type_id"],
                        "schedule_type_id[]": [],  # all schedule types
                        "offset": 0,
                        "result_limit": 25,
                        "sort": "recent",
                        "base_query_type": "keyword",
                        "city": location.split(",")[0].strip(),
                        "country": "CAN",
                        "region": "Ontario",
                    }

                    resp = await client.get(SEARCH_URL, params=params)
                    resp.raise_for_status()
                    data = resp.json()

                    jobs = data.get("jobs", [])
                    log.info(f"{location}: {len(jobs)} jobs found")
                    all_jobs.extend(jobs)

                except Exception as e:
                    log.warning(f"Error fetching for {location}: {e}")
                    self.stats["errors"] += 1

                await asyncio.sleep(2)  # polite delay between requests

        # Deduplicate by job ID
        seen = set()
        unique = []
        for j in all_jobs:
            jid = j.get("id") or j.get("job_id")
            if jid and jid not in seen:
                seen.add(jid)
                unique.append(j)

        return unique

    def _parse_job(self, raw: dict) -> dict:
        """Normalize a raw amazon.jobs job dict."""
        job_id = raw.get("id") or raw.get("job_id", "")
        title = raw.get("title", "N/A")
        location = raw.get("location", raw.get("normalized_location", "N/A"))
        posted = raw.get("posted_date", raw.get("updated_time", "N/A"))
        job_path = raw.get("job_path", "")
        url = f"https://www.amazon.jobs{job_path}" if job_path else "https://www.amazon.jobs"
        schedule = raw.get("schedule_type", raw.get("employment_type", "N/A"))
        description = raw.get("description_short", raw.get("description", ""))[:300]

        # Clean posted date
        if posted and "T" in posted:
            try:
                posted = datetime.fromisoformat(posted.replace("Z", "+00:00")).strftime("%b %d, %Y")
            except Exception:
                pass

        return {
            "id": job_id,
            "title": title,
            "location": location,
            "posted": posted,
            "schedule": schedule,
            "url": url,
            "description": description,
        }

    def _is_warehouse_job(self, job: dict) -> bool:
        """Filter to only warehouse/FC type roles."""
        title_lower = job["title"].lower()
        desc_lower = job["description"].lower()
        combined = title_lower + " " + desc_lower

        # Must contain at least one warehouse keyword
        return any(kw in combined for kw in WAREHOUSE_KEYWORDS)

    def _matches_schedule(self, job: dict) -> bool:
        """Check if schedule matches user preference."""
        pref = self.config.get("schedule_filter", "both").lower()
        if pref == "both":
            return True
        schedule = job.get("schedule", "").lower()
        if pref == "full" and "full" in schedule:
            return True
        if pref == "part" and "part" in schedule:
            return True
        # If schedule info is missing, include it anyway
        if not schedule or schedule == "n/a":
            return True
        return False

    async def _notify(self, text: str, disable_preview: bool = True):
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode="Markdown",
                disable_web_page_preview=disable_preview
            )
        except Exception as e:
            log.error(f"Telegram notify failed: {e}")

    async def check_now(self) -> list[dict]:
        """Single check — returns list of new matching jobs."""
        raw_jobs = await self.fetch_jobs()
        self.stats["checks"] += 1
        self.stats["last_check"] = datetime.now().strftime("%H:%M:%S")

        parsed = [self._parse_job(j) for j in raw_jobs]
        warehouse = [j for j in parsed if self._is_warehouse_job(j)]
        scheduled = [j for j in warehouse if self._matches_schedule(j)]
        new = [j for j in scheduled if j["id"] not in self.seen_job_ids]

        self.stats["jobs_found"] += len(new)
        for j in new:
            self.seen_job_ids.add(j["id"])

        return new

    async def run(self):
        """Main monitoring loop."""
        self.running = True
        interval = self.config.get("poll_interval", 300)  # default 5 min for jobs
        log.info(f"Amazon Jobs monitor started (interval={interval}s)")

        # First run — load existing jobs silently (don't alert on startup)
        log.info("Initial scan — loading existing jobs (no alerts)...")
        try:
            existing = await self.check_now()
            log.info(f"Found {len(existing)} existing matching jobs (silently stored)")
        except Exception as e:
            log.error(f"Initial scan error: {e}")

        await self.bot.send_message(
            chat_id=self.chat_id,
            text=(
                "✅ *Amazon Jobs Monitor active!*\n\n"
                f"Watching: Toronto · Mississauga · Brampton + surroundings\n"
                f"Schedule: Full-time & Part-time\n"
                f"Checking every `{interval}` seconds\n\n"
                "You'll get an alert the moment a new warehouse posting goes live. 🔔"
            ),
            parse_mode="Markdown"
        )

        while self.running:
            await asyncio.sleep(interval)

            if not self.running:
                break

            try:
                new_jobs = await self.check_now()

                for job in new_jobs:
                    self.stats["new_alerts_sent"] += 1
                    schedule_tag = f" · {job['schedule']}" if job['schedule'] != 'N/A' else ""
                    msg = (
                        f"🚨 *New Amazon Job Posted!*\n\n"
                        f"*{job['title']}*\n"
                        f"📍 {job['location']}\n"
                        f"🗓 Posted: {job['posted']}{schedule_tag}\n\n"
                        f"[👉 Apply Now]({job['url']})\n\n"
                        f"_{job['description'][:200]}..._" if job['description'] else ""
                    )
                    await self._notify(msg, disable_preview=False)

                if new_jobs:
                    log.info(f"Sent {len(new_jobs)} new job alerts")
                else:
                    log.info(f"Check complete — no new jobs")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Monitor loop error: {e}")
                self.stats["errors"] += 1

        self.running = False
        log.info("Monitor stopped")
