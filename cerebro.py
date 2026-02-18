#!/usr/bin/env python3
"""
CEREBRO — Continuous Extraction & Remote Evaluation By Rule Optimization
A practical job alert pipeline: scrape → filter → AI score → email.
"""

import hashlib
import json
import logging
import os
import re
import sqlite3
import smtplib
import time
import urllib.request
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import yaml

from jobspy import scrape_jobs

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("cerebro")

DB_PATH = Path(__file__).parent / "cerebro.db"
CONFIG_PATH = Path(__file__).parent / "config.yaml"

# ── Jaison's resume summary for AI scoring (condensed for prompt efficiency) ──
RESUME_SUMMARY = """
Jaison George — 8 YOE across private equity, corporate development, and product leadership.

EXPERIENCE:
- Head of Product & Finance Strategy, Neptyne (YC startup), 2022–2025
  Led product for Pythonic spreadsheet platform. 65+ customer interviews, 35+ features shipped.
  Built 3-year financial model, reduced OpEx 30% via AI tools. AI coding assistant feature.
  GTM strategy: 40% trial-to-paid conversion. React/Python/PostHog/FullStory stack.

- Senior Associate, Corporate Development, Google / Sidewalk Labs, 2019–2022
  Led acquisition & investment strategy for tech verticals. Built $4.1B P&L model.
  Managed end-to-end deal process for $3.1B mixed-use development (Toronto).
  ROI framework for ML/deep learning initiatives. Strategic partnerships with McKinsey, BCG.

- Associate, The Blackstone Group ($315B PE), 2018–2019
  BREP VIII fund: 184 investments across industrial, multifamily, retail.
  Completed $2B Vivint acquisition. $135M divestiture. 400bps EBITDA margin expansion.

- Investment Analyst, Leon Capital Group ($3B PE), 2016–2018
  49 acquisitions totaling $1.1B. Python GIS algorithm for deal sourcing.
  Post-acquisition value creation playbooks.

EDUCATION: BS Finance & Computer Science, Wayne State University (Honors)
SKILLS: Python, JAX, React, Node.js, SQL, Financial Modeling, Bloomberg, ARGUS, CoStar
SEEKING: Remote or NYC hybrid. Open to junior-through-senior. Needs schedule flexibility (building a side project).
PRIORITY INDUSTRIES: PropTech, fintech, AI/ML, spreadsheet/data tools — structural advantages here.
COMPANY SIZE: Growth-stage (Series B-D) or enterprise. Early-stage only if role is an exact background match.
"""

# ── Resume variant mapping ──
RESUME_VARIANTS = {
    "corp_dev": {
        "file": "Block - Corp Dev/Jaison_George - Resume.pdf",
        "label": "Corp Dev (Block variant)",
        "keywords": ["corporate development", "corp dev", "m&a", "mergers", "acquisitions",
                     "strategic partnerships", "due diligence", "investment committee"],
    },
    "strategic_finance": {
        "file": "Brex/Jaison_George - Resume.pdf",
        "label": "Strategic Finance (Brex variant)",
        "keywords": ["strategic finance", "fp&a", "financial planning", "financial analyst",
                     "finance manager", "budget", "variance analysis", "p&l", "revenue operations",
                     "bizops", "business operations", "chief of staff"],
    },
    "product": {
        "file": "Datadog/Jaison_George - Resume.pdf",
        "label": "Product Management variant",
        "keywords": ["product manager", "product owner", "product lead", "product strategy",
                     "head of product", "program manager", "technical product", "roadmap",
                     "user research", "backlog"],
    },
    "real_estate": {
        "file": "New Folder With Items/Jaison_George - Resume - Real Estate.pdf",
        "label": "Real Estate variant",
        "keywords": ["real estate", "acquisitions", "asset management", "underwriting",
                     "proptech", "multifamily", "commercial real estate", "reit",
                     "investment analyst", "investment associate"],
    },
}


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def init_db(db: sqlite3.Connection):
    db.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            fingerprint TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            url TEXT,
            title TEXT,
            company TEXT,
            location TEXT,
            description TEXT,
            salary_min REAL,
            salary_max REAL,
            date_posted TEXT,
            is_remote BOOLEAN,
            ingested_at TEXT DEFAULT (datetime('now')),
            is_match BOOLEAN,
            rejection_reason TEXT,
            matched_poison_token TEXT,
            evaluated_at TEXT,
            ai_score REAL,
            ai_reasoning TEXT,
            recommended_resume TEXT,
            scored_at TEXT,
            notified_at TEXT
        )
    """)
    # Migrate existing DBs that lack new columns
    try:
        db.execute("ALTER TABLE jobs ADD COLUMN ai_score REAL")
    except sqlite3.OperationalError:
        pass
    try:
        db.execute("ALTER TABLE jobs ADD COLUMN ai_reasoning TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        db.execute("ALTER TABLE jobs ADD COLUMN recommended_resume TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        db.execute("ALTER TABLE jobs ADD COLUMN scored_at TEXT")
    except sqlite3.OperationalError:
        pass
    db.commit()


def fingerprint(company, title, date_posted) -> str:
    """Deterministic content hash for cross-platform dedup."""
    c = str(company).lower().strip() if company and str(company) != "nan" else ""
    t = str(title).lower().strip() if title and str(title) != "nan" else ""
    d = str(date_posted).strip() if date_posted and str(date_posted) != "nan" else ""
    return hashlib.sha256(f"{c}|{t}|{d}".encode()).hexdigest()[:16]


# ── Phase 1: Ingest ──────────────────────────────────────────────────────────

def ingest(config: dict, db: sqlite3.Connection):
    total_new = 0
    total_dupes = 0

    for search in config["searches"]:
        log.info(f"Scraping: '{search['term']}' in {search.get('location', 'anywhere')}")
        try:
            df = scrape_jobs(
                site_name=search.get("sites"),
                search_term=search["term"],
                location=search.get("location"),
                results_wanted=search.get("results_wanted", 25),
                hours_old=search.get("hours_old", 24),
                country_indeed=search.get("country", "USA"),
                is_remote=False if search.get("nyc_hybrid") else config["filters"].get("require_remote", False),
                verbose=0,
            )
        except Exception as e:
            log.error(f"Scrape failed for '{search['term']}': {e}")
            continue

        if df.empty:
            log.info("  No results.")
            continue

        new, dupes = 0, 0
        for _, row in df.iterrows():
            fp = fingerprint(row.get("company"), row.get("title"), str(row.get("date_posted", "")))
            try:
                db.execute(
                    """INSERT OR IGNORE INTO jobs
                       (fingerprint, source, url, title, company, location,
                        description, salary_min, salary_max, date_posted, is_remote)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        fp,
                        row.get("site", ""),
                        row.get("job_url", ""),
                        row.get("title", ""),
                        row.get("company", ""),
                        row.get("location", ""),
                        row.get("description", ""),
                        row.get("min_amount"),
                        row.get("max_amount"),
                        str(row.get("date_posted", "")),
                        bool(row.get("is_remote")),
                    ),
                )
                if db.total_changes:
                    new += 1
                else:
                    dupes += 1
            except Exception as e:
                log.warning(f"  Insert error: {e}")
                dupes += 1

        db.commit()
        total_new += new
        total_dupes += dupes
        log.info(f"  {len(df)} scraped, {new} new, {dupes} dupes")

    log.info(f"Ingest complete: {total_new} new jobs, {total_dupes} dupes skipped")


# ── Phase 2: Evaluate ────────────────────────────────────────────────────────

# False positive patterns to auto-reject regardless of title match
FALSE_POSITIVE_PATTERNS = [
    (r'\bcounsel\b.*\breal estate\b|\breal estate\b.*\bcounsel\b', "false_positive", "lawyer role"),
    (r'\battorney\b|\blegal counsel\b|\bjuris doctor\b|\bbar admission\b|\blaw firm\b|\bamlaw\b', "false_positive", "lawyer role"),
    (r'\bpartner\b.*\b(m&a|private equity|capital markets|finance|funds|transactions)\b.*\b(attorney|firm|law)\b', "false_positive", "lawyer role"),
    (r'\bappraiser\b', "false_positive", "appraiser role"),
    (r'\bunderwriter\b.*\binsurance\b|\binsurance\b.*\bunderwriter\b', "false_positive", "insurance underwriter"),
    (r'\bwaterproofing\b|\broofing\b|\bhvac\b|\bplumbing\b', "false_positive", "construction/trades"),
    # Traditional RE — want proptech/RE+tech, not old-school finance RE
    (r'\boriginations?\b', "false_positive", "traditional RE"),
    (r'\bpre-mba\b|\b20\d{2}\s+program\b', "false_positive", "traditional RE/rotational"),
    (r'\bmortgage\b(?!.*\btech\b)', "false_positive", "traditional RE"),
    (r'\bleasing\s+(agent|manager|associate|coordinator)\b', "false_positive", "traditional RE"),
    (r'\bproperty manag(er|ement)\b(?!.*\btech\b|\bsoftware\b|\bplatform\b)', "false_positive", "traditional RE"),
    (r'\bloan\s+officer\b|\btitle\s+officer\b|\bescrow\b', "false_positive", "traditional RE"),
    (r'\breal estate\b.*\b(financing|investing|debt|equity)\b', "false_positive", "traditional RE"),
    (r'\b(asset management|portfolio management)\b(?!.*\btech\b|\bsoftware\b|\bproduct\b)', "false_positive", "traditional RE"),
]

FALSE_POSITIVE_COMPANIES = [
    # Applied / ghosted
    "meta",  "google",  "harvey",  "coinbase",  "datadog",
    "headway",  "found",  "realpage",  "wing",  "lifetime",
    "ford",  "toyota",  "crusoe",  "jll",  "welltower",
    "endex",  "block",  "verra",  "pulley",  "brex",
    "smartsheet",
    # Spam / recruiters
    "lensa",  "platinum legal",  "bcg attorney",  "dataannotation",
]


def evaluate(config: dict, db: sqlite3.Connection):
    filters = config["filters"]
    target_titles = [t.lower() for t in filters.get("target_titles", [])]
    poison_tokens = [t.lower() for t in filters.get("poison_tokens", [])]
    exclude_seniority = [s.lower() for s in filters.get("exclude_seniority", [])]
    require_remote = filters.get("require_remote", False)
    min_salary = filters.get("min_salary", 0)

    allow_hybrid_locs = [loc.lower() for loc in filters.get("allow_hybrid_locations", [])]

    max_age_days = filters.get("max_age_days", 14)

    rows = db.execute("SELECT fingerprint, title, company, description, is_remote, salary_max, location, date_posted FROM jobs WHERE evaluated_at IS NULL").fetchall()
    if not rows:
        log.info("Nothing to evaluate.")
        return

    now = datetime.now(timezone.utc).isoformat()
    matched, rejected = 0, 0

    for fp, title, company, description, is_remote, salary_max, location, date_posted in rows:
        title_lower = (title or "").lower()
        company_lower = (company or "").lower()
        desc_lower = (description or "").lower()
        corpus = title_lower + " " + desc_lower

        # Filter 0: false positive companies
        if any(bad in company_lower for bad in FALSE_POSITIVE_COMPANIES):
            _reject(db, fp, now, "false_positive", company_lower.strip())
            rejected += 1
            continue

        # Filter 0b: false positive patterns (lawyers, appraisers, trades)
        fp_hit = None
        for pattern, reason, label in FALSE_POSITIVE_PATTERNS:
            if re.search(pattern, corpus, re.IGNORECASE):
                fp_hit = label
                break
        if fp_hit:
            _reject(db, fp, now, "false_positive", fp_hit)
            rejected += 1
            continue

        # Filter 1: title must match at least one target
        if target_titles and not any(t in title_lower for t in target_titles):
            _reject(db, fp, now, "title_mismatch")
            rejected += 1
            continue

        # Filter 2: poison tokens in title or description (word-boundary aware)
        poison_hit = next((tok for tok in poison_tokens if re.search(r'\b' + re.escape(tok) + r'\b', corpus)), None)
        if poison_hit:
            _reject(db, fp, now, "poison_token", poison_hit)
            rejected += 1
            continue

        # Filter 3: must be remote OR in an allowed hybrid location (e.g. NYC)
        if require_remote and not is_remote:
            loc_lower = (location or "").lower()
            in_allowed_area = any(loc in loc_lower for loc in allow_hybrid_locs)
            if not in_allowed_area:
                _reject(db, fp, now, "geo_restricted")
                rejected += 1
                continue

        # Filter 4: exclude seniority levels (word-boundary aware)
        seniority_hit = next((s for s in exclude_seniority if re.search(r'\b' + re.escape(s) + r'\b', title_lower)), None)
        if seniority_hit:
            _reject(db, fp, now, "seniority_excluded", seniority_hit)
            rejected += 1
            continue

        # Filter 5: salary floor (only reject if salary IS listed and is too low)
        if min_salary and salary_max and salary_max < min_salary:
            _reject(db, fp, now, "salary_too_low", f"${salary_max:,.0f}")
            rejected += 1
            continue

        # Filter 6: reject stale postings (only if date is known)
        if max_age_days and date_posted and str(date_posted) not in ("nan", "NaT", ""):
            try:
                posted = datetime.strptime(str(date_posted)[:10], "%Y-%m-%d").date()
                age = (datetime.now(timezone.utc).date() - posted).days
                if age > max_age_days:
                    _reject(db, fp, now, "stale_posting", f"{age}d old")
                    rejected += 1
                    continue
            except (ValueError, TypeError):
                pass

        # Passed all filters
        db.execute(
            "UPDATE jobs SET is_match = 1, evaluated_at = ? WHERE fingerprint = ?",
            (now, fp),
        )
        matched += 1

    db.commit()
    log.info(f"Evaluated {len(rows)} jobs: {matched} matched, {rejected} rejected")


def _reject(db: sqlite3.Connection, fp: str, now: str, reason: str, token: str = None):
    db.execute(
        "UPDATE jobs SET is_match = 0, rejection_reason = ?, matched_poison_token = ?, evaluated_at = ? WHERE fingerprint = ?",
        (reason, token, now, fp),
    )


# ── Phase 3: AI Score + Resume Recommendation ────────────────────────────────

def _pick_resume(title: str, description: str) -> str:
    """Pick best resume variant based on keyword density in title + description."""
    text = (title or "").lower() + " " + (description or "").lower()
    scores = {}
    for variant_key, variant in RESUME_VARIANTS.items():
        scores[variant_key] = sum(1 for kw in variant["keywords"] if kw in text)

    best = max(scores, key=scores.get)
    if scores[best] == 0:
        return RESUME_VARIANTS["product"]["label"]  # Default to product (broadest)
    return RESUME_VARIANTS[best]["label"]


def _call_gemini(prompt: str) -> str:
    """Call Gemini Flash via REST API."""
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 1024},
    }).encode()

    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    resp = urllib.request.urlopen(req, timeout=30)
    data = json.loads(resp.read())
    return data["candidates"][0]["content"]["parts"][0]["text"]


def score(db: sqlite3.Connection):
    """AI-score matched jobs via comparative ranking (not individual scoring).

    Individual scoring fails because LLMs anchor every pre-filtered job at the same
    score. Instead we:
      1. Set resume recommendations for all unscored matches.
      2. Send ALL matched job titles+companies to Gemini in ONE prompt.
      3. Ask it to rank them into tiers (S/A/B/C) via forced comparison.
      4. Map tiers → numeric scores (S=9, A=7, B=5, C=3).
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    now = datetime.now(timezone.utc).isoformat()

    # Always set resume recommendations (even without API key)
    unscored = db.execute(
        "SELECT fingerprint, title, description FROM jobs WHERE is_match = 1 AND scored_at IS NULL"
    ).fetchall()
    for fp, title, desc in unscored:
        resume = _pick_resume(title, desc)
        db.execute(
            "UPDATE jobs SET recommended_resume = ?, scored_at = ? WHERE fingerprint = ?",
            (resume, now, fp),
        )

    if not api_key:
        db.commit()
        if unscored:
            log.info(f"Resume recommendations set for {len(unscored)} jobs (no AI scoring).")
        return

    # Fetch ALL matched jobs for comparative ranking (including previously scored)
    all_matches = db.execute(
        "SELECT fingerprint, title, company, salary_min, salary_max FROM jobs WHERE is_match = 1"
    ).fetchall()

    if not all_matches:
        db.commit()
        log.info("No jobs to score.")
        return

    log.info(f"AI ranking {len(all_matches)} jobs via comparative analysis...")

    # Process in batches of 50 to avoid token limits
    BATCH_SIZE = 50
    tier_map = {}  # global_index -> tier letter
    tier_scores = {"S": 9, "A": 7, "B": 5, "C": 3}
    tier_reasons = {
        "S": "Top-tier match — role + company directly leverage your hybrid background",
        "A": "Strong fit — clear overlap with your experience, competitive candidate",
        "B": "Decent match — some overlap, would require positioning your fit",
        "C": "Stretch — tangential fit, doesn't fully leverage your strengths",
    }

    for batch_start in range(0, len(all_matches), BATCH_SIZE):
        batch = all_matches[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(all_matches) + BATCH_SIZE - 1) // BATCH_SIZE

        # Build numbered job list (1-indexed within batch)
        job_lines = []
        for i, (fp, title, company, sal_min, sal_max) in enumerate(batch, 1):
            salary = ""
            if sal_min and sal_max:
                salary = f" (${sal_min:,.0f}–${sal_max:,.0f})"
            elif sal_min:
                salary = f" (${sal_min:,.0f}+)"
            job_lines.append(f"{i}. {title} @ {company or 'Unknown'}{salary}")

        jobs_text = "\n".join(job_lines)

        prompt = f"""You are ranking job opportunities for a specific candidate. You MUST sort every job into a tier.

CANDIDATE SUMMARY:
{RESUME_SUMMARY}

JOBS TO RANK:
{jobs_text}

TIER DEFINITIONS:
S-TIER (apply immediately): Company is a top brand (FAANG, well-funded unicorn, major PE/VC, Zillow, Workday, etc.) AND role directly leverages 2+ of his domains (M&A + product, RE + tech, finance + AI, PE + operations). His resume IS the job description.
A-TIER (strong fit): Reputable company + clear overlap with his background. He'd be competitive and the role is a natural next step.
B-TIER (decent): Some overlap but generic role or small/unknown company. Only one skill area matches. Would require selling the fit.
C-TIER (stretch): Tangential. Sales-heavy, wrong industry, doesn't leverage the hybrid background, or tiny unknown company.

RULES:
- S-TIER should be 5-15% of jobs (the truly exceptional matches)
- A-TIER should be 25-35% of jobs
- B-TIER should be 30-40% of jobs
- C-TIER should be the remainder
- You MUST assign EVERY job number to exactly one tier

Respond in EXACTLY this JSON format (use job numbers from the list above):
{{"S": [1, 5], "A": [2, 3, 7, 8], "B": [4, 6, 9, 10], "C": [11, 12]}}"""

        try:
            raw = _call_gemini(prompt)
            raw = raw.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            tiers = json.loads(raw)
            for tier, indices in tiers.items():
                for idx in indices:
                    global_idx = batch_start + int(idx)  # convert to global index
                    tier_map[global_idx] = tier.upper()
            log.info(f"  Batch {batch_num}/{total_batches}: ranked {len(batch)} jobs")
        except Exception as e:
            log.warning(f"  Batch {batch_num}/{total_batches} failed: {e}")
            for i in range(1, len(batch) + 1):
                tier_map[batch_start + i] = "B"

        # Rate limit between batches
        if batch_start + BATCH_SIZE < len(all_matches):
            time.sleep(4)

    # ── Pass 2: Refine S-tier ──
    # Batching inflates S-tier (each batch gets its own 10%). Take all S+A from pass 1,
    # send as one list, and ask for the TRUE top ~10% to be S-tier with specific reasoning.
    sa_indices = [i for i, t in tier_map.items() if t in ("S", "A")]
    if len(sa_indices) > 15:
        log.info(f"  Refining: {len(sa_indices)} S+A tier jobs → picking true S-tier...")
        sa_lines = []
        for rank, idx in enumerate(sa_indices, 1):
            fp, title, company, sal_min, sal_max = all_matches[idx - 1]
            salary = ""
            if sal_min and sal_max:
                salary = f" (${sal_min:,.0f}–${sal_max:,.0f})"
            sa_lines.append(f"{rank}. {title} @ {company or 'Unknown'}{salary}")

        refine_prompt = f"""From these pre-screened job matches, pick the TRUE top 10-15 that are the absolute best fit for this candidate. The rest stay as A-tier (strong but not exceptional).

CANDIDATE:
{RESUME_SUMMARY}

JOBS (all pre-screened as strong matches):
{chr(10).join(sa_lines)}

For each S-tier pick, give a SPECIFIC one-sentence reason (not generic — mention what about THIS role + company makes it exceptional for THIS candidate).

BOOST FACTORS (prioritize these):
- PropTech, fintech, AI/ML, or spreadsheet/data tool companies
- Growth-stage (Series B-D) or well-known enterprise companies
- Roles where finance+product or RE+tech hybrid is the core requirement
- Companies known for good work-life balance / schedule flexibility

PENALIZE:
- Tiny unknown startups (unless role is an exact background match)
- Roles that are pure generic PM with no domain advantage
- Companies known for intense/burnout culture

Respond in EXACTLY this JSON format:
{{"S": [{{"num": 1, "reason": "Specific reason for this job"}}, {{"num": 5, "reason": "Specific reason"}}]}}

RULES:
- Pick 10-15 jobs MAX for S-tier. Be selective.
- The reason must be SPECIFIC to the job+candidate combo, not generic.
- Everything not listed stays A-tier."""

        try:
            raw = _call_gemini(refine_prompt)
            raw = raw.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            result = json.loads(raw)

            # Demote all S to A first
            for idx in sa_indices:
                if tier_map.get(idx) == "S":
                    tier_map[idx] = "A"

            # Promote only the refined S-tier picks with specific reasons
            s_reasons = {}
            for entry in result.get("S", []):
                rank = int(entry["num"])
                if rank <= len(sa_indices):
                    global_idx = sa_indices[rank - 1]
                    tier_map[global_idx] = "S"
                    s_reasons[global_idx] = entry.get("reason", "")

            # Store specific reasons for S-tier
            tier_reasons_specific = s_reasons
            log.info(f"  Refined to {sum(1 for t in tier_map.values() if t == 'S')} S-tier jobs")
            time.sleep(4)
        except Exception as e:
            log.warning(f"  Refinement failed: {e}")
            tier_reasons_specific = {}
    else:
        tier_reasons_specific = {}

    updated = 0
    for i, (fp, title, company, sal_min, sal_max) in enumerate(all_matches, 1):
        tier = tier_map.get(i, "B")
        ai_score = tier_scores.get(tier, 5)
        # Use specific reason for S-tier if available, otherwise generic
        reason = tier_reasons_specific.get(i) if i in tier_reasons_specific else tier_reasons.get(tier, "")
        db.execute(
            "UPDATE jobs SET ai_score = ?, ai_reasoning = ? WHERE fingerprint = ?",
            (ai_score, reason, fp),
        )
        updated += 1

    db.commit()

    # Log distribution
    from collections import Counter
    dist = Counter(tier_map.values())
    dist_str = ", ".join(f"{t}: {dist.get(t, 0)}" for t in ["S", "A", "B", "C"])
    log.info(f"Ranked {updated} jobs. Distribution: {dist_str}")


# ── Phase 4: Notify ──────────────────────────────────────────────────────────

def notify(config: dict, db: sqlite3.Connection):
    rows = db.execute(
        """SELECT fingerprint, title, company, location, url, salary_min, salary_max,
                  source, ai_score, ai_reasoning, recommended_resume, date_posted
           FROM jobs WHERE is_match = 1 AND notified_at IS NULL
           ORDER BY ai_score DESC NULLS LAST"""
    ).fetchall()

    if not rows:
        log.info("No new matches to notify.")
        return

    log.info(f"{len(rows)} new matches to send.")

    email_cfg = config.get("email", {})
    method = email_cfg.get("method", "smtp")

    god_tier = sum(1 for r in rows if r[8] and r[8] >= 7)
    subject = f"CEREBRO: {len(rows)} jobs ({god_tier} god-tier)" if god_tier else f"CEREBRO: {len(rows)} new matches"

    html = _build_email_html(rows)

    try:
        if method == "sendgrid":
            _send_sendgrid(email_cfg, subject, html)
        else:
            _send_smtp(email_cfg, subject, html)
    except Exception as e:
        log.error(f"Email send failed: {e}")
        return

    fps = [r[0] for r in rows]
    now = datetime.now(timezone.utc).isoformat()
    db.executemany(
        "UPDATE jobs SET notified_at = ? WHERE fingerprint = ?",
        [(now, fp) for fp in fps],
    )
    db.commit()
    log.info(f"Notified: {len(rows)} jobs emailed.")


def _score_badge(score):
    if not score:
        return '<span style="background:#666;color:#fff;padding:2px 8px;border-radius:10px;font-size:12px;">—</span>'
    if score >= 8:
        color = "#16a34a"  # green
    elif score >= 6:
        color = "#ca8a04"  # amber
    else:
        color = "#dc2626"  # red
    return f'<span style="background:{color};color:#fff;padding:2px 8px;border-radius:10px;font-size:12px;font-weight:bold;">{score:.0f}</span>'


def _build_email_html(rows) -> str:
    god_tier_rows = ""
    other_rows = ""

    for _, title, company, location, url, sal_min, sal_max, source, ai_score, ai_reason, resume, date_posted in rows:
        salary = ""
        if sal_min and sal_max:
            salary = f"${sal_min:,.0f}–${sal_max:,.0f}"
        elif sal_min:
            salary = f"${sal_min:,.0f}+"

        # Format date as relative ("today", "yesterday", "3d ago") or raw
        date_str = ""
        if date_posted and str(date_posted) not in ("nan", "NaT", ""):
            try:
                posted = datetime.strptime(str(date_posted)[:10], "%Y-%m-%d").date()
                delta = (datetime.now(timezone.utc).date() - posted).days
                if delta == 0:
                    date_str = "today"
                elif delta == 1:
                    date_str = "1d ago"
                elif delta <= 7:
                    date_str = f"{delta}d ago"
                else:
                    date_str = str(date_posted)[:10]
            except (ValueError, TypeError):
                date_str = ""

        badge = _score_badge(ai_score)
        reason_html = f'<br><span style="color:#666;font-size:12px;font-style:italic;">{ai_reason}</span>' if ai_reason else ""
        resume_html = f'<br><span style="color:#2563eb;font-size:11px;">📄 {resume}</span>' if resume else ""

        row_html = f"""
        <tr>
            <td style="text-align:center;">{badge}</td>
            <td><a href="{url}" style="color:#2563eb;text-decoration:none;font-weight:600;">{title}</a>{reason_html}{resume_html}</td>
            <td>{company or '—'}</td>
            <td>{salary or '—'}</td>
            <td style="font-size:12px;color:#888;">{date_str or '—'}</td>
            <td style="font-size:12px;color:#888;">{source}</td>
        </tr>"""

        if ai_score and ai_score >= 7:
            god_tier_rows += row_html
        else:
            other_rows += row_html

    sections = ""

    if god_tier_rows:
        sections += f"""
        <h3 style="color:#16a34a;margin:20px 0 8px;">GOD TIER — Apply Today</h3>
        <table border="0" cellpadding="8" cellspacing="0" style="border-collapse:collapse;font-family:sans-serif;font-size:14px;width:100%;border:1px solid #e5e7eb;">
            <tr style="background:#f0fdf4;">
                <th style="width:40px;">Fit</th><th style="text-align:left;">Role</th><th>Company</th><th>Salary</th><th>Posted</th><th>Src</th>
            </tr>
            {god_tier_rows}
        </table>"""

    if other_rows:
        sections += f"""
        <h3 style="color:#666;margin:20px 0 8px;">Other Matches</h3>
        <table border="0" cellpadding="8" cellspacing="0" style="border-collapse:collapse;font-family:sans-serif;font-size:14px;width:100%;border:1px solid #e5e7eb;">
            <tr style="background:#f9fafb;">
                <th style="width:40px;">Fit</th><th style="text-align:left;">Role</th><th>Company</th><th>Salary</th><th>Posted</th><th>Src</th>
            </tr>
            {other_rows}
        </table>"""

    return f"""
    <html><body style="font-family:sans-serif;max-width:800px;margin:0 auto;padding:20px;">
    <h2 style="margin-bottom:4px;">CEREBRO</h2>
    <p style="color:#666;margin-top:0;">Found {len(rows)} match{'es' if len(rows) != 1 else ''} · {datetime.now(timezone.utc).strftime('%b %d, %Y %H:%M UTC')}</p>
    {sections}
    <p style="color:#aaa;font-size:11px;margin-top:24px;">Score guide: 9-10 = perfect fit, 7-8 = strong, 5-6 = decent, &lt;5 = stretch · God-tier = 7+<br>
    Resume recommendations based on job description keyword analysis.</p>
    </body></html>"""


def _send_smtp(cfg: dict, subject: str, html: str):
    password = os.environ.get("CEREBRO_SMTP_PASSWORD")
    if not password:
        log.warning("CEREBRO_SMTP_PASSWORD not set, skipping email.")
        raise RuntimeError("No SMTP password configured")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"CEREBRO <{cfg['from']}>"
    msg["To"] = cfg["to"]
    msg["X-Priority"] = "1"
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(cfg.get("smtp_host", "smtp.gmail.com"), cfg.get("smtp_port", 587)) as server:
        server.starttls()
        server.login(cfg["from"], password)
        server.sendmail(cfg["from"], cfg["to"], msg.as_string())


def _send_sendgrid(cfg: dict, subject: str, html: str):
    api_key = os.environ.get("CEREBRO_SENDGRID_KEY")
    if not api_key:
        log.warning("CEREBRO_SENDGRID_KEY not set, skipping email.")
        raise RuntimeError("No SendGrid key configured")

    payload = json.dumps({
        "personalizations": [{"to": [{"email": cfg["to"]}]}],
        "from": {"email": cfg["from"]},
        "subject": subject,
        "content": [{"type": "text/html", "value": html}],
    }).encode()

    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=payload,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
    )
    urllib.request.urlopen(req)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("CEREBRO starting")
    config = load_config()
    db = sqlite3.connect(DB_PATH)
    init_db(db)

    ingest(config, db)
    evaluate(config, db)
    score(db)
    notify(config, db)

    # Summary
    stats = db.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN is_match = 1 THEN 1 ELSE 0 END) as matches,
            SUM(CASE WHEN is_match = 0 THEN 1 ELSE 0 END) as rejected,
            SUM(CASE WHEN notified_at IS NOT NULL THEN 1 ELSE 0 END) as notified,
            SUM(CASE WHEN ai_score >= 7 THEN 1 ELSE 0 END) as god_tier
        FROM jobs
    """).fetchone()
    log.info(f"DB totals: {stats[0]} jobs, {stats[1]} matches ({stats[4]} god-tier), {stats[2]} rejected, {stats[3]} notified")

    db.close()
    log.info("CEREBRO complete")


if __name__ == "__main__":
    main()
