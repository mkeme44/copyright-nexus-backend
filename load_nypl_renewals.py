#!/usr/bin/env python3
"""
load_nypl_renewals.py — Load NYPL CCE Renewal records into Supabase

Source: github.com/NYPL/cce-renewals
Coverage: All copyright renewal classes, 1950-1991
  - 1950-1977: transcribed from Catalog of Copyright Entries (Project Gutenberg)
    Files: {year}-1A.tsv, {year}-1.tsv, etc.
  - 1978-1991: from Copyright Office database (Google export)
    Files: {year}-from-db.tsv  ← different naming AND different column names

TSV columns — pre-1978 files:
  entry_id | volume | part | number | page | author | title |
  oreg | odat | id | rdat | claimants | new_matter | see_also_ren |
  see_also_reg | notes | full_text

TSV columns — 1978-1991 from-db files (4 columns differ):
  entry_id | volume | part | number | page | auth  | titl  |
  oreg | odat | id | dreg | claimants | new_matter | see_also_ren |
  see_also_reg | note  | full_text
                  ^^^^   ^^^^              ^^^^        ^^^^
  (author→auth, title→titl, rdat→dreg, notes→note)
  Also: Windows CRLF line endings — handled automatically by csv.DictReader.

Key fields:
  author/auth → author name
  title/titl  → title of work
  oreg        → original registration number (e.g. A704623)
  odat        → original registration date → pub_year
  id          → renewal number (e.g. R77831)
  rdat/dreg   → renewal date → renewal_year
  claimants   → pipe-delimited rights holders

FIX (2025): The original loader only tried part-suffix filenames
  (e.g. 1978-1A.tsv, 1978-1.tsv) which all return 404 for 1978-1991.
  The actual files are named 1978-from-db.tsv through 1991-from-db.tsv.
  This version adds a from-db fetch path and normalizes the column names.

Usage:
    py -3.12 load_nypl_renewals.py            # full run
    py -3.12 load_nypl_renewals.py 1990       # resume from a specific year

Prerequisites:
    - supabase_nypl_setup.sql run in Supabase SQL Editor
    - Same .env file as rest of Copyright Compass
    - pip install requests python-dotenv supabase==1.2.0
"""

import os
import re
import csv
import io
import sys
import time
import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)

# ── Configuration ──────────────────────────────────────────────────────────────

GITHUB_BASE = "https://raw.githubusercontent.com/NYPL/cce-renewals/master/data"

# 1950-1977: CCE transcriptions — one file per year, part suffix varies
PRE_1978_YEARS  = list(range(1950, 1978))
PRE_1978_PARTS  = ['1A', '1B', '2', '1', '3', '4']   # try all; skip 404s

# 1978-1991: Copyright Office database export — one file per year, fixed name
POST_1977_YEARS = list(range(1978, 1992))              # 1978-1991 inclusive

BATCH_SIZE = 100   # smaller = fewer timeouts; was 500


# ── Helpers ────────────────────────────────────────────────────────────────────

def extract_year(date_str: str) -> int | None:
    """Extract 4-digit year from a date string like '1952-03-15' or '15Mar52'."""
    if not date_str:
        return None
    # ISO format: 1952-03-15
    m = re.search(r'\b(19[0-9]{2}|20[0-2][0-9])\b', str(date_str))
    if m:
        return int(m.group(1))
    # Short format: 15Mar52 → expand 2-digit year
    m2 = re.search(r'(\d{2})([A-Za-z]{3})(\d{2})', str(date_str))
    if m2:
        yr = int(m2.group(3))
        return 1900 + yr if yr >= 20 else 2000 + yr
    return None


# ── Download ───────────────────────────────────────────────────────────────────

def _get(url: str) -> str | None:
    """Fetch a URL; return text or None on 404/error."""
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.text
    except requests.RequestException:
        return None


def fetch_pre1978(year: int, part: str) -> str | None:
    """Fetch a 1950-1977 part file, e.g. 1977-1.tsv"""
    return _get(f"{GITHUB_BASE}/{year}-{part}.tsv")


def fetch_from_db(year: int) -> str | None:
    """
    Fetch a 1978-1991 from-db file, e.g. 1978-from-db.tsv
    These files have different column names (auth/titl/dreg/note)
    and Windows CRLF line endings.
    """
    return _get(f"{GITHUB_BASE}/{year}-from-db.tsv")


# ── Parser ─────────────────────────────────────────────────────────────────────

def parse_tsv(text: str, year: int, source_label: str) -> list[dict]:
    """
    Parse a NYPL renewals TSV file into Supabase-ready dicts.

    Handles both pre-1978 column names (author/title/rdat/notes)
    and from-db column names (auth/titl/dreg/note) via fallback lookups.
    CRLF line endings are handled transparently by csv.DictReader.
    """
    records = []
    reader  = csv.DictReader(io.StringIO(text), delimiter='\t')

    for row in reader:
        # Normalize column names: try canonical name first, then from-db alias
        title  = (row.get('title')  or row.get('titl')  or '').strip()
        if not title:
            continue

        author     = (row.get('author')    or row.get('auth')  or '').strip()
        oreg       = (row.get('oreg')      or '').strip()
        odat       = (row.get('odat')      or '').strip()
        renewal_id = (row.get('id')        or '').strip()
        rdat       = (row.get('rdat')      or row.get('dreg') or '').strip()
        claimants  = (row.get('claimants') or '').strip()
        new_matter = (row.get('new_matter') or '').strip()
        notes      = (row.get('notes')     or row.get('note') or '').strip()
        entry_id   = (row.get('entry_id')  or '').strip()

        pub_year     = extract_year(odat)
        renewal_year = extract_year(rdat) or year   # fallback to file year

        records.append({
            'entry_id':     entry_id[:100],
            'author':       author[:500],
            'title':        title[:500],
            'oreg':         oreg[:50],
            'odat':         odat[:50],
            'renewal_id':   renewal_id[:50],
            'rdat':         rdat[:50],
            'claimants':    claimants[:500],
            'new_matter':   new_matter[:200],
            'notes':        notes[:500],
            'pub_year':     pub_year,
            'renewal_year': renewal_year,
        })

    return records


# ── Supabase loader ────────────────────────────────────────────────────────────

def load_batch(rows: list[dict]) -> tuple[int, int]:
    """Batch insert into Supabase nypl_renewals table, with retry on timeout."""
    for attempt in range(3):
        try:
            supabase.table('nypl_renewals').insert(rows).execute()
            return len(rows), 0
        except Exception as e:
            err_str = str(e)
            if 'timed out' in err_str.lower() and attempt < 2:
                wait = 5 * (attempt + 1)
                print(f"\n    ⏱  Timeout on attempt {attempt+1}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"\n    Batch error: {e}")
                # Fall through to row-by-row
                break

    ok = err = 0
    for row in rows:
        for attempt in range(2):
            try:
                supabase.table('nypl_renewals').insert(row).execute()
                ok += 1
                break
            except Exception:
                if attempt == 0:
                    time.sleep(2)
                else:
                    err += 1
    return ok, err


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    # Optional: resume from a specific year to skip already-loaded data
    # Usage: py -3.12 load_nypl_renewals.py 1990
    resume_from = None
    if len(sys.argv) > 1:
        try:
            resume_from = int(sys.argv[1])
            print(f"  ▶  Resuming from year {resume_from} (skipping earlier years)")
        except ValueError:
            print(f"  ⚠  Invalid year argument '{sys.argv[1]}', running full load")

    print("=" * 65)
    print("  Copyright Compass — NYPL CCE Renewals Loader")
    print("  Source: github.com/NYPL/cce-renewals")
    print("  Coverage: All classes, renewal years 1950-1991")
    print("=" * 65)
    print()
    print("  1950-1977 : CCE transcriptions  ({year}-1A.tsv / {year}-1.tsv)")
    print("  1978-1991 : CO database export  ({year}-from-db.tsv)  ← new")
    print()
    print(f"  Batch size: {BATCH_SIZE} rows  (smaller = fewer timeouts)")
    print()
    print("  Downloading from GitHub (~406k records expected)...")
    print("  Expected time: 5-10 minutes.")
    print()

    total_loaded = 0
    total_errors = 0
    files_found  = 0
    batch        = []
    start        = time.time()

    # ── Phase 1: 1950-1977 (part-suffix files) ────────────────────────────────
    for year in PRE_1978_YEARS:
        if resume_from and year < resume_from:
            continue
        year_records = 0

        for part in PRE_1978_PARTS:
            text = fetch_pre1978(year, part)
            if text is None:
                continue

            files_found += 1
            records = parse_tsv(text, year, f"{year}-{part}.tsv")
            year_records += len(records)

            for rec in records:
                batch.append(rec)
                if len(batch) >= BATCH_SIZE:
                    ok, err = load_batch(batch)
                    total_loaded += ok
                    total_errors += err
                    batch = []

        if year_records > 0:
            print(f"  {year}: {year_records:,} records  "
                  f"(total so far: {total_loaded:,})")
        else:
            print(f"  {year}: no files found")

        time.sleep(0.3)

    # ── Phase 2: 1978-1991 (from-db files) ───────────────────────────────────
    print()
    print("  --- switching to from-db files (1978-1991) ---")
    print()

    for year in POST_1977_YEARS:
        if resume_from and year < resume_from:
            continue
        text = fetch_from_db(year)

        if text is None:
            print(f"  {year}: ⚠  from-db file not found (unexpected)")
            continue

        files_found += 1
        records = parse_tsv(text, year, f"{year}-from-db.tsv")

        for rec in records:
            batch.append(rec)
            if len(batch) >= BATCH_SIZE:
                ok, err = load_batch(batch)
                total_loaded += ok
                total_errors += err
                batch = []

        print(f"  {year}: {len(records):,} records  "
              f"(total so far: {total_loaded:,})")

        time.sleep(0.3)

    # ── Final batch ───────────────────────────────────────────────────────────
    if batch:
        ok, err = load_batch(batch)
        total_loaded += ok
        total_errors += err

    elapsed = time.time() - start

    print()
    print("=" * 65)
    print("  SUMMARY")
    print("=" * 65)
    print(f"  Files downloaded:  {files_found}")
    print(f"  Records loaded:    {total_loaded:,}")
    print(f"  Errors:            {total_errors:,}")
    print(f"  Time:              {elapsed/60:.1f} minutes")

    if total_loaded == 0:
        print()
        print("  ⚠  Zero records loaded.")
        print("  Check your internet connection and Supabase keys.")
    else:
        print("\n  ✓ Done!")
        print()
        print("  Verify in Supabase SQL Editor:")
        print("  SELECT COUNT(*) FROM nypl_renewals;")
        print()
        print("  Check 1978-1991 coverage (should be ~247k):")
        print("  SELECT COUNT(*) FROM nypl_renewals")
        print("    WHERE renewal_year BETWEEN 1978 AND 1991;")
        print()
        print("  Test Hemingway (renewed 1980, was missing before this fix):")
        print("  SELECT * FROM nypl_renewals")
        print("    WHERE lower(title) LIKE '%old man%' LIMIT 5;")


if __name__ == "__main__":
    main()
