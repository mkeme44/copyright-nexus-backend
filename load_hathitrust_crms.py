#!/usr/bin/env python3
"""
load_hathitrust_crms.py — Load HathiTrust CRMS-verified renewal records into Supabase

Source: https://www.hathitrust.org/files/CRMSRenewals.tsv
These are human-reviewed copyright determinations from the CRMS (Copyright Review
Management System) program. Volunteer reviewers at HathiTrust member libraries have
manually investigated hundreds of thousands of volumes; this file captures the renewal
registration numbers they confirmed.

Each row maps a HathiTrust volume ID (htid) to a copyright renewal registration number.
In query_compass.py, this data cross-validates Stanford/NYPL renewal hits — if we find
a renewal record AND it appears in CRMS, confidence is elevated to "CRMS-verified."

TSV columns (from https://www.hathitrust.org/member-libraries/resources-for-librarians/
data-resources/renewal-id-data-file/):
  htid        — HathiTrust volume identifier (e.g., mdp.39015005731453)
  renewal_id  — Copyright renewal registration number (e.g., R345678)

Prerequisites:
  1. Run supabase_crms_setup.sql in your Supabase SQL Editor
  2. Same .env as the rest of Copyright Compass
  3. pip install requests python-dotenv supabase==1.2.0

Usage:
    py -3.12 load_hathitrust_crms.py
    py -3.12 load_hathitrust_crms.py peek      # inspect first 20 rows, don't load
    py -3.12 load_hathitrust_crms.py verify    # count rows already loaded
"""

import os
import io
import sys
import csv
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

CRMS_URL   = "https://www.hathitrust.org/files/CRMSRenewals.tsv"
BATCH_SIZE = 1_000

# ── Download ───────────────────────────────────────────────────────────────────

def download_tsv() -> str | None:
    """Download the CRMS TSV from HathiTrust. Returns raw text content."""
    print(f"  Downloading: {CRMS_URL}")
    print("  (This file is ~10-30 MB — should take under 30 seconds)")
    try:
        r = requests.get(CRMS_URL, timeout=120)
        r.raise_for_status()
        print(f"  ✓ Downloaded {len(r.content) / 1_048_576:.1f} MB")
        return r.text
    except requests.Timeout:
        print("  ✗ Download timed out. Try again or download manually from:")
        print(f"    {CRMS_URL}")
        return None
    except requests.HTTPError as e:
        print(f"  ✗ HTTP error: {e}")
        return None
    except Exception as e:
        print(f"  ✗ Download failed: {e}")
        return None


# ── Parser ─────────────────────────────────────────────────────────────────────

def parse_tsv(text: str) -> list[dict]:
    """
    Parse the CRMS TSV file.

    The file may or may not have a header row. We inspect the first line:
    - If it contains 'htid' → has header, skip it
    - Otherwise → no header, treat first column as htid, second as renewal_id

    Each row becomes: {'htid': '...', 'renewal_id': '...'}
    Rows with missing/empty values are skipped.
    """
    reader = csv.reader(io.StringIO(text), delimiter='\t')
    rows = list(reader)

    if not rows:
        return []

    # Detect header
    first = [c.strip().lower() for c in rows[0]]
    has_header = 'htid' in first or 'renewal' in ' '.join(first)
    data_rows  = rows[1:] if has_header else rows

    if has_header:
        # Find column indices dynamically
        htid_col    = next((i for i, c in enumerate(first) if 'htid' in c), 0)
        renewal_col = next((i for i, c in enumerate(first) if 'renewal' in c), 1)
    else:
        htid_col    = 0
        renewal_col = 1

    records = []
    for row in data_rows:
        if len(row) <= max(htid_col, renewal_col):
            continue
        htid       = row[htid_col].strip()
        renewal_id = row[renewal_col].strip()
        if not htid or not renewal_id:
            continue
        records.append({'htid': htid[:200], 'renewal_id': renewal_id[:50]})

    return records


# ── Supabase loader ────────────────────────────────────────────────────────────

def load_batch(rows: list[dict]) -> tuple[int, int]:
    """Insert a batch into hathitrust_crms_renewals. Returns (ok, err)."""
    try:
        supabase.table('hathitrust_crms_renewals').insert(rows).execute()
        return len(rows), 0
    except Exception as e:
        print(f"\n    Batch error: {e}")
        # Fall back to row-by-row
        ok = err = 0
        for row in rows:
            try:
                supabase.table('hathitrust_crms_renewals').insert(row).execute()
                ok += 1
            except Exception:
                err += 1
        return ok, err


# ── Modes ──────────────────────────────────────────────────────────────────────

def peek():
    """Download and print first 20 rows without loading anything."""
    print("PEEK MODE — inspecting first 20 rows")
    print()
    text = download_tsv()
    if not text:
        return
    records = parse_tsv(text)
    print(f"Total parsed: {len(records):,} records")
    print()
    print(f"{'HTID':<50}  RENEWAL_ID")
    print("-" * 65)
    for r in records[:20]:
        print(f"{r['htid']:<50}  {r['renewal_id']}")
    print()
    print("If columns look wrong, check the TSV format and adjust parse_tsv().")


def verify():
    """Count rows currently in the Supabase table."""
    print("VERIFY MODE — checking Supabase row count")
    try:
        # Query the table directly — avoids supabase==1.2.0 no-params RPC issue
        result = supabase.table('hathitrust_crms_renewals').select('id', count='exact').execute()
        count = result.count if result.count is not None else len(result.data)
        print(f"\n  hathitrust_crms_renewals: {count:,} rows")
        if count == 0:
            print("  → Table is empty. Run without arguments to load.")
        else:
            print("  → Data is loaded and ready.")
            # Show a sample renewal ID so we can confirm R/RE formats loaded
            sample = result.data[:3] if result.data else []
            if sample:
                print(f"\n  Sample renewal IDs: {[r['renewal_id'] for r in sample]}")
    except Exception as e:
        print(f"  Error: {e}")
        print("  Make sure supabase_crms_setup.sql has been run.")


def main():
    print("=" * 65)
    print("  Copyright Compass — HathiTrust CRMS Renewals Loader")
    print("  Source: hathitrust.org/files/CRMSRenewals.tsv")
    print("  These are human-reviewed CRMS copyright determinations")
    print("=" * 65)
    print()

    text = download_tsv()
    if not text:
        return

    print("\n  Parsing TSV...")
    records = parse_tsv(text)
    print(f"  Parsed {len(records):,} records")

    if not records:
        print("\n  ✗ No records found. Check TSV format with peek mode:")
        print("    py -3.12 load_hathitrust_crms.py peek")
        return

    # Show sample
    print(f"\n  Sample (first 3 rows):")
    for r in records[:3]:
        print(f"    htid={r['htid']}  renewal_id={r['renewal_id']}")

    print(f"\n  Loading {len(records):,} records into Supabase...")
    print("  (Ctrl+C to cancel at any time)")
    print()

    batch        = []
    total_loaded = 0
    total_errors = 0
    start        = time.time()

    for i, record in enumerate(records, 1):
        batch.append(record)

        if len(batch) >= BATCH_SIZE:
            ok, err    = load_batch(batch)
            total_loaded += ok
            total_errors += err
            batch = []

            elapsed = time.time() - start
            pct     = i / len(records) * 100
            print(f"  Progress: {i:,}/{len(records):,} ({pct:.0f}%) | "
                  f"Loaded: {total_loaded:,} | "
                  f"Elapsed: {elapsed:.0f}s")
            time.sleep(0.1)

    if batch:
        ok, err    = load_batch(batch)
        total_loaded += ok
        total_errors += err

    elapsed = time.time() - start

    print()
    print("=" * 65)
    print("  SUMMARY")
    print("=" * 65)
    print(f"  Records loaded:  {total_loaded:,}")
    print(f"  Errors:          {total_errors:,}")
    print(f"  Time:            {elapsed:.0f}s")

    if total_loaded > 0:
        print("\n  ✓ Done!")
        print()
        print("  Verify in Supabase SQL Editor:")
        print("  SELECT COUNT(*) FROM hathitrust_crms_renewals;")
        print()
        print("  Test a known renewal (Stanford/NYPL format, e.g. R123456):")
        print("  SELECT crms_verify_renewal('R123456');")
        print()
        print("  Next step: query_compass.py will now show 'CRMS-verified'")
        print("  badges when a renewal hit is confirmed in this table.")
    else:
        print("\n  ✗ Nothing loaded. Run peek mode to debug:")
        print("    py -3.12 load_hathitrust_crms.py peek")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == 'peek':
            peek()
        elif sys.argv[1] == 'verify':
            verify()
        else:
            print(f"Unknown argument: {sys.argv[1]}")
            print("Usage: py -3.12 load_hathitrust_crms.py [peek|verify]")
    else:
        main()
