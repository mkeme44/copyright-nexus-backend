#!/usr/bin/env python3
"""
test_search.py — Verify all three search functions work after running
supabase_search_fix_v3.sql

Usage:
    py -3.12 test_search.py
"""

import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

def test(fn_name, params, label):
    print(f"\n  {label}")
    try:
        result = supabase.rpc(fn_name, params).execute()
        rows = result.data or []
        if rows:
            r = rows[0]
            print(f"  ✅ {len(rows)} result(s). Best: [{r.get('similarity_score', 0):.2f}] {r.get('title', '?')[:60]}")
        else:
            print(f"  ⚠  Function works but 0 results (data may not contain this title)")
    except Exception as e:
        print(f"  ✗  Error: {e}")

print("=" * 65)
print("  Search Function Tests")
print("=" * 65)

test('search_renewals',
     {'search_title': 'The Old Man and the Sea', 'search_author': 'Hemingway',
      'search_pub_year': 1952, 'result_limit': 3},
     "Stanford (search_renewals) — Hemingway:")

test('search_nypl_renewals',
     {'search_title': 'The Old Man and the Sea', 'search_author': 'Hemingway',
      'search_pub_year': 1952, 'result_limit': 3},
     "NYPL (search_nypl_renewals) — Hemingway:")

test('search_usco_renewals',
     {'search_title': 'The Old Man and the Sea', 'search_author': 'Hemingway',
      'search_pub_year': 1952, 'result_limit': 3},
     "USCO (search_usco_renewals) — Hemingway:")

test('search_usco_renewals',
     {'search_title': 'Sabrina the teenage witch', 'search_author': None,
      'search_pub_year': 1977, 'result_limit': 3},
     "USCO (search_usco_renewals) — Sabrina (known to be in first 10k rows):")

print()
