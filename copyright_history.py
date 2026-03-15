#!/usr/bin/env python3
"""
copyright_history.py — Copyright History Lookup for Copyright Nexus

Retrieves the complete copyright lifecycle for a work:
  • Publication details (year, registration number)
  • Renewal record (if any) from Stanford + NYPL + USCO
  • Current copyright status under US law
  • Expiration date (or date already expired)
  • Recommended RightsStatements.org URI

Three renewal database sources queried in parallel:
  1. Stanford (copyright_renewals)  — 246k book renewals, pubs 1923-1963
  2. NYPL CCE   (nypl_renewals)     — all classes, 1950-1991, incl. CO database
  3. USCO       (usco_renewals)     — ~908k RE-prefixed records

Prerequisite SQL:
  Run supabase_usco_search_function.sql in Supabase SQL Editor before
  USCO lookups will work. Stanford and NYPL search functions already exist.

Usage:
    py -3.12 copyright_history.py
    py -3.12 copyright_history.py "The Old Man and the Sea" Hemingway
    py -3.12 copyright_history.py "Gone with the Wind"
    py -3.12 copyright_history.py "A Farewell to Arms" "Hemingway" --json
"""

import os
import sys
import re
import json
import time
from datetime import date
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)

CURRENT_YEAR       = date.today().year
SIMILARITY_CUTOFF  = 0.65   # Minimum match confidence to accept a hit
RETRY_ATTEMPTS     = 3      # Retry count for transient Supabase read timeouts
RETRY_DELAY        = 1.5    # Seconds between retries


# ─────────────────────────────────────────────────────────────────────────────
#  Copyright duration rules
# ─────────────────────────────────────────────────────────────────────────────

def determine_status(pub_year: int | None,
                     renewed: bool | None,
                     renewal_year: int | None,
                     has_notice: bool | None = None) -> dict:
    """
    Apply US copyright duration rules.

    Parameters
    ----------
    pub_year    : Year of first publication (None = unknown)
    renewed     : True = renewal record found; False = searched, not found;
                  None = not applicable or not searched
    renewal_year: Year renewal was filed (if known)
    has_notice  : Whether a copyright notice was present (used for 1930-1989 works)

    Returns
    -------
    dict with keys:
        status, rights_statement, uri, expires, confidence, notes
    """
    today = CURRENT_YEAR

    if pub_year is None:
        return {
            'status':           'Undetermined',
            'rights_statement': 'Copyright Undetermined',
            'uri':              'https://rightsstatements.org/vocab/UND/1.0/',
            'expires':          None,
            'confidence':       'Low',
            'notes':            'Publication year unknown — cannot determine copyright status.',
        }

    # ── Pre-1930: Always Public Domain ────────────────────────────────────────
    if pub_year < 1930:
        exp = pub_year + 95
        return {
            'status':           'Public Domain',
            'rights_statement': 'No Copyright - United States',
            'uri':              'https://rightsstatements.org/vocab/NoC-US/1.0/',
            'expires':          f'Expired January 1, {exp + 1}',
            'confidence':       'High',
            'notes':            (
                f'Published before 1930. Maximum 95-year term '
                f'({pub_year} + 95 = {exp}) has expired.'
            ),
        }

    # ── 1930-1963: Notice required + renewal required ─────────────────────────
    if 1930 <= pub_year <= 1963:
        # Notice absent → immediate public domain
        if has_notice is False:
            return {
                'status':           'Public Domain',
                'rights_statement': 'No Copyright - United States',
                'uri':              'https://rightsstatements.org/vocab/NoC-US/1.0/',
                'expires':          'Expired (no copyright notice)',
                'confidence':       'High',
                'notes':            'No copyright notice found. Notice was mandatory. Copyright never arose.',
            }

        if renewed is True:
            exp = pub_year + 95
            in_copyright = today <= exp
            return {
                'status':           'In Copyright' if in_copyright else 'Public Domain',
                'rights_statement': ('In Copyright' if in_copyright
                                     else 'No Copyright - United States'),
                'uri':              ('https://rightsstatements.org/vocab/InC/1.0/' if in_copyright
                                     else 'https://rightsstatements.org/vocab/NoC-US/1.0/'),
                'expires':          f'{"Expires" if in_copyright else "Expired"} January 1, {exp + 1}',
                'confidence':       'High',
                'notes':            (
                    f'Renewal confirmed (filed: {renewal_year or "unknown"}). '
                    f'95-year term = {pub_year} + 95 = {exp}.'
                ),
            }

        if renewed is False:
            initial_exp = pub_year + 28
            return {
                'status':           'Public Domain',
                'rights_statement': 'No Copyright - United States',
                'uri':              'https://rightsstatements.org/vocab/NoC-US/1.0/',
                'expires':          f'Expired January 1, {initial_exp + 1}',
                'confidence':       'High',
                'notes':            (
                    f'No renewal record found in Stanford, NYPL, or USCO databases. '
                    f'Copyright lapsed after 28-year initial term (expired {initial_exp + 1}). '
                    f'~93% of works in this period were not renewed.'
                ),
            }

        # Renewal status not yet searched
        return {
            'status':           'Undetermined',
            'rights_statement': 'Copyright Undetermined',
            'uri':              'https://rightsstatements.org/vocab/UND/1.0/',
            'expires':          None,
            'confidence':       'Low',
            'notes':            (
                'Renewal status unknown. Search Stanford/NYPL/USCO databases. '
                'If no renewal found after thorough search, very likely public domain.'
            ),
        }

    # ── 1964-1977: Notice required, renewal automatic ─────────────────────────
    if 1964 <= pub_year <= 1977:
        if has_notice is False:
            return {
                'status':           'Public Domain',
                'rights_statement': 'No Copyright - United States',
                'uri':              'https://rightsstatements.org/vocab/NoC-US/1.0/',
                'expires':          'Expired (no copyright notice)',
                'confidence':       'High',
                'notes':            'No copyright notice found. Notice was mandatory through 1978.',
            }
        exp = pub_year + 95
        in_copyright = today <= exp
        return {
            'status':           'In Copyright' if in_copyright else 'Public Domain',
            'rights_statement': ('In Copyright' if in_copyright
                                 else 'No Copyright - United States'),
            'uri':              ('https://rightsstatements.org/vocab/InC/1.0/' if in_copyright
                                 else 'https://rightsstatements.org/vocab/NoC-US/1.0/'),
            'expires':          f'{"Expires" if in_copyright else "Expired"} January 1, {exp + 1}',
            'confidence':       'High',
            'notes':            (
                'Renewal was automatic under the Copyright Renewal Act of 1992. '
                f'95-year term from publication = {pub_year} + 95 = {exp}.'
            ),
        }

    # ── 1978 - Feb 28 1989: Notice required, no renewal ──────────────────────
    if 1978 <= pub_year <= 1989:
        # Approximate with 95 years from publication for works for hire;
        # individual authors: life + 70. We flag this distinction.
        exp_wfh = pub_year + 95
        return {
            'status':           'In Copyright',
            'rights_statement': 'In Copyright',
            'uri':              'https://rightsstatements.org/vocab/InC/1.0/',
            'expires':          (
                f'Work for hire: expires January 1, {exp_wfh + 1}. '
                f'Individual author: expires 70 years after death.'
            ),
            'confidence':       'High',
            'notes':            (
                'No renewal required. Notice still required until March 1, 1989. '
                'Consult registration records if notice was absent — '
                'a 5-year cure window may apply.'
            ),
        }

    # ── March 1989+: No formalities required ─────────────────────────────────
    exp_wfh = pub_year + 95
    return {
        'status':           'In Copyright',
        'rights_statement': 'In Copyright',
        'uri':              'https://rightsstatements.org/vocab/InC/1.0/',
        'expires':          (
            f'Work for hire: expires January 1, {exp_wfh + 1}. '
            f'Individual author: expires 70 years after death.'
        ),
        'confidence':       'High',
        'notes':            (
            'Copyright automatic. No notice, registration, or renewal required. '
            'Term: life + 70 years (individual) or 95/120 years from creation (corporate).'
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Database lookup helpers (with retry)
# ─────────────────────────────────────────────────────────────────────────────

def _rpc_with_retry(fn_name: str, params: dict) -> list:
    """Call a Supabase RPC function with exponential-backoff retry."""
    last_err = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            result = supabase.rpc(fn_name, params).execute()
            return result.data or []
        except Exception as e:
            last_err = e
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY * attempt)
    print(f"   ⚠  {fn_name} failed after {RETRY_ATTEMPTS} attempts: {last_err}")
    return []


def _lookup_stanford(title: str, author: str | None, year: int | None) -> list[dict]:
    rows = _rpc_with_retry('search_renewals', {
        'search_title':    title,
        'search_author':   author,
        'search_pub_year': year,
        'result_limit':    5,
    })
    return [r for r in rows if (r.get('similarity_score') or 0) >= SIMILARITY_CUTOFF]


def _lookup_nypl(title: str, author: str | None, year: int | None) -> list[dict]:
    rows = _rpc_with_retry('search_nypl_renewals', {
        'search_title':    title,
        'search_author':   author,
        'search_pub_year': year,
        'result_limit':    5,
    })
    return [r for r in rows if (r.get('similarity_score') or 0) >= SIMILARITY_CUTOFF]


def _lookup_usco(title: str, author: str | None, year: int | None) -> list[dict]:
    """
    Requires search_usco_renewals() to be deployed in Supabase.
    See supabase_usco_search_function.sql — run once in SQL Editor.
    Skips gracefully if function not yet installed.

    USCO column mapping differs from Stanford/NYPL:
      original_pub_year   → publication year of the original work
      reg_year            → year the renewal was filed
      registration_number → the RE-prefixed renewal number
      authors             → author/claimant name
    """
    rows = _rpc_with_retry('search_usco_renewals', {
        'search_title':    title,
        'search_author':   author,
        'search_pub_year': year,
        'result_limit':    5,
    })

    results = []
    for r in rows:
        score = r.get('similarity_score') or 0
        if score < SIMILARITY_CUTOFF:
            continue
        # Normalise USCO columns to match field names used by build_history
        # and print_history so all three sources display consistently.
        pub_yr = r.get('original_pub_year') or r.get('pub_year')
        results.append({
            **r,
            'pub_year':     pub_yr,
            'renewal_year': r.get('reg_year'),
            'renewal_id':   r.get('registration_number'),
            'rdat':         r.get('reg_date'),
            'author':       r.get('authors'),
            'claimant':     r.get('claimants') or r.get('authors'),
        })
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  History assembler
# ─────────────────────────────────────────────────────────────────────────────

def build_history(title: str,
                  author: str | None = None,
                  pub_year_hint: int | None = None) -> dict:
    """
    Query all three renewal databases and assemble a complete copyright history.

    Returns
    -------
    dict:
        query        — {title, author, pub_year_hint}
        pub_year     — best-determined publication year
        renewed      — True / False / None
        renewal_year — year of renewal filing (if known)
        hits         — {stanford: [...], nypl: [...], usco: [...]}
        status       — output of determine_status()
        sources_with_hits — list of source names that returned records
    """
    # Strip inline year hint like "Title [1952]"
    year_bracket = re.search(r'\[(\d{4})\]', title)
    if year_bracket:
        pub_year_hint = pub_year_hint or int(year_bracket.group(1))
        title = title.replace(year_bracket.group(0), '').strip()

    print(f'\n   → Stanford Renewal DB...', end='  ', flush=True)
    stanford_hits = _lookup_stanford(title, author, pub_year_hint)
    print(f'{"✅ " + str(len(stanford_hits)) + " record(s)" if stanford_hits else "not found"}')

    print(f'   → NYPL CCE Renewals...', end='     ', flush=True)
    nypl_hits = _lookup_nypl(title, author, pub_year_hint)
    print(f'{"✅ " + str(len(nypl_hits)) + " record(s)" if nypl_hits else "not found"}')

    print(f'   → USCO Renewals...', end='        ', flush=True)
    usco_hits = _lookup_usco(title, author, pub_year_hint)
    print(f'{"✅ " + str(len(usco_hits)) + " record(s)" if usco_hits else "not found"}')

    all_hits = stanford_hits + nypl_hits + usco_hits

    # ── Determine best pub_year ───────────────────────────────────────────────
    # Always trust the year the user explicitly passed in.
    # Without a hint, infer from the highest-confidence DB record (>=0.80).
    # Track whether the year was provided or inferred so the display can flag it.
    pub_year_inferred = False

    if pub_year_hint:
        pub_year = pub_year_hint
    else:
        # Collect years from all high-confidence records (>=0.80) and take
        # the EARLIEST — the original publication is always the earliest, and
        # that's what determines the copyright term. Taking the best-match year
        # risks picking a later edition or reprint with a newer registration.
        candidate_years = []
        for h in all_hits:
            if (h.get('similarity_score') or 0) >= 0.80:
                for key in ('pub_year', 'original_pub_year'):
                    py = h.get(key)
                    if py and 1800 <= py <= CURRENT_YEAR:
                        candidate_years.append(py)
        if candidate_years:
            pub_year = min(candidate_years)   # earliest = original publication
            pub_year_inferred = True
        else:
            pub_year = None

    # ── Was it renewed? ───────────────────────────────────────────────────────
    # Only meaningful for 1923-1963 publications
    if pub_year and 1923 <= pub_year <= 1963:
        renewed = len(all_hits) > 0
    else:
        renewed = None   # not applicable

    # ── Best renewal year ─────────────────────────────────────────────────────
    ren_years = []
    for h in all_hits:
        for key in ('renewal_year', 'reg_year'):
            ry = h.get(key)
            if ry and 1950 <= ry <= 1995:
                ren_years.append(ry)
    renewal_year = min(ren_years) if ren_years else None

    # ── Copyright status ──────────────────────────────────────────────────────
    status = determine_status(pub_year, renewed, renewal_year)

    sources_with_hits = (
        (['Stanford'] if stanford_hits else []) +
        (['NYPL'] if nypl_hits else []) +
        (['USCO'] if usco_hits else [])
    )

    return {
        'query':             {'title': title, 'author': author, 'pub_year_hint': pub_year_hint},
        'pub_year':          pub_year,
        'pub_year_inferred': pub_year_inferred,
        'renewed':           renewed,
        'renewal_year':      renewal_year,
        'hits':              {'stanford': stanford_hits, 'nypl': nypl_hits, 'usco': usco_hits},
        'status':            status,
        'sources_with_hits': sources_with_hits,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Output formatter
# ─────────────────────────────────────────────────────────────────────────────

ICONS = {
    'Public Domain': '🟢',
    'In Copyright':  '🔴',
    'Undetermined':  '🟡',
}


def print_history(history: dict):
    title             = history['query']['title']
    author            = history['query']['author']
    pub_yr            = history['pub_year']
    pub_year_inferred = history.get('pub_year_inferred', False)
    hits              = history['hits']
    status            = history['status']
    all_hits          = hits['stanford'] + hits['nypl'] + hits['usco']
    icon              = ICONS.get(status['status'], '⚪')

    # ── Header ────────────────────────────────────────────────────────────────
    print()
    print('═' * 65)
    print(f'  COPYRIGHT HISTORY: "{title}"')
    if author:
        print(f'  Author: {author}')
    if pub_yr and pub_year_inferred:
        print(f'  Published: {pub_yr}  ⚠ inferred from renewal records — verify independently')
    elif pub_yr:
        print(f'  Published: {pub_yr}')
    else:
        print( '  Published: Unknown')
        print( '  ⚠  No year provided and none could be inferred.')
        print( '     Add a publication year for a definitive determination.')
    print('═' * 65)

    # ── 1. Current Status ─────────────────────────────────────────────────────
    print()
    print(f'⚖️   STATUS:  {icon} {status["status"]}')
    print(f'    Expiration:  {status["expires"] or "See notes"}')

    # URAA caveat sits right under the status
    if pub_yr and 1928 <= pub_yr <= 1963 and status['status'] == 'Public Domain':
        print()
        print('    ⚠  URAA CAVEAT: If this work was first published OUTSIDE the US,')
        print('       the URAA (1994) may have retroactively restored copyright.')
        print('       Verify country of first publication before concluding PD.')

    # ── 2. Renewal Records ────────────────────────────────────────────────────
    print()
    print('📝  RENEWAL HISTORY')

    if all_hits:
        print()
        seen_keys: set[str] = set()
        row_num = 1

        for src_key, src_label in [('stanford', 'Stanford'), ('nypl', 'NYPL'), ('usco', 'USCO')]:
            for rec in hits[src_key]:
                reg_num = (
                    rec.get('reg_num') or rec.get('oreg') or
                    rec.get('registration_number') or ''
                ).strip().upper()
                dedup_key = f'{src_label}:{reg_num}' if reg_num else f'{src_label}:{row_num}'
                if dedup_key in seen_keys:
                    continue
                seen_keys.add(dedup_key)

                ren_num  = (rec.get('renewal_num') or rec.get('renewal_id') or
                            rec.get('registration_number') or '–')
                ren_date = (rec.get('renewal_date') or rec.get('rdat') or
                            rec.get('reg_date') or '–')
                claimant = (rec.get('claimant') or rec.get('claimants') or
                            rec.get('authors') or '–')
                sim      = rec.get('similarity_score', 0)
                matched  = (rec.get('title') or '–')
                reg_date = rec.get('reg_date') or rec.get('odat') or '–'

                if sim >= 0.90:
                    record_label = f'Record {row_num}  [{src_label}]'
                else:
                    record_label = f'Record {row_num}  [{src_label}]  — Related Work'

                print(f'    ┌─ {record_label}')
                print(f'    │  Title:       {matched}')
                print(f'    │  Claimant:    {claimant[:80]}')
                if reg_date and reg_date != '–':
                    reg_ref = f' (ref: {reg_num})' if reg_num else ''
                    print(f'    │  Registered:  {reg_date}{reg_ref}')
                if ren_date and ren_date != '–':
                    ren_ref = f' (ref: {ren_num})' if ren_num and ren_num != '–' else ''
                    print(f'    │  Renewed:     {ren_date}{ren_ref}')
                if sim < 0.90:
                    print(f'    │  Confidence:  {sim:.0%}')
                print(f'    └─')
                print()
                row_num += 1

    else:
        if pub_yr and 1923 <= pub_yr <= 1963:
            print('    No renewal records found in Stanford, NYPL, or USCO.')
            print('    This strongly suggests the copyright was NOT renewed.')
            print('    (~93% of works from this era were not renewed.)')
        else:
            print('    Renewal records not applicable for this publication period.')

    # ── 3. Basis ──────────────────────────────────────────────────────────────
    print()
    print('📋  BASIS')
    print(f'    {status["notes"]}')

    # ── 4. Public Domain Timeline ─────────────────────────────────────────────
    if pub_yr:
        print()
        print('📅  PUBLIC DOMAIN TIMELINE')
        if status['status'] == 'Public Domain':
            print('    ✅ This work is currently in the public domain.')
        elif status['status'] == 'In Copyright':
            exp_match = re.search(r'January 1, (\d{4})', status.get('expires', ''))
            if exp_match:
                pd_year = int(exp_match.group(1))
                years_left = pd_year - CURRENT_YEAR
                print(f'    🔒 Protected until: January 1, {pd_year}')
                print(f'       Enters public domain in ~{years_left} year(s).')
            else:
                print(f"    🔒 Enters public domain 70 years after the author's death")
                print(f'       (or January 1, {pub_yr + 96} for works for hire).')
        else:
            print(f'    ❓ Cannot determine — renewal research required.')
            print(f'       If no renewal found: entered PD on January 1, {pub_yr + 29}.')
            print(f'       If renewal found:    protected until January 1, {(pub_yr + 95) + 1}.')

    # ── 5. Confidence ─────────────────────────────────────────────────────────
    print()
    print(f'🎯  CONFIDENCE:  {status["confidence"]}')

    # ── 6. Rights Statement ───────────────────────────────────────────────────
    print()
    print('🏷️   RIGHTS STATEMENT')
    print(f'    {status["rights_statement"]}')
    print(f'    {status["uri"]}')

    # ── 7. Databases Searched ─────────────────────────────────────────────────
    print()
    print('🔍  DATABASES SEARCHED')
    print('    Stanford Copyright Renewals  https://exhibits.stanford.edu/copyrightrenewals')
    print('    NYPL CCE Renewals            https://cce-search.nypl.org/')
    print('    USCO Public Records          https://publicrecords.copyright.gov/')
    if pub_yr and 1923 <= pub_yr <= 1963 and not all_hits:
        ren_window_start = pub_yr + 27
        ren_window_end   = pub_yr + 28
        print(f'    Renewal window for a {pub_yr} work: calendar years {ren_window_start}–{ren_window_end}')

    print()
    print('═' * 65)


def print_history_json(history: dict):
    """Output history as structured JSON for programmatic use."""
    output = {
        'title':            history['query']['title'],
        'author':           history['query']['author'],
        'pub_year':         history['pub_year'],
        'renewed':          history['renewed'],
        'renewal_year':     history['renewal_year'],
        'sources_with_hits': history['sources_with_hits'],
        'status':           history['status'],
        'renewal_records':  [],
    }
    for src_key in ('stanford', 'nypl', 'usco'):
        for rec in history['hits'][src_key]:
            output['renewal_records'].append({'source': src_key, **rec})
    print(json.dumps(output, indent=2, default=str))


# ─────────────────────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print('=' * 65)
    print('  COPYRIGHT NEXUS — Copyright History Lookup')
    print('  Sources: Stanford + NYPL CCE + USCO Renewals')
    print('=' * 65)

    json_mode = '--json' in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith('--')]

    # ── CLI args mode ─────────────────────────────────────────────────────────
    if args:
        title  = args[0]
        author = args[1] if len(args) >= 2 else None

        # Allow optional year as third arg (e.g. 1952)
        pub_year_hint = None
        if len(args) >= 3 and args[2].isdigit():
            pub_year_hint = int(args[2])

        print(f'\n🔍 Searching: "{title}"' + (f' by {author}' if author else ''))
        history = build_history(title, author, pub_year_hint)

        if json_mode:
            print_history_json(history)
        else:
            print_history(history)
        return

    # ── Interactive mode ──────────────────────────────────────────────────────
    print('\nEnter a title and optional author to retrieve complete copyright history.')
    print("Type 'quit' to exit.\n")
    print('Examples:')
    print('  "The Old Man and the Sea" + Hemingway')
    print('  "Gone with the Wind" + Mitchell')
    print('  "Invisible Man" + Ellison')
    print()

    while True:
        try:
            title = input('📚  Title: ').strip()
            if not title:
                continue
            if title.lower() in ['quit', 'exit', 'q']:
                print('\nGoodbye!')
                break

            author_raw = input('✍️   Author (optional — press Enter to skip): ').strip()
            author = author_raw or None

            year_raw = input('📅  Publication year (optional — press Enter to skip): ').strip()
            pub_year_hint = int(year_raw) if year_raw.isdigit() else None

            print(f'\n🔍 Searching renewal databases for "{title}"...')
            history = build_history(title, author, pub_year_hint)
            print_history(history)

        except KeyboardInterrupt:
            print('\n\nGoodbye!')
            break
        except Exception as e:
            print(f'\n❌ Error: {e}')
            print('Check your .env file and Supabase connection.')


if __name__ == '__main__':
    main()
