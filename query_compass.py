#!/usr/bin/env python3
"""
query_compass.py — Copyright Compass Interactive Query Interface

Data sources queried on every applicable question:
  1. Knowledge chunks (Supabase pgvector) — copyright law & decision rules
  2. Stanford Renewal DB (copyright_renewals) — 246k book renewals, 1923-1963 pubs
  3. NYPL CCE Renewals (nypl_renewals) — all classes 1950-1991, incl. CO database

Together: Stanford covers 1950-1992 books via title/author search.
          NYPL covers 1950-1991 all classes with better registration matching,
          and the 1978-1991 CO database window (books published 1950-1963).

Usage:
    py -3.12 query_compass.py
"""

import os
import re
import json
import time
from datetime import date
from dotenv import load_dotenv
from supabase import create_client
from openai import OpenAI

load_dotenv()

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)
openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

EMBEDDING_MODEL = "text-embedding-ada-002"


# ── Embeddings ─────────────────────────────────────────────────────────────────

def generate_embedding(text: str) -> list[float]:
    response = openai_client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=text
    )
    return response.data[0].embedding


# ── RAG chunk search ───────────────────────────────────────────────────────────

def detect_filters(question: str) -> dict:
    filters = {}
    question_lower = question.lower()

    year_match = re.search(r'\b(1[89]\d{2}|20[012]\d)\b', question)
    if year_match:
        year = int(year_match.group())
        if year < 1930:
            filters['filter_date'] = ['pre-1930']
        elif year <= 1963:
            filters['filter_date'] = ['1930-1963']
        elif year <= 1977:
            filters['filter_date'] = ['1964-1977']
        elif year <= 1989:
            filters['filter_date'] = ['1978-1989']
        else:
            filters['filter_date'] = ['post-1989']

    if any(w in question_lower for w in ['published', 'book', 'magazine', 'newspaper', 'printed']):
        filters['filter_material'] = ['published']
    elif any(w in question_lower for w in ['unpublished', 'letter', 'manuscript', 'diary', 'personal']):
        filters['filter_material'] = ['unpublished']
    elif any(w in question_lower for w in ['government', 'federal', 'agency', 'congress']):
        filters['filter_material'] = ['government']

    return filters


def search_chunks(question: str, match_count: int = 3) -> list:
    query_embedding = generate_embedding(question)
    filters = detect_filters(question)

    params = {
        'query_embedding':  query_embedding,
        'match_threshold':  0.65,
        'match_count':      match_count,
        'filter_topic':     None,
        'filter_date':      filters.get('filter_date'),
        'filter_material':  filters.get('filter_material')
    }

    result = supabase.rpc('match_copyright_chunks', params).execute()
    return result.data


# ── LLM work info extractor ────────────────────────────────────────────────────

def extract_work_info(question: str) -> dict:
    """Use LLM to extract title, author, year from natural language question."""
    response = openai_client.chat.completions.create(
        model="gpt-4-turbo-preview",
        messages=[
            {
                "role": "system",
                "content": """Extract bibliographic information from copyright questions.
Respond with ONLY a JSON object, no other text, no markdown:
{
  "title": "exact title or null",
  "author": "author name or null",
  "year": 1952,
  "needs_renewal_check": true
}

Rules:
- title: exact commonly-known title, or null if no specific work mentioned
- author: last name or full name, or null
- year: publication year as integer, or null
- needs_renewal_check: true if published work MIGHT be in 1923-1963 range

Examples:
"Is Old Man and the Sea by Hemingway still in copyright" ->
  {"title": "The Old Man and the Sea", "author": "Hemingway", "year": 1952, "needs_renewal_check": true}

"What about Gone with the Wind" ->
  {"title": "Gone with the Wind", "author": "Margaret Mitchell", "year": 1936, "needs_renewal_check": true}

"Can I use letters from my grandmother who died in 1955" ->
  {"title": null, "author": null, "year": null, "needs_renewal_check": false}

"Is a book from 1975 in copyright" ->
  {"title": null, "author": null, "year": 1975, "needs_renewal_check": false}"""
            },
            {"role": "user", "content": question}
        ],
        temperature=0,
        max_tokens=120
    )

    try:
        text = response.choices[0].message.content.strip()
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text)
        return json.loads(text)
    except Exception:
        return {"title": None, "author": None, "year": None, "needs_renewal_check": False}


# ── Stanford renewal lookup ────────────────────────────────────────────────────

def _lookup_stanford(title: str, author: str | None, year: int | None) -> dict | None:
    try:
        result = supabase.rpc('search_renewals', {
            'search_title':    title,
            'search_author':   author,
            'search_pub_year': year,
            'result_limit':    5
        }).execute()

        records = result.data or []
        if not records:
            return None

        best  = records[0]
        score = best.get('similarity_score', 0)
        if score < 0.40:
            return None

        pub_yr   = best.get('pub_year')
        exp_year = (pub_yr + 95) if pub_yr else None

        return {
            'source':       'Stanford Renewal DB',
            'title':        best.get('title'),
            'author':       best.get('author'),
            'pub_year':     pub_yr,
            'reg_num':      best.get('reg_num'),
            'reg_date':     best.get('reg_date'),
            'renewal_num':  best.get('renewal_num'),
            'renewal_date': best.get('renewal_date'),
            'claimant':     best.get('claimant'),
            'similarity':   score,
            'expiration_year': exp_year,
        }

    except Exception as e:
        print(f"   ⚠  Stanford lookup error: {e}")
        return None


# ── NYPL renewal lookup ────────────────────────────────────────────────────────

def _lookup_nypl(title: str, author: str | None, year: int | None) -> dict | None:
    """
    Query NYPL CCE renewals (all classes, 1950-1991).
    Includes 1978-1991 CO database window — catches books published 1950-1963.
    Retries up to 3 times with exponential backoff on transient errors.
    """
    max_retries = 3
    base_delay  = 1.0   # seconds — doubles each retry: 1s, 2s, 4s

    for attempt in range(1, max_retries + 1):
        try:
            result = supabase.rpc('search_nypl_renewals', {
                'search_title':    title,
                'search_author':   author,
                'search_pub_year': year,
                'result_limit':    5
            }).execute()

            records = result.data or []
            if not records:
                return None

            best  = records[0]
            score = best.get('similarity_score', 0)
            if score < 0.40:
                return None

            pub_yr   = best.get('pub_year')
            exp_year = (pub_yr + 95) if pub_yr else None

            return {
                'source':       'NYPL CCE Renewals (CO database)',
                'title':        best.get('title'),
                'author':       best.get('author'),
                'claimants':    best.get('claimants'),
                'pub_year':     pub_yr,
                'oreg':         best.get('oreg'),
                'odat':         best.get('odat'),
                'renewal_id':   best.get('renewal_id'),
                'rdat':         best.get('rdat'),
                'renewal_year': best.get('renewal_year'),
                'similarity':   score,
                'expiration_year': exp_year,
            }

        except Exception as e:
            if attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))   # 1s, 2s, 4s
                print(f"   ⚠  NYPL lookup error (attempt {attempt}/{max_retries}): {e}")
                print(f"      Retrying in {delay:.0f}s...")
                time.sleep(delay)
            else:
                print(f"   ⚠  NYPL lookup failed after {max_retries} attempts: {e}")
                return None


# ── Combined renewal lookup ────────────────────────────────────────────────────

def lookup_renewal(question: str) -> dict:
    """
    Query Stanford AND NYPL renewals. Returns combined result dict with keys:
      applicable, found, title, author, year, stanford, nypl
    """
    info = extract_work_info(question)

    if not info.get('needs_renewal_check'):
        return {'applicable': False}

    title  = info.get('title')
    author = info.get('author')
    year   = info.get('year')

    if not title:
        return {'applicable': False}

    if year and not (1923 <= year <= 1963):
        return {'applicable': False}

    year_display = str(year) if year else "year unknown"
    print(f"\n   🔍 Renewal check: \"{title}\"", end="")
    if author:
        print(f" by {author}", end="")
    print(f" ({year_display})")

    print(f"      → Stanford Renewal DB...", end="  ", flush=True)
    stanford = _lookup_stanford(title, author, year)
    print("✅ FOUND" if stanford else "not found")

    print(f"      → NYPL CCE Renewals...", end="     ", flush=True)
    nypl = _lookup_nypl(title, author, year)
    print("✅ FOUND" if nypl else "not found")

    return {
        'applicable': True,
        'found':      bool(stanford or nypl),
        'title':      title,
        'author':     author,
        'year':       year,
        'stanford':   stanford,
        'nypl':       nypl,
    }


# ── Context builder ────────────────────────────────────────────────────────────

def format_renewal_context(renewal: dict) -> str:
    if not renewal.get('applicable'):
        return ""

    title    = renewal['title']
    author   = renewal.get('author', '')
    year     = renewal.get('year')
    found    = renewal['found']
    stanford = renewal.get('stanford')
    nypl     = renewal.get('nypl')

    byline  = f" by {author}" if author else ""
    publine = f" (pub. {year})" if year else ""

    lines = [
        "\n\n--- RENEWAL DATABASE LOOKUP ---",
        f"Work: \"{title}\"{byline}{publine}",
        "Sources checked: Stanford Renewal DB (246k books) + "
        "NYPL CCE Renewals (445k records, all classes, 1950-1991)",
    ]

    if found:
        # Determine the best expiration year from whichever source found it
        best_exp = None
        if stanford and stanford.get('expiration_year'):
            best_exp = stanford['expiration_year']
        if nypl and nypl.get('expiration_year'):
            if best_exp is None or nypl['expiration_year'] > best_exp:
                best_exp = nypl['expiration_year']

        # Copyright expires on January 1 of (expiration_year + 1).
        # If today's year is already past that date, it's public domain.
        today_year = date.today().year
        already_expired = best_exp is not None and today_year > best_exp

        if already_expired:
            lines.append(
                f"RESULT: RENEWAL WAS FILED — but COPYRIGHT HAS NOW EXPIRED.\n"
                f"        Expiration: January 1, {best_exp + 1}  |  Today: {today_year}\n"
                f"        THIS WORK IS NOW PUBLIC DOMAIN.\n"
            )
        else:
            lines.append("RESULT: RENEWAL CONFIRMED — Work IS IN COPYRIGHT\n")

        if stanford:
            exp = stanford.get('expiration_year') or 0
            lines += [
                "  [Stanford Renewal DB]",
                f"  Matched title:  {stanford.get('title')}",
                f"  Original reg:   {stanford.get('reg_num')} ({stanford.get('reg_date')})",
                f"  Renewal:        {stanford.get('renewal_num')} ({stanford.get('renewal_date')})",
                f"  Renewed by:     {stanford.get('claimant', 'unknown')}",
                f"  Confidence:     {stanford.get('similarity', 0):.0%}",
                f"  Expires:        January 1, {exp + 1}" if exp else "  Expires: unknown",
            ]

        if nypl:
            exp = nypl.get('expiration_year') or 0
            lines += [
                "  [NYPL CCE Renewals]",
                f"  Matched title:  {nypl.get('title')}",
                f"  Original reg:   {nypl.get('oreg')} ({nypl.get('odat')})",
                f"  Renewal:        {nypl.get('renewal_id')} ({nypl.get('rdat')})",
                f"  Claimant:       {nypl.get('claimants', 'unknown')}",
                f"  Confidence:     {nypl.get('similarity', 0):.0%}",
                f"  Expires:        January 1, {exp + 1}" if exp else "  Expires: unknown",
            ]

        if already_expired:
            lines += [
                "\nRights Statement: No Copyright - United States",
                "URI: https://rightsstatements.org/vocab/NoC-US/1.0/",
            ]
        else:
            lines += [
                "\nRights Statement: In Copyright",
                "URI: https://rightsstatements.org/vocab/InC/1.0/",
            ]

    else:
        lines += [
            "RESULT: NO RENEWAL RECORD FOUND in either database.",
            "Implication: Copyright almost certainly was NOT renewed.",
            "This work is very likely PUBLIC DOMAIN.",
            "",
            "Rights Statement: No Copyright - United States",
            "URI: https://rightsstatements.org/vocab/NoC-US/1.0/",
            "",
            "⚠  URAA CAVEAT: If this work was first published OUTSIDE the US,",
            "   the URAA (1994) may have retroactively restored copyright.",
            "   Verify country of first publication before concluding public domain.",
            "",
            "For manual verification:",
            "  Stanford:  https://exhibits.stanford.edu/copyrightrenewals",
            "  NYPL:      https://cce-search.nypl.org/",
            "  USCO CPRS: https://publicrecords.copyright.gov/",
        ]

    lines.append("--- END RENEWAL LOOKUP ---")
    return "\n".join(lines)


# ── Answer generation ──────────────────────────────────────────────────────────

def generate_answer(question: str, chunks: list, renewal: dict) -> str:
    renewal_context = format_renewal_context(renewal)

    if not chunks and not renewal_context:
        return (
            "I couldn't find relevant information for your question. "
            "Try specifying the publication date and whether the work is "
            "published or unpublished."
        )

    context_parts = [
        f"[Knowledge Base — {c['chunk_id']}]\n{c['content']}"
        for c in chunks
    ]
    if renewal_context:
        context_parts.append(renewal_context)

    context = "\n\n---\n\n".join(context_parts)

    response = openai_client.chat.completions.create(
        model="gpt-4-turbo-preview",
        messages=[
            {
                "role": "system",
                "content": f"""You are Copyright Compass, an expert assistant helping
cultural heritage professionals determine copyright status of materials in their collections.
Today's date is {date.today().strftime('%B %d, %Y')}. Use this when evaluating whether a copyright expiration date has already passed.

Answer based ONLY on the provided context. Always include:

1. COPYRIGHT STATUS — Public Domain / In Copyright / Undetermined
2. RIGHTS STATEMENT — Exact RightsStatements.org label and URI
3. CONFIDENCE — High / Medium / Low with brief explanation
4. REASONING — Concise legal basis
5. ACTION ITEMS — Any remaining research steps needed

When renewal lookup results appear in context:
- RENEWAL CONFIRMED → In Copyright, state expiration year. Confidence: High.
- NO RENEWAL FOUND (both DBs searched) → Public Domain (NoC-US).
  High confidence for US works. Flag URAA caveat for foreign works.
- Always cite the specific renewal record number or database when a record was found.

Be direct and practical."""
            },
            {
                "role": "user",
                "content": f"CONTEXT:\n{context}\n\n---\n\nQUESTION: {question}"
            }
        ],
        temperature=0.2
    )

    return response.choices[0].message.content


# ── Main loop ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  COPYRIGHT COMPASS — Interactive Query Interface")
    print("  Sources: Knowledge Base + Stanford + NYPL CCE Renewals")
    print("=" * 65)
    print("\nAsk questions about copyright status of any material.")
    print("Type 'quit' to exit.\n")
    print("Examples:")
    print('  - Is "The Old Man and the Sea" by Hemingway still in copyright?')
    print('  - What about "Gone with the Wind" published in 1936?')
    print("  - Unpublished letters from someone who died in 1950?")
    print("  - Can I use a federal government report freely?")
    print()

    while True:
        try:
            question = input("\n📚 Your question: ").strip()

            if not question:
                continue
            if question.lower() in ['quit', 'exit', 'q']:
                print("\nGoodbye!")
                break

            print("\n🔍 Searching knowledge base...")
            chunks = search_chunks(question)
            if chunks:
                print(f"   Found {len(chunks)} relevant chunks:")
                for c in chunks:
                    print(f"   - {c['chunk_id']} (similarity: {c['similarity']:.2f})")

            renewal = lookup_renewal(question)

            print("\n💡 Generating answer...\n")
            answer = generate_answer(question, chunks, renewal)

            print("-" * 65)
            print(answer)
            print("-" * 65)

        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            print("Check your .env file and Supabase connection.")


if __name__ == "__main__":
    main()
