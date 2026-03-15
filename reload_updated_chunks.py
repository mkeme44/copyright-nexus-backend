#!/usr/bin/env python3
"""
reload_updated_chunks.py — Re-upsert the three 2026 date-updated chunks

Chunks updated for 2026 rolling date changes (per Minnesota Rights Review 2026):
  03_pre1930_published      → threshold 1930 → 1931
  09_unpublished_known_creator → death threshold 1955 → 1956
  10_unpublished_unknown_creator → creation threshold 1905 → 1906

Uses upsert on chunk_id so existing rows are updated in place.
No other chunks are touched.

Usage:
    py -3.12 reload_updated_chunks.py
"""

import os
import re
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
from openai import OpenAI

load_dotenv()

supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

CHUNKS_TO_UPDATE = [
    "03_pre1930_published.md",
    "09_unpublished_known_creator.md",
    "10_unpublished_unknown_creator.md",
]

# ── Parser (same logic as load_chunks.py) ─────────────────────────────────────

def parse_chunk_file(filepath: Path) -> dict:
    content = filepath.read_text(encoding='utf-8')
    chunk_id = filepath.stem
    metadata = {
        'chunk_id': chunk_id,
        'topic_area': [],
        'date_relevance': [],
        'material_type': [],
        'confidence_level': None
    }

    metadata_match = re.search(r'## Metadata\n(.*?)\n## Content', content, re.DOTALL)
    if metadata_match:
        metadata_text = metadata_match.group(1)
        m = re.search(r'chunk_id:\s*(\S+)', metadata_text)
        if m: metadata['chunk_id'] = m.group(1)
        m = re.search(r'topic_area:\s*(.+)', metadata_text)
        if m: metadata['topic_area'] = [t.strip() for t in m.group(1).split(',')]
        m = re.search(r'date_relevance:\s*(.+)', metadata_text)
        if m: metadata['date_relevance'] = [d.strip() for d in m.group(1).split(',')]
        m = re.search(r'material_type:\s*(.+)', metadata_text)
        if m: metadata['material_type'] = [x.strip() for x in m.group(1).split(',')]
        m = re.search(r'confidence_level:\s*(\S+)', metadata_text)
        if m: metadata['confidence_level'] = m.group(1)

    content_match = re.search(r'## Content\n(.+)', content, re.DOTALL)
    metadata['content'] = content_match.group(1).strip() if content_match else content
    return metadata


def generate_embedding(text: str) -> list[float]:
    if len(text) > 30000:
        text = text[:30000]
    resp = openai_client.embeddings.create(model="text-embedding-ada-002", input=text)
    return resp.data[0].embedding


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Reloading 2026 date-updated chunks")
    print("=" * 60)

    # Look for the chunk files in the same directory as this script,
    # then fall back to a chunks/ subdirectory
    script_dir = Path(__file__).parent
    search_dirs = [script_dir, script_dir / "chunks"]

    ok_count = 0
    for filename in CHUNKS_TO_UPDATE:
        filepath = None
        for d in search_dirs:
            candidate = d / filename
            if candidate.exists():
                filepath = candidate
                break

        if not filepath:
            print(f"\n✗ Not found: {filename}")
            print(f"  Place the updated .md files in the same folder as this script.")
            continue

        print(f"\nProcessing: {filename}")
        chunk = parse_chunk_file(filepath)
        print(f"  chunk_id:  {chunk['chunk_id']}")
        print(f"  content:   {len(chunk['content'])} chars")
        print(f"  Generating embedding...", end="  ", flush=True)

        try:
            embedding = generate_embedding(chunk['content'])
            record = {
                'chunk_id':        chunk['chunk_id'],
                'content':         chunk['content'],
                'topic_area':      chunk['topic_area'],
                'date_relevance':  chunk['date_relevance'],
                'material_type':   chunk['material_type'],
                'confidence_level': chunk['confidence_level'],
                'embedding':       embedding,
            }
            supabase.table('copyright_chunks').upsert(
                record, on_conflict='chunk_id'
            ).execute()
            print("✓ upserted")
            ok_count += 1
        except Exception as e:
            print(f"✗ ERROR: {e}")

    print()
    print("=" * 60)
    print(f"  Updated {ok_count}/{len(CHUNKS_TO_UPDATE)} chunks")
    if ok_count == len(CHUNKS_TO_UPDATE):
        print("  ✓ All chunks current for 2026")
    print("=" * 60)


if __name__ == "__main__":
    main()
