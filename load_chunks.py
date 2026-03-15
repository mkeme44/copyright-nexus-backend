#!/usr/bin/env python3
"""
load_chunks.py - Load Copyright Compass chunks into Supabase with embeddings

Usage:
    1. Install dependencies: pip install supabase openai python-dotenv
    2. Create a .env file with your keys (see .env.example)
    3. Run: python load_chunks.py

This script:
    - Reads all markdown chunk files from the chunks/ directory
    - Extracts metadata from the header
    - Generates embeddings via OpenAI
    - Inserts everything into Supabase
"""

import os
import re
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
from openai import OpenAI
import time

# Load environment variables
load_dotenv()

# Configuration
CHUNKS_DIR = Path("chunks")
EMBEDDING_MODEL = "text-embedding-ada-002"

# Initialize clients
supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)
openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])


def parse_chunk_file(filepath: Path) -> dict:
    """
    Parse a chunk markdown file and extract metadata and content.
    
    Expected format:
        # Chunk XX: Title
        
        ## Metadata
        - chunk_id: xxx
        - topic_area: xxx
        - date_relevance: xxx
        - material_type: xxx
        - confidence_level: xxx (optional)
        
        ## Content
        ...
    """
    content = filepath.read_text(encoding='utf-8')
    
    # Extract chunk_id from filename as fallback
    chunk_id = filepath.stem
    
    # Try to extract metadata from content
    metadata = {
        'chunk_id': chunk_id,
        'topic_area': [],
        'date_relevance': [],
        'material_type': [],
        'confidence_level': None
    }
    
    # Parse metadata section
    metadata_match = re.search(r'## Metadata\n(.*?)\n## Content', content, re.DOTALL)
    if metadata_match:
        metadata_text = metadata_match.group(1)
        
        # Extract each field
        chunk_id_match = re.search(r'chunk_id:\s*(\S+)', metadata_text)
        if chunk_id_match:
            metadata['chunk_id'] = chunk_id_match.group(1)
        
        topic_match = re.search(r'topic_area:\s*(.+)', metadata_text)
        if topic_match:
            topics = topic_match.group(1).strip()
            metadata['topic_area'] = [t.strip() for t in topics.split(',')]
        
        date_match = re.search(r'date_relevance:\s*(.+)', metadata_text)
        if date_match:
            dates = date_match.group(1).strip()
            metadata['date_relevance'] = [d.strip() for d in dates.split(',')]
        
        material_match = re.search(r'material_type:\s*(.+)', metadata_text)
        if material_match:
            materials = material_match.group(1).strip()
            metadata['material_type'] = [m.strip() for m in materials.split(',')]
        
        confidence_match = re.search(r'confidence_level:\s*(\S+)', metadata_text)
        if confidence_match:
            metadata['confidence_level'] = confidence_match.group(1)
    
    # Extract content section (everything after ## Content)
    content_match = re.search(r'## Content\n(.+)', content, re.DOTALL)
    if content_match:
        metadata['content'] = content_match.group(1).strip()
    else:
        # Fallback: use everything after the metadata section
        metadata['content'] = content
    
    return metadata


def generate_embedding(text: str) -> list[float]:
    """Generate embedding for text using OpenAI."""
    # Truncate if needed (ada-002 has 8191 token limit)
    if len(text) > 30000:  # Rough character limit
        text = text[:30000]
    
    response = openai_client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=text
    )
    return response.data[0].embedding


def load_chunk_to_supabase(chunk_data: dict) -> bool:
    """Insert a chunk into Supabase."""
    try:
        # Generate embedding for the content
        print(f"  Generating embedding...")
        embedding = generate_embedding(chunk_data['content'])
        
        # Prepare the record
        record = {
            'chunk_id': chunk_data['chunk_id'],
            'content': chunk_data['content'],
            'topic_area': chunk_data['topic_area'],
            'date_relevance': chunk_data['date_relevance'],
            'material_type': chunk_data['material_type'],
            'confidence_level': chunk_data['confidence_level'],
            'embedding': embedding
        }
        
        # Upsert (insert or update if exists)
        result = supabase.table('copyright_chunks').upsert(
            record,
            on_conflict='chunk_id'
        ).execute()
        
        return True
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def main():
    """Main function to load all chunks."""
    print("=" * 60)
    print("Copyright Compass - Chunk Loader")
    print("=" * 60)
    
    # Check for chunks directory
    if not CHUNKS_DIR.exists():
        print(f"ERROR: Chunks directory '{CHUNKS_DIR}' not found!")
        print("Make sure you're running this from the copyright_rag_corpus directory.")
        return
    
    # Get all markdown files
    chunk_files = sorted(CHUNKS_DIR.glob("*.md"))
    print(f"\nFound {len(chunk_files)} chunk files to process.\n")
    
    if not chunk_files:
        print("No chunk files found!")
        return
    
    # Process each chunk
    success_count = 0
    error_count = 0
    
    for i, filepath in enumerate(chunk_files, 1):
        print(f"[{i}/{len(chunk_files)}] Processing: {filepath.name}")
        
        # Parse the file
        chunk_data = parse_chunk_file(filepath)
        print(f"  Chunk ID: {chunk_data['chunk_id']}")
        print(f"  Topics: {chunk_data['topic_area']}")
        print(f"  Content length: {len(chunk_data['content'])} chars")
        
        # Load to Supabase
        if load_chunk_to_supabase(chunk_data):
            print(f"  ✓ Loaded successfully")
            success_count += 1
        else:
            print(f"  ✗ Failed to load")
            error_count += 1
        
        # Small delay to avoid rate limits
        time.sleep(0.5)
        print()
    
    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Successfully loaded: {success_count}")
    print(f"Errors: {error_count}")
    print(f"Total: {len(chunk_files)}")
    
    if error_count == 0:
        print("\n✓ All chunks loaded successfully!")
    else:
        print(f"\n⚠ {error_count} chunks failed. Check the errors above.")


if __name__ == "__main__":
    main()
