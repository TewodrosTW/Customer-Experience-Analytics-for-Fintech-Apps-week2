#!/usr/bin/env python3
"""Find likely Play Store package IDs for a bank name using google_play_scraper.search

Outputs JSON list of candidate apps to `--output` (default: `data/<query>_apps.json`).
"""
import argparse
import json
from pathlib import Path

try:
    from google_play_scraper import search
except Exception as e:
    raise SystemExit('google_play_scraper is required. Install it in the venv: pip install google-play-scraper')


def find_apps(query: str, limit: int = 20):
    # search returns list of dicts with 'appId', 'title', 'summary'
    return search(query, lang='en', country='us', n_hits=limit)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--query', '-q', required=True, help='Search query (e.g. "Dashen Bank")')
    p.add_argument('--limit', '-n', type=int, default=20, help='Max results to return')
    p.add_argument('--output', '-o', help='Output JSON path (defaults to data/<query>_apps.json)')
    args = p.parse_args()

    results = find_apps(args.query, args.limit)
    out_path = Path(args.output) if args.output else Path('data') / (args.query.replace(' ', '_') + '_apps.json')
    out_path.parent.mkdir(parents=True, exist_ok=True)

    simplified = []
    for r in results:
        simplified.append({
            'appId': r.get('appId'),
            'title': r.get('title'),
            'summary': r.get('summary'),
            'score': r.get('score') if 'score' in r else None,
            'installs': r.get('installs') if 'installs' in r else None,
        })

    out_path.write_text(json.dumps({'query': args.query, 'candidates': simplified}, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f'Wrote {len(simplified)} candidates to {out_path}')


if __name__ == '__main__':
    main()
