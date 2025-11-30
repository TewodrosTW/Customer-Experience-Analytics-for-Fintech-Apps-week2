"""
Scrapes Google Play reviews for CBE, BOA, and Dashen Bank.
Saves to data/reviews.csv with columns: review, rating, date, bank, source
"""

import argparse
import json
import logging
import sys
import warnings
from google_play_scraper import reviews, reviews_all, Sort
import pandas as pd
import time
import os

# Ensure data folder exists
os.makedirs('data', exist_ok=True)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description='Scrape Google Play reviews for banks')
    p.add_argument('--count', '-c', type=int, default=450,
                   help='Number of reviews to request per app (default: 450)')
    p.add_argument('--lang', default='en', help='Review language (default: en)')
    p.add_argument('--country', default='us', help='Country store to query (default: us)')
    p.add_argument('--min-per-bank', type=int, default=400,
                   help='Assert at least this many unique reviews per bank after cleaning (default: 400)')
    p.add_argument('--output', '-o', default='data/reviews.csv', help='Output CSV path')
    p.add_argument('--banks-file', help='Path to JSON file mapping bank names to app IDs')
    p.add_argument('--bank', action='append', help='Specify a bank mapping as NAME=APP_ID (can be used multiple times)')
    p.add_argument('--log-level', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    return p.parse_args(argv)

# Bank app IDs (updated to the Play Store app IDs discovered via search)
# Bank app IDs (updated to the Play Store app IDs discovered via search)
banks = {
    "CBE": "com.combanketh.mobilebanking",
    "BOA": "com.boa.boaMobileBanking",
    "Dashen": "com.cr2.amolelight"
}

def main(argv=None):
    args = parse_args(argv)

    # Configure logging
    numeric_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')

    # Prepare banks mapping (allow override from file or CLI)
    banks_local = banks.copy()
    if args.banks_file:
        try:
            with open(args.banks_file, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
                if isinstance(data, dict):
                    banks_local = data
                    logging.info('Loaded banks mapping from %s', args.banks_file)
                else:
                    logging.error('Banks file must contain a JSON object mapping names to app IDs')
                    raise SystemExit(1)
        except Exception as e:
            logging.error('Failed to load banks file: %s', e)
            raise

    if args.bank:
        for pair in args.bank:
            if '=' not in pair:
                logging.error("Invalid --bank value: %s. Expect NAME=APP_ID", pair)
                raise SystemExit(1)
            name, appid = pair.split('=', 1)
            banks_local[name.strip()] = appid.strip()
        logging.info('Applied %d --bank overrides', len(args.bank))

    all_reviews = []
    per_bank_raw = {}

    # Scrape reviews per bank (request many, then dedupe)
    for bank_name, app_id in banks_local.items():
        logging.info("Scraping %s reviews (app=%s) using lang=%s, country=%s...", bank_name, app_id, args.lang, args.country)

        try:
            # Try the paginated `reviews` first (requests up to `count` items)
            result, _ = reviews(
                app_id,
                lang=args.lang,
                country=args.country,
                sort=Sort.NEWEST,
                count=args.count
            )

            # If `reviews` returned nothing, fall back to `reviews_all` which
            # iterates through continuations and tends to be more reliable.
            if not result:
                logging.info("  → 'reviews' returned 0 reviews; trying 'reviews_all' fallback...")
                result = reviews_all(
                    app_id,
                    lang=args.lang,
                    country=args.country,
                    sort=Sort.NEWEST
                )

            # Record raw count for reporting
            per_bank_raw[bank_name] = len(result)

            # Process and normalize each review before storing
            processed = 0
            for r in result:
                content = r.get('content') or r.get('review') or ''
                score = r.get('score') or r.get('rating') or None
                at = r.get('at')
                if hasattr(at, 'strftime'):
                    date_str = at.strftime('%Y-%m-%d')
                else:
                    date_str = str(at) if at else ''

                all_reviews.append({
                    'review': content,
                    'rating': score,
                    'date': date_str,
                    'bank': bank_name,
                    'source': f'Google Play ({args.country.upper()})'
                })
                processed += 1

            logging.info("  → Got %d raw reviews", processed)

        except Exception as e:
            logging.error("  → ERROR for %s: %s", bank_name, str(e))
            per_bank_raw[bank_name] = 0

        # Be respectful to Google's servers
        time.sleep(2)

    # Convert to DataFrame
    df = pd.DataFrame(all_reviews)

    # Basic data-quality checks & summary
    logging.info('\n--- Data Quality Summary ---')
    if df.empty:
        warnings.warn('No reviews were collected. Check app IDs, network, or Play Store availability.')
    else:
        total = len(df)
        logging.info('Total raw reviews collected: %d', total)

        # Count per bank (raw -> after dedupe)
        raw_counts = per_bank_raw
        logging.info('\nRaw counts per bank:')
        for bank, cnt in raw_counts.items():
            logging.info(' - %s: %d', bank, cnt)

        # Remove duplicates (same review text)
        before_dedup = len(df)
        df = df.drop_duplicates(subset=['review'])
        after_dedup = len(df)
        logging.info('After deduplication: %d (removed %d)', after_dedup, before_dedup - after_dedup)

        # Missing values summary
        logging.info('\nMissing values per column:')
        na = df.isna().sum()
        for col, cnt in na.items():
            pct = cnt / len(df) * 100
            logging.info(' - %s: %d missing (%.1f%%)', col, cnt, pct)

        # Per-bank final counts
        logging.info('\nFinal counts per bank:')
        final_counts = df['bank'].value_counts(dropna=False).to_dict()
        for bank in banks_local.keys():
            logging.info(' - %s: %d', bank, final_counts.get(bank, 0))

        # Show a few problematic rows (empty review texts)
        empty_reviews = df[df['review'].str.strip() == '']
        if not empty_reviews.empty:
            logging.warning('%d reviews with empty text found. Showing up to 3', len(empty_reviews))
            logging.warning('%s', empty_reviews.head(3).to_dict(orient='records'))

        # Optional assertion to enforce minimum per bank
        if args.min_per_bank and args.min_per_bank > 0:
            failing = [b for b, cnt in final_counts.items() if cnt < args.min_per_bank]
            if failing:
                raise AssertionError(f"Banks with fewer than {args.min_per_bank} reviews: {failing}")

    # Save to CSV
    output_path = args.output
    df.to_csv(output_path, index=False)
    logging.info("\n✅ Total clean reviews: %d", len(df))
    logging.info("✅ Saved to %s", output_path)


if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except AssertionError:
        raise