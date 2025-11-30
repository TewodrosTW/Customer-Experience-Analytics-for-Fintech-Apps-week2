"""
Scrapes Google Play reviews for CBE, BOA, and Dashen Bank.
Saves to data/reviews.csv with columns: review, rating, date, bank, source
"""

import argparse
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
    p.add_argument('--min-per-bank', type=int, default=0,
                   help='Optional: assert at least this many unique reviews per bank (0 = disabled)')
    p.add_argument('--output', '-o', default='data/reviews.csv', help='Output CSV path')
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

    all_reviews = []
    per_bank_raw = {}

    # Scrape reviews per bank (request many, then dedupe)
    for bank_name, app_id in banks.items():
        print(f"Scraping {bank_name} reviews (app={app_id}) using lang={args.lang}, country={args.country}...")

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
                print("  → 'reviews' returned 0 reviews; trying 'reviews_all' fallback...")
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

            print(f"  → Got {processed} raw reviews")

        except Exception as e:
            print(f"  → ERROR for {bank_name}: {str(e)}")
            per_bank_raw[bank_name] = 0

        # Be respectful to Google's servers
        time.sleep(2)

    # Convert to DataFrame
    df = pd.DataFrame(all_reviews)

    # Basic data-quality checks & summary
    print('\n--- Data Quality Summary ---')
    if df.empty:
        warnings.warn('No reviews were collected. Check app IDs, network, or Play Store availability.')
    else:
        total = len(df)
        print(f'Total raw reviews collected: {total}')

        # Count per bank (raw -> after dedupe)
        raw_counts = per_bank_raw
        print('\nRaw counts per bank:')
        for bank, cnt in raw_counts.items():
            print(f' - {bank}: {cnt}')

        # Remove duplicates (same review text)
        before_dedup = len(df)
        df = df.drop_duplicates(subset=['review'])
        after_dedup = len(df)
        print(f'After deduplication: {after_dedup} (removed {before_dedup - after_dedup})')

        # Missing values summary
        print('\nMissing values per column:')
        na = df.isna().sum()
        for col, cnt in na.items():
            pct = cnt / len(df) * 100
            print(f' - {col}: {cnt} missing ({pct:.1f}%)')

        # Per-bank final counts
        print('\nFinal counts per bank:')
        final_counts = df['bank'].value_counts(dropna=False).to_dict()
        for bank in banks.keys():
            print(f' - {bank}: {final_counts.get(bank, 0)}')

        # Show a few problematic rows (empty review texts)
        empty_reviews = df[df['review'].str.strip() == '']
        if not empty_reviews.empty:
            print(f"\nWarning: {len(empty_reviews)} reviews with empty text found. Showing up to 3:")
            print(empty_reviews.head(3).to_dict(orient='records'))

        # Optional assertion to enforce minimum per bank
        if args.min_per_bank and args.min_per_bank > 0:
            failing = [b for b, cnt in final_counts.items() if cnt < args.min_per_bank]
            if failing:
                raise AssertionError(f"Banks with fewer than {args.min_per_bank} reviews: {failing}")

    # Save to CSV
    output_path = args.output
    df.to_csv(output_path, index=False)
    print(f"\n✅ Total clean reviews: {len(df)}")
    print(f"✅ Saved to {output_path}")


if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except AssertionError:
        raise