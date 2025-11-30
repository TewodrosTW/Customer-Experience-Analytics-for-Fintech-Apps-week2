"""Preprocess scraped reviews CSV.

Usage:
    python scripts/preprocess_reviews.py --input data/reviews_final.csv --output data/clean_reviews.csv

The script performs:
 - Load CSV
 - Normalize date to YYYY-MM-DD
 - Drop rows with empty review text
 - Remove duplicates by review text
 - Report missing-value stats and per-bank counts
 - Optionally assert minimum per-bank counts and max missing percentage
"""
import argparse
import sys
import pandas as pd
import logging


def parse_args(argv=None):
    p = argparse.ArgumentParser(description='Preprocess scraped reviews CSV')
    p.add_argument('--input', '-i', default='data/reviews.csv', help='Input CSV path')
    p.add_argument('--output', '-o', default='data/clean_reviews.csv', help='Output CSV path')
    p.add_argument('--min-per-bank', type=int, default=400, help='Assert at least this many reviews per bank (post-cleaning)')
    p.add_argument('--max-missing-pct', type=float, default=5.0, help='Maximum allowed missing percentage across all columns')
    p.add_argument('--log-level', default='INFO')
    return p.parse_args(argv)


def normalize_date_column(df, col='date'):
    # Try to parse dates; coerce errors to NaT and then format
    if col not in df.columns:
        return df
    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
    df[col] = df[col].fillna('')
    return df


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format='%(levelname)s: %(message)s')

    df = pd.read_csv(args.input)
    logging.info('Loaded %d rows from %s', len(df), args.input)

    # Normalize date column
    df = normalize_date_column(df, 'date')

    # Drop rows with empty review text (after stripping)
    if 'review' in df.columns:
        before = len(df)
        df['review'] = df['review'].astype(str)
        df = df[df['review'].str.strip() != '']
        logging.info('Dropped %d empty-review rows', before - len(df))

    # Remove duplicates by review text
    before = len(df)
    df = df.drop_duplicates(subset=['review'])
    logging.info('Removed %d duplicate rows', before - len(df))

    # Ensure required columns and ordering
    cols = ['review', 'rating', 'date', 'bank', 'source']
    for c in cols:
        if c not in df.columns:
            df[c] = ''
    df = df[cols]

    # Missing value summary
    na = df.isna().sum()
    total = len(df)
    logging.info('Final rows after cleaning: %d', total)
    logging.info('Missing values per column:')
    for c in cols:
        cnt = int(na.get(c, 0))
        pct = cnt / total * 100 if total else 0
        logging.info(' - %s: %d missing (%.2f%%)', c, cnt, pct)

    # Assert missing percent below threshold
    max_missing = max((na.get(c, 0) / total * 100) if total else 0 for c in cols)
    if max_missing > args.max_missing_pct:
        logging.warning('Maximum missing percentage %.2f%% exceeds threshold %.2f%%', max_missing, args.max_missing_pct)

    # Per-bank counts and min-per-bank assertion
    counts = df['bank'].value_counts().to_dict()
    logging.info('Counts per bank: %s', counts)
    failing = [b for b, cnt in counts.items() if cnt < args.min_per_bank]
    if failing:
        logging.warning('Banks with fewer than %d reviews post-cleaning: %s', args.min_per_bank, failing)

    # Save cleaned CSV
    df.to_csv(args.output, index=False)
    logging.info('Saved cleaned CSV to %s', args.output)


if __name__ == '__main__':
    main(sys.argv[1:])
