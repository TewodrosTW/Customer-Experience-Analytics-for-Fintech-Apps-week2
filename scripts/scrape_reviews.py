"""
Scrapes Google Play reviews for CBE, BOA, and Dashen Bank.
Saves to data/reviews.csv with columns: review, rating, date, bank, source
"""

from google_play_scraper import reviews, reviews_all, Sort
import pandas as pd
import time
import os

# Ensure data folder exists
os.makedirs('data', exist_ok=True)

# Bank app IDs (updated to the Play Store app IDs discovered via search)
banks = {
    "CBE": "com.combanketh.mobilebanking",
    "BOA": "com.boa.boaMobileBanking",
    "Dashen": "com.cr2.amolelight"
}

all_reviews = []

# Scrape reviews per bank (request many, then dedupe)
for bank_name, app_id in banks.items():
    print(f"Scraping {bank_name} reviews...")

    try:
        # Try the paginated `reviews` first (requests up to `count` items)
        result, _ = reviews(
            app_id,
            lang='en',        # English reviews
            country='US',     # Use US to bypass geo-blocks
            sort=Sort.NEWEST, # Get recent reviews
            count=450         # Request more to ensure 400+ unique
        )

        # If `reviews` returned nothing, fall back to `reviews_all` which
        # iterates through continuations and tends to be more reliable.
        if not result:
            print("  → 'reviews' returned 0 reviews; trying 'reviews_all' fallback...")
            result = reviews_all(
                app_id,
                lang='en',
                country='US',
                sort=Sort.NEWEST
            )

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
                'source': 'Google Play (US)'
            })
            processed += 1

        print(f"  → Got {processed} reviews")

    except Exception as e:
        print(f"  → ERROR for {bank_name}: {str(e)}")

    # Be respectful to Google's servers
    time.sleep(2)

# Convert to DataFrame
df = pd.DataFrame(all_reviews)

# Remove duplicates (same review text)
if not df.empty:
    df = df.drop_duplicates(subset=['review'])

# Save to CSV
df.to_csv('data/reviews.csv', index=False)
print(f"\n✅ Total clean reviews: {len(df)}")
print("✅ Saved to data/reviews.csv")