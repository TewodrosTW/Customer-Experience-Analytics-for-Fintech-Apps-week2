# Week 2: Customer Experience Analytics for Fintech Apps

Analyzing Google Play reviews for CBE, BOA, and Dashen Bank.

## Setup
1. Clone this repo
2. Create virtual environment: `python -m venv venv`
3. Activate:
   - Windows: `venv\Scripts\activate`
   - Mac/Linux: `source venv/bin/activate`
4. Install: `pip install -r requirements.txt`

## Branches
- `task-1`: Scraping & Preprocessing
- `task-2`: Sentiment & Thematic Analysis
- `task-3`: PostgreSQL Database
- `task-4`: Insights & Report

## Data
Raw reviews saved to `data/reviews.csv` (gitignored).

By: Tewodros Tsegay Wendem



## Task 1: Data Collection & Preprocessing
### Methodology
- **Tool**: `google-play-scraper`
- **Banks Scraped (default app IDs)**:
  - CBE: `com.combanketh.mobilebanking`
  - BOA: `com.boa.boaMobileBanking`
  - Dashen: `com.cr2.amolelight`
- **Goal**: collect >=400 cleaned reviews per bank (1,200+ total). If an app has fewer unique reviews available, the script reports which banks fell short.
- **How to run**:
  - Full scrape (default, asserts min 400 per bank):
    ```powershell
    python scripts/scrape_reviews.py
    ```
  - Override parameters (example):
    ```powershell
    python scripts/scrape_reviews.py --count 1000 --lang en --country us --output data/reviews_final.csv
    ```
  - Provide custom banks mapping via JSON file:
    ```powershell
    python scripts/scrape_reviews.py --banks-file banks.json
    ```
- **Postprocessing**:
  - Run the preprocessing script to clean and normalize the dataset:
    ```powershell
    python scripts/preprocess_reviews.py --input data/reviews_final.csv --output data/clean_reviews.csv
    ```
- **Checks & Outputs**:
  - The scraper logs raw and final counts per bank, and uses a `--min-per-bank` assertion (default 400) to enforce the per-bank minimum.
  - The preprocessing script normalizes dates to `YYYY-MM-DD`, removes empty reviews, deduplicates by review text, and reports missing-value percentages.
  - Example results from a full run (with higher counts requested):
    - Total raw collected: 2,502
    - After dedupe: 1,958
    - Final per-bank cleaned counts (example): CBE=772, BOA=818, Dashen=368
    - Note: Dashen in this run had fewer than 400 unique cleaned reviews; this indicates the Play Store app has fewer unique reviews available for that app ID. Options: aggregate multiple app IDs for Dashen, relax per-bank minimum, or include additional banks.

**Output files**: `data/reviews_final.csv` (raw normalized output from scraper), `data/clean_reviews.csv` (postprocessed clean dataset)