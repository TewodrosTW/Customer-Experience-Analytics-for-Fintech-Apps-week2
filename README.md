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
- **Tool**: `google-play-scraper` (v1.2.3)
- **Banks Scraped**: 
  - CBE (`com.cbe.mobile`)
  - BOA (`com.boa.mobilebanking`)
  - Dashen (`com.dashen.bank.mobile`)
- **Reviews Collected**: 1,074 (450 CBE, 450 BOA, 450 Dashen)
- **Preprocessing**:
  - Removed duplicates
  - Normalized dates to `YYYY-MM-DD`
  - Handled missing values (<1%)
- **Output**: `data/reviews.csv` (columns: `review`, `rating`, `date`, `bank`, `source`)