"""Run sentiment analysis and thematic extraction as a script.

Saves:
 - per-review CSV: sentiment + themes (`--output`, default `data/sentiment_themes.csv`)
 - bank keywords JSON (`--keywords`, default `data/bank_keywords.json`)

Falls back to VADER if transformers pipeline is not available.
"""
import argparse
import logging
import json
from pathlib import Path
import sys
import pandas as pd
import numpy as np


def parse_args(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument('--input', '-i', default='data/clean_reviews.csv')
    p.add_argument('--output', '-o', default='data/sentiment_themes.csv')
    p.add_argument('--keywords', default='data/bank_keywords.json')
    p.add_argument('--min-per-bank', type=int, default=0)
    p.add_argument('--log-level', default='INFO')
    return p.parse_args(argv)


def top_n_keywords(texts, n=30, ngram_range=(1,2)):
    from sklearn.feature_extraction.text import TfidfVectorizer
    vec = TfidfVectorizer(stop_words='english', ngram_range=ngram_range, max_features=2000)
    X = vec.fit_transform(texts)
    sums = np.asarray(X.sum(axis=0)).ravel()
    terms = np.array(vec.get_feature_names_out())
    idx = np.argsort(sums)[::-1]
    top_terms = terms[idx][:n].tolist()
    return top_terms


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format='%(asctime)s %(levelname)s: %(message)s')

    inp = Path(args.input)
    if not inp.exists():
        logging.error('Input file not found: %s', inp)
        raise SystemExit(1)

    df = pd.read_csv(inp)
    if 'review' not in df.columns:
        logging.error('Input CSV must contain a `review` column')
        raise SystemExit(1)

    df['review'] = df['review'].astype(str)

    # Load sentiment model (transformer) if available
    use_transformer = False
    sentiment_model = None
    try:
        from transformers import pipeline
        logging.info('Loading transformer sentiment pipeline...')
        sentiment_model = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')
        use_transformer = True
    except Exception as e:
        logging.info('Transformer not available or failed to load: %s', e)
        logging.info('Falling back to VADER')
        try:
            from nltk.sentiment import SentimentIntensityAnalyzer
            import nltk
            nltk.download('vader_lexicon')
            vader = SentimentIntensityAnalyzer()
        except Exception as ex:
            logging.error('VADER failed to load: %s', ex)
            raise

    def sentiment_transformer(text):
        try:
            out = sentiment_model(text[:512])[0]
            label = out.get('label')
            score = float(out.get('score', 0.0))
            if label.upper() == 'NEGATIVE':
                return 'negative', -score
            else:
                return 'positive', score
        except Exception:
            return None

    def sentiment_vader(text):
        s = vader.polarity_scores(text)
        compound = s['compound']
        if compound >= 0.05:
            return 'positive', float(compound)
        elif compound <= -0.05:
            return 'negative', float(compound)
        else:
            return 'neutral', float(compound)

    labels = []
    scores = []
    for text in df['review'].fillna(''):
        if use_transformer:
            out = sentiment_transformer(text)
            if out is None:
                out = sentiment_vader(text)
        else:
            out = sentiment_vader(text)
        labels.append(out[0])
        scores.append(out[1])

    df['sentiment_label'] = labels
    df['sentiment_score'] = scores

    # Keywords per bank
    bank_keywords = {}
    for bank, group in df.groupby('bank'):
        texts = group['review'].astype(str).tolist()
        bank_keywords[bank] = top_n_keywords(texts, n=30)

    # Simple theme mapping (rule-based)
    theme_map = {
        'login': 'Account Access Issues',
        'password': 'Account Access Issues',
        'slow': 'Performance & Reliability',
        'crash': 'Performance & Reliability',
        'transfer': 'Transaction Performance',
        'payment': 'Transaction Performance',
        'ui': 'User Interface & Experience',
        'interface': 'User Interface & Experience',
        'support': 'Customer Support',
        'help': 'Customer Support',
        'feature': 'Feature Requests',
        'update': 'Maintenance / Updates',
    }

    def detect_themes(text):
        t = str(text).lower()
        themes = set()
        for kw, theme in theme_map.items():
            if kw in t:
                themes.add(theme)
        if not themes:
            return ['Other']
        return sorted(list(themes))

    df['themes'] = df['review'].apply(detect_themes)

    # Aggregations
    agg_by_bank = df.groupby('bank').agg(
        reviews=('review', 'size'),
        mean_sentiment_score=('sentiment_score', 'mean'),
        positive_pct=('sentiment_label', lambda x: (x=='positive').sum()/len(x)*100)
    )

    logging.info('Aggregated counts per bank:\n%s', agg_by_bank)

    # Save outputs
    outp = Path(args.output)
    df.to_csv(outp, index=False)
    logging.info('Saved per-review output to %s', outp)

    with open(args.keywords, 'w', encoding='utf-8') as fh:
        json.dump(bank_keywords, fh, indent=2, ensure_ascii=False)
    logging.info('Saved bank keywords to %s', args.keywords)

    # Optional: assert min per bank
    if args.min_per_bank and args.min_per_bank > 0:
        final_counts = df['bank'].value_counts().to_dict()
        failing = [b for b, cnt in final_counts.items() if cnt < args.min_per_bank]
        if failing:
            raise AssertionError(f'Banks with fewer than {args.min_per_bank} reviews: {failing}')


if __name__ == '__main__':
    main(sys.argv[1:])
