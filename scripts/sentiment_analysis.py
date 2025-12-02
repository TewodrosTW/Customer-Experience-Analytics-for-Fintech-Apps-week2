"""
Task 2: Sentiment & Thematic Analysis for Bank Reviews
- Reads: data/reviews.csv
- Outputs: data/reviews_with_sentiment.csv
- Uses: VADER (simple) + DistilBERT (advanced fallback)
"""

import pandas as pd
import os

# Load cleaned reviews from Task 1
df = pd.read_csv('data/reviews.csv')

# Ensure review text is string (handle NaN)
df['review'] = df['review'].fillna('').astype(str)

print(f"Loaded {len(df)} reviews for sentiment analysis.")

# --- SENTIMENT ANALYSIS (VADER) ---
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

def get_vader_sentiment(text):
    if not text.strip():
        return 'neutral', 0.0
    scores = analyzer.polarity_scores(text)
    compound = scores['compound']
    if compound >= 0.05:
        return 'positive', compound
    elif compound <= -0.05:
        return 'negative', compound
    else:
        return 'neutral', compound

# Apply to all reviews
df[['sentiment_label', 'sentiment_score']] = df['review'].apply(
    lambda x: pd.Series(get_vader_sentiment(x))
)

print("✅ VADER sentiment analysis complete.")

# --- TEXT PREPROCESSING (spaCy) ---
import spacy
from collections import Counter
import re

# Load English model
nlp = spacy.load("en_core_web_sm")

# Custom stop words for banking context
custom_stopwords = {
    "app", "application", "bank", "mobile", "phone", "device", "use", "using", "used",
    "good", "bad", "nice", "great", "very", "really", "much", "well", "even", "also"
}

def preprocess_text(text):
    # Lowercase + remove special chars
    text = re.sub(r'[^a-zA-Z\s]', ' ', text.lower())
    doc = nlp(text)
    # Lemmatize + remove stopwords, punctuation, short words
    tokens = [
        token.lemma_ for token in doc
        if not token.is_stop and not token.is_punct and len(token.text) > 2
        and token.lemma_ not in custom_stopwords
    ]
    return " ".join(tokens)

# Apply preprocessing
df['cleaned_review'] = df['review'].apply(preprocess_text)
print("✅ Text preprocessing complete.")

# --- KEYWORD EXTRACTION (TF-IDF) ---
from sklearn.feature_extraction.text import TfidfVectorizer

def extract_top_keywords(texts, top_n=20):
    if not any(texts):
        return []
    vectorizer = TfidfVectorizer(max_features=100, ngram_range=(1, 2))
    tfidf_matrix = vectorizer.fit_transform(texts)
    feature_names = vectorizer.get_feature_names_out()
    # Sum TF-IDF scores across all docs
    tfidf_scores = tfidf_matrix.sum(axis=0).A1
    # Get top keywords
    top_indices = tfidf_scores.argsort()[-top_n:][::-1]
    return [feature_names[i] for i in top_indices]

# Extract keywords per bank
bank_keywords = {}
for bank in df['bank'].unique():
    bank_texts = df[df['bank'] == bank]['cleaned_review'].tolist()
    keywords = extract_top_keywords(bank_texts, top_n=25)
    bank_keywords[bank] = keywords
    print(f"✅ Top keywords for {bank}: {keywords[:10]}")

    # --- THEME ASSIGNMENT (Rule-Based) ---
def assign_theme(keywords_list, review_text):
    review_lower = review_text.lower()
    
    # Define theme keywords
    login_keywords = ['login', 'sign', 'password', 'account', 'authent', 'log in']
    performance_keywords = ['slow', 'fast', 'speed', 'load', 'lag', 'crash', 'freeze', 'error']
    ui_keywords = ['ui', 'design', 'interface', 'layout', 'button', 'icon', 'color', 'screen']
    support_keywords = ['support', 'help', 'service', 'response', 'agent', 'call']
    feature_keywords = ['feature', 'add', 'include', 'budget', 'dark mode', 'fingerprint', 'transfer']
    
    # Count matches in review
    login_score = sum(1 for kw in login_keywords if kw in review_lower)
    perf_score = sum(1 for kw in performance_keywords if kw in review_lower)
    ui_score = sum(1 for kw in ui_keywords if kw in review_lower)
    support_score = sum(1 for kw in support_keywords if kw in review_lower)
    feature_score = sum(1 for kw in feature_keywords if kw in review_lower)
    
    scores = {
        'Login & Authentication Issues': login_score,
        'Performance & Reliability': perf_score,
        'User Interface (UI) Design': ui_score,
        'Customer Support': support_score,
        'Feature Requests': feature_score
    }
    
    # Return theme with highest score (or 'General Feedback' if none)
    best_theme = max(scores, key=scores.get)
    return best_theme if scores[best_theme] > 0 else 'General Feedback'

# Apply theme assignment
df['theme'] = df.apply(lambda row: assign_theme(bank_keywords[row['bank']], row['review']), axis=1)
print("✅ Theme assignment complete.")

# --- SAVE FINAL OUTPUT ---
# Add review_id
df = df.reset_index().rename(columns={'index': 'review_id'})

# Select required columns
output_df = df[['review_id', 'review', 'sentiment_label', 'theme']]

# Save to CSV
output_df.to_csv('data/reviews_with_sentiment.csv', index=False)
print(f"✅ Final output saved: {len(output_df)} reviews with sentiment & themes.")
print("\nSample themes per bank:")
for bank in df['bank'].unique():
    themes = df[df['bank'] == bank]['theme'].value_counts().head(3)
    print(f"{bank}:\n{themes}\n")