import pandas as pd
from textblob import TextBlob
from datetime import datetime
from services.sheets_service import SheetsService
from config.settings import SHEET_CLEAN_DATA, SHEET_SENTIMENT

TRAINER_MODEL_COL_ALT = "What's the brand and model of this trainer? e.g. Nike Pegasus 40 or Adidas Adizero Pro 4"


def _resolve_column(df, preferred_name, *keywords):
    if preferred_name in df.columns:
        return preferred_name
    key = [k.lower() for k in keywords]
    for col in df.columns:
        c = str(col).lower()
        if all(k in c for k in key):
            return col
    return None


def analyze_sentiment():
    """Analyze sentiment from qualitative feedback"""
    print("Analyzing sentiment...")
    
    sheets = SheetsService()
    
    # Read clean data
    df = sheets.read_to_dataframe(SHEET_CLEAN_DATA)
    
    # Text columns to analyze (resolve long/raw headers when clean names are absent)
    text_cols = [
        _resolve_column(df, "Post Run Feel", "typical", "run", "feel"),
        _resolve_column(df, "Pain Experienced", "pain", "discomfort"),
        _resolve_column(df, "Improvement Suggestions", "magic", "wand"),
    ]
    text_cols = [c for c in text_cols if c is not None]
    
    # Sentiment analysis function
    def get_sentiment(text):
        blob = TextBlob(str(text))
        return blob.sentiment.polarity, blob.sentiment.subjectivity
    
    # Apply sentiment analysis to each text column
    for col in text_cols:
        if col in df.columns:
            df[f"{col} Polarity"] = df[col].apply(lambda x: get_sentiment(x)[0])
            df[f"{col} Subjectivity"] = df[col].apply(lambda x: get_sentiment(x)[1])
        else:
            print(f"Warning: Column '{col}' not found")
            df[f"{col} Polarity"] = None
            df[f"{col} Subjectivity"] = None
    
    # Calculate overall sentiment
    sentiment_polarity_cols = [f"{c} Polarity" for c in text_cols if f"{c} Polarity" in df.columns]
    sentiment_subjectivity_cols = [f"{c} Subjectivity" for c in text_cols if f"{c} Subjectivity" in df.columns]
    
    if sentiment_polarity_cols:
        df["Overall Polarity"] = df[sentiment_polarity_cols].mean(axis=1)
    else:
        df["Overall Polarity"] = None
    
    if sentiment_subjectivity_cols:
        df["Overall Subjectivity"] = df[sentiment_subjectivity_cols].mean(axis=1)
    else:
        df["Overall Subjectivity"] = None
    
    # Aggregate by trainer model
    trainer_col = "Trainer Model" if "Trainer Model" in df.columns else (
        TRAINER_MODEL_COL_ALT if TRAINER_MODEL_COL_ALT in df.columns else _resolve_column(df, "Trainer Model", "brand", "model")
    )

    if 'Overall Polarity' in df.columns and 'Overall Subjectivity' in df.columns and trainer_col:
        sentiment_summary = (
            df[[trainer_col, "Overall Polarity", "Overall Subjectivity"]]
            .groupby(trainer_col)
            .mean()
            .reset_index()
            .rename(columns={trainer_col: "Trainer Model"})
        )
        
        # Write to Sentiment Analysis sheet
        sheets.write_dataframe(SHEET_SENTIMENT, sentiment_summary)
        
        print("[OK] Sentiment analysis complete")
        print("\n[SENTIMENT] Sentiment by Trainer:")
        print(sentiment_summary.head())
        
        return sentiment_summary
    else:
        print("Error: Required columns not found for sentiment aggregation")
        return None

if __name__ == "__main__":
    analyze_sentiment()