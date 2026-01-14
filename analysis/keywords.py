import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string
from collections import Counter
from services.sheets_service import SheetsService
from config.settings import SHEET_CLEAN_DATA, SHEET_KEYWORD_FREQ

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')
    
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

def analyze_keywords():
    """Analyze keyword frequency from qualitative feedback"""
    print("Analyzing keyword frequency...")
    
    sheets = SheetsService()
    
    # Read clean data
    df = sheets.read_to_dataframe(SHEET_CLEAN_DATA)
    
    # Text columns to analyze
    text_cols = ["Post Run Feel", "Pain Experienced", "Improvement Suggestions"]
    
    # Check which columns exist
    existing_text_cols = [col for col in text_cols if col in df.columns]
    
    if not existing_text_cols:
        print(f"Warning: None of the text columns {text_cols} found in data")
        return None
    
    print(f"Found text columns: {existing_text_cols}")
    
    # Combine all text
    all_text = " ".join(
        df[col].astype(str).fillna('').str.cat(sep=' ') 
        for col in existing_text_cols
    )
    
    if len(all_text.strip()) == 0:
        print("Warning: No text content found to analyze")
        return None
    
    # Tokenize and clean
    tokens = word_tokenize(all_text.lower())
    tokens = [
        word for word in tokens
        if word.isalpha() 
        and word not in stopwords.words("english") 
        and word not in string.punctuation
        and len(word) > 2  # Filter out very short words
    ]
    
    # Count keywords
    word_counts = Counter(tokens)
    keyword_df = pd.DataFrame(word_counts.most_common(20), columns=["Keyword", "Frequency"])
    
    if keyword_df.empty:
        print("Warning: No keywords found")
        return None
    
    # Write to Keyword Frequency sheet
    sheets.write_dataframe(SHEET_KEYWORD_FREQ, keyword_df)
    
    print("[OK] Keyword frequency analysis complete")
    print("\n[KEYWORDS] Top 20 Keywords:")
    print(keyword_df)
    
    return keyword_df

if __name__ == "__main__":
    analyze_keywords()