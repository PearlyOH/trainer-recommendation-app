print("=== LEADERBOARD MODULE LOADED ===")
import pandas as pd
from datetime import datetime
from services.sheets_service import SheetsService
from config.settings import SHEET_CLEAN_DATA, SHEET_LEADERBOARD

TRAINER_MODEL_COL_ALT = "What's the brand and model of this trainer? e.g. Nike Pegasus 40 or Adidas Adizero Pro 4"

def _trainer_model_column(df):
    """Resolve trainer model column. Prefer long Tally name (Column F with data); Column G may be empty 'Trainer Model' from old question."""
    if TRAINER_MODEL_COL_ALT in df.columns:
        return TRAINER_MODEL_COL_ALT
    if "Trainer Model" in df.columns:
        return "Trainer Model"
    for c in df.columns:
        if c and "brand" in c.lower() and "model" in c.lower():
            return c
    return None

def create_leaderboard():
    """Create top 5 trainers leaderboard"""
    print("Creating leaderboard...")
    
    sheets = SheetsService()
    
    # Read clean data
    df = sheets.read_to_dataframe(SHEET_CLEAN_DATA)
    
    # Convert Score to numeric
    if 'Score' in df.columns:
        df['Score'] = pd.to_numeric(df['Score'], errors='coerce')
    else:
        print("Error: 'Score' column not found")
        return None
    
    # Resolve trainer model column (cleaning may rename to "Trainer Model" or leave Tally name)
    trainer_col = _trainer_model_column(df)
    if trainer_col is None:
        print("Error: 'Trainer Model' column not found. Actual columns:", list(df.columns))
        return None
    
    leaderboard = (
        df.groupby(trainer_col)
        .agg(
            Avg_Score=("Score", "mean"),
            Respondents=("Score", "count")
        )
        .reset_index()
        .rename(columns={trainer_col: "Trainer Model"})
        .sort_values(by="Avg_Score", ascending=False)
        .head(5)
    )
    
    # Add timestamp
    leaderboard['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Write to Leaderboard sheet
    sheets.write_dataframe(SHEET_LEADERBOARD, leaderboard)
    
    print("[OK] Leaderboard created")
    print("\n[TOP 5] Top 5 Trainers:")
    print(leaderboard)
    
    return leaderboard

if __name__ == "__main__":
    create_leaderboard()