print("=== SCORING MODULE LOADED ===")
import pandas as pd
from datetime import datetime
from services.sheets_service import SheetsService
from config.settings import SHEET_CLEAN_DATA, SHEET_QUANT_ANALYSIS

def assign_score_tiers():
    """Assign segmentation tiers based on scores"""
    print("Starting score tier assignment...")
    
    sheets = SheetsService()
    
    # Read clean data
    df = sheets.read_to_dataframe(SHEET_CLEAN_DATA)
    
    # Resolve name column (cleaning may leave Tally question as header; sheet may add newline/spaces)
    name_col = None
    if "Name" in df.columns:
        name_col = "Name"
    else:
        for col in df.columns:
            if "what shall we call you" in str(col).lower().strip():
                name_col = col
                break
    if name_col is None:
        print("Error: Name column not found (expected 'Name' or a column containing 'what shall we call you')")
        return None
    if "Score" not in df.columns:
        print("Error: 'Score' column not found")
        return None

    # Convert Score to numeric
    df['Score'] = pd.to_numeric(df['Score'], errors='coerce')
    
    # Drop rows with missing scores
    df.dropna(subset=['Score'], inplace=True)
    
    # Assign tier
    def assign_tier(score):
        if score <= 4:
            return "Low"
        elif 5 <= score <= 9:
            return "Medium"
        else:
            return "High"
    
    df['Segmentation Tier'] = df['Score'].apply(assign_tier)
    df['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Select columns to write (use name_col for reading, output as "Name")
    output_df = df[[name_col, "Score", "Segmentation Tier", "Timestamp"]].copy()
    output_df.rename(columns={name_col: "Name"}, inplace=True)
    
    # Write to Quant Analysis sheet
    sheets.write_dataframe(SHEET_QUANT_ANALYSIS, output_df, clear_first=False)
    
    print("[OK] Score tier assignment complete")
    return output_df

if __name__ == "__main__":
    assign_score_tiers()