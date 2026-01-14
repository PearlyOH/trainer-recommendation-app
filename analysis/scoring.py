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
    
    # Check if required columns exist
    if 'Name' not in df.columns or 'Score' not in df.columns:
        print("Error: 'Name' or 'Score' column not found")
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
    
    # Select columns to write
    output_df = df[["Name", "Score", "Segmentation Tier", "Timestamp"]]
    
    # Write to Quant Analysis sheet
    sheets.write_dataframe(SHEET_QUANT_ANALYSIS, output_df, clear_first=False)
    
    print("[OK] Score tier assignment complete")
    return output_df

if __name__ == "__main__":
    assign_score_tiers()