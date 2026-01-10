print("=== LEADERBOARD MODULE LOADED ===")
import pandas as pd
from datetime import datetime
from services.sheets_service import SheetsService
from config.settings import SHEET_CLEAN_DATA, SHEET_LEADERBOARD

def create_leaderboard():
    """Create top 5 trainers leaderboard"""
    print("Creating leaderboard...")
    
    sheets = SheetsService()
    
    # Read clean data
    df = sheets.read_to_dataframe(SHEET_CLEAN_DATA)
    
    # Convert Score to numeric
    df['Score'] = pd.to_numeric(df['Score'], errors='coerce')
    
    # Create leaderboard
    leaderboard = (
        df.groupby("Trainer Model")
        .agg(
            Avg_Score=("Score", "mean"),
            Respondents=("Score", "count")
        )
        .reset_index()
        .sort_values(by="Avg_Score", ascending=False)
        .head(5)
    )
    
    # Add timestamp
    leaderboard['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Write to Leaderboard sheet
    sheets.write_dataframe(SHEET_LEADERBOARD, leaderboard)
    
    print("✅ Leaderboard created")
    print("\n🏆 Top 5 Trainers:")
    print(leaderboard)
    
    return leaderboard

if __name__ == "__main__":
    create_leaderboard()