print("=== USAGE PATTERNS MODULE LOADED ===")
import pandas as pd
from datetime import datetime
from services.sheets_service import SheetsService
from config.settings import SHEET_CLEAN_DATA, SHEET_USAGE_PATTERNS

def analyze_usage_patterns():
    """Analyze usage patterns by trainer model"""
    print("Analyzing usage patterns...")
    
    sheets = SheetsService()
    
    # Read clean data
    df = sheets.read_to_dataframe(SHEET_CLEAN_DATA)
    
    if df.empty:
        print("Error: No data found in clean data sheet")
        return None
    
    # Convert distance to numeric
    if 'Distance in Trainers (km)' in df.columns:
        df['Distance in Trainers (km)'] = pd.to_numeric(df['Distance in Trainers (km)'], errors='coerce')
    
    # Helper function for most common value
    def most_common(series):
        return series.mode().iloc[0] if not series.mode().empty else None
    
    # Aggregate usage patterns
    usage_patterns = (
        df.groupby("Trainer Model")
        .agg(
            Most_Common_RunType=("Run Type", most_common),
            Most_Common_Terrain=("Terrain", most_common),
            Avg_Distance=("Distance in Trainers (km)", "mean"),
            Respondents=("Name", "count")
        )
        .reset_index()
        .sort_values(by="Respondents", ascending=False)
    )
    
    # Add timestamp
    usage_patterns["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Replace NaN values for JSON compatibility
    usage_patterns = usage_patterns.fillna('')
    
    # Write to Usage Patterns sheet
    sheets.write_dataframe(SHEET_USAGE_PATTERNS, usage_patterns)
    
    print("✅ Usage patterns analysis complete")
    print("\n📊 Usage Patterns:")
    print(usage_patterns.head())
    
    return usage_patterns

if __name__ == "__main__":
    analyze_usage_patterns()