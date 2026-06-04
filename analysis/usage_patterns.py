print("=== USAGE PATTERNS MODULE LOADED ===")
import pandas as pd
from datetime import datetime
from services.sheets_service import SheetsService
from config.settings import SHEET_CLEAN_DATA, SHEET_USAGE_PATTERNS

TRAINER_MODEL_COL_ALT = "What's the brand and model of this trainer? e.g. Nike Pegasus 40 or Adidas Adizero Pro 4"
NAME_COL_ALT = "First things first, what shall we call you?"

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


def _resolve_column(df, preferred_name, *keywords):
    """Return column name if preferred exists, else first column whose name contains all keywords (lowercase)."""
    if preferred_name in df.columns:
        return preferred_name
    key = [k.lower() for k in keywords]
    for c in df.columns:
        if not c:
            continue
        cl = c.lower()
        if all(k in cl for k in key):
            return c
    return None


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
    if 'Total Distance' in df.columns:
        df['Total Distance'] = pd.to_numeric(df['Total Distance'], errors='coerce')
    
    # Helper function for most common value
    def most_common(series):
        return series.mode().iloc[0] if not series.mode().empty else None
    
    # Resolve trainer model column (cleaning may rename to "Trainer Model" or leave Tally name)
    trainer_col = _trainer_model_column(df)
    if trainer_col is None:
        print("Error: 'Trainer Model' column not found. Actual columns:", list(df.columns))
        return None

    # Resolve other columns (Clean Live Data may have long Tally headers)
    run_type_col = _resolve_column(df, "Run Type", "run", "type")
    terrain_col = _resolve_column(df, "Terrain", "terrain")
    distance_col = _resolve_column(df, "Total Distance", "distance", "trainer") or _resolve_column(df, "Total Distance", "distance")
    # Name: prefer Clean Live Data header, else "Name", else column containing "call you" / "what shall we call"
    name_col = NAME_COL_ALT if NAME_COL_ALT in df.columns else _resolve_column(df, "Name", "name")
    if not name_col:
        for c in df.columns:
            if c and ("call you" in c.lower() or c.lower().startswith("name") or "what shall we call" in c.lower()):
                name_col = c
                break
    missing = []
    if not run_type_col:
        missing.append("Run Type")
    if not terrain_col:
        missing.append("Terrain")
    if not distance_col:
        missing.append("Total Distance")
    if not name_col:
        missing.append("Name")
    if missing:
        print("Error: Required column(s) not found:", missing)
        print("Actual columns:", list(df.columns))
        return None

    usage_patterns = (
        df.groupby(trainer_col)
        .agg(
            Most_Common_RunType=(run_type_col, most_common),
            Most_Common_Terrain=(terrain_col, most_common),
            Avg_Distance=(distance_col, "mean"),
            Respondents=(name_col, "count")
        )
        .reset_index()
        .rename(columns={trainer_col: "Trainer Model"})
        .sort_values(by="Respondents", ascending=False)
    )
    
    # Add timestamp
    usage_patterns["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Replace NaN values for JSON compatibility
    usage_patterns = usage_patterns.fillna('')
    
    # Write to Usage Patterns sheet
    sheets.write_dataframe(SHEET_USAGE_PATTERNS, usage_patterns)
    
    print("[OK] Usage patterns analysis complete")
    print("\n[PATTERNS] Usage Patterns:")
    print(usage_patterns.head())
    
    return usage_patterns

if __name__ == "__main__":
    analyze_usage_patterns()