print("=== RECOMMENDATIONS MODULE LOADED ===")
import pandas as pd
from services.sheets_service import SheetsService
from config.settings import SHEET_CLEAN_DATA


def parse_5k_time(time_str):
    """Convert 'sub 20', 'sub 25', '40 minutes', '40 mins' etc to numeric value"""
    import re
    if pd.isna(time_str) or time_str == '':
        return None
    
    time_str = str(time_str).lower().strip()
    
    # Remove "sub" so "sub 40" or "sub40" leave "40"
    if 'sub' in time_str:
        time_str = time_str.replace('sub', '').strip()
    
    # Extract first number (handles "40", "40 minutes", "40 mins", "40-45" -> 40)
    match = re.search(r'(\d+)', time_str)
    if match:
        try:
            return float(match.group(1))
        except (ValueError, TypeError):
            return None
    try:
        return float(time_str)
    except (ValueError, TypeError):
        return None


def get_recommendations(five_k_time, run_type, terrain, gender, foot_width=None):
    """
    Get trainer recommendations based on user inputs
    
    Parameters:
    - five_k_time: int (minutes, e.g., 25 for "sub 25")
    - run_type: str (e.g., "Easy run", "Long run", "Tempo", "Race")
    - terrain: str (e.g., "Road", "Trail", "Track", "Mixed")
    - gender: str (e.g., "Male", "Female", "Non-binary")
    - foot_width: str (e.g., "Narrow", "Regular", "Wide")
    
    Returns:
    - DataFrame with recommended trainers, or None if no matches
    """
    print(f"\n{'='*50}")
    print("STRIDELY RECOMMENDATION ENGINE")
    print(f"{'='*50}")
    print(f"Finding trainers for: {gender}, sub {five_k_time} 5k, {run_type} on {terrain}, foot width: {foot_width}")
    
    sheets = SheetsService()
    df = sheets.read_to_dataframe(SHEET_CLEAN_DATA)
    
    if df.empty:
        print("ERROR: No data in Clean Live Data sheet")
        return None
    
    # Convert Score to numeric
    if 'Score' not in df.columns:
        print("ERROR: 'Score' column not found")
        return None
    df['Score'] = pd.to_numeric(df['Score'], errors='coerce')
    
    # Parse 5k times
    if 'Average 5k Time' not in df.columns:
        print("ERROR: 'Average 5k Time' column not found")
        return None
    df['5k_Time_Numeric'] = df['Average 5k Time'].apply(parse_5k_time)
    
    print(f"\nTotal records in database: {len(df)}")
    
    # ===========================================
    # FILTERING
    # ===========================================
    
    # Filter by run type
    if run_type:
        if 'Run Type' not in df.columns:
            print("WARNING: 'Run Type' column not found, skipping filter")
        else:
            df = df[df['Run Type'].str.lower().str.contains(run_type.lower(), na=False)]
            print(f"After filtering by run type '{run_type}': {len(df)} records")
    
    # Filter by terrain
    if terrain:
        if 'Terrain' not in df.columns:
            print("WARNING: 'Terrain' column not found, skipping filter")
        else:
            df = df[df['Terrain'].str.lower().str.contains(terrain.lower(), na=False)]
            print(f"After filtering by terrain '{terrain}': {len(df)} records")
    
    # Filter by gender (optional)
    if gender and gender.lower() not in ["prefer not to say", "non-binary", ""]:
        if 'Gender' not in df.columns:
            print("WARNING: 'Gender' column not found, skipping filter")
        else:
            df = df[df['Gender'].str.lower().str.contains(gender.lower(), na=False)]
            print(f"After filtering by gender '{gender}': {len(df)} records")
    
    # Resolve Foot Width column (sheet header may differ, e.g. trailing newline or full Tally question)
    foot_width_col = None
    if 'Foot Width' in df.columns:
        foot_width_col = 'Foot Width'
    else:
        for col in df.columns:
            c = str(col).lower().strip()
            if 'foot' in c and 'width' in c:
                foot_width_col = col
                break
    
    # Filter by foot width (optional)
    if foot_width and foot_width.lower() not in ["", "any"]:
        if foot_width_col is None:
            print("WARNING: 'Foot Width' column not found, skipping filter")
        else:
            # Compare as strings, normalise whitespace and handle NaN
            fw_series = df[foot_width_col].astype(str).str.lower().str.strip()
            df = df[fw_series == foot_width.lower().strip()]
            print(f"After filtering by foot width '{foot_width}': {len(df)} records")
    
    # Filter by exact 5k time bracket match
    if five_k_time:
        df = df[df['5k_Time_Numeric'] == five_k_time]
        print(f"After filtering by 5k time (sub {five_k_time}): {len(df)} records")
    
    # ===========================================
    # CHECK RESULTS
    # ===========================================
    
    if df.empty:
        print("\n⚠ No trainers found matching all criteria")
        print("Suggestions:")
        print("  - Try a different 5k time bracket")
        print("  - Try 'Mixed' terrain")
        print("  - Remove gender filter")
        print("  - Try 'Regular' foot width")
        return None
    
    if 'Trainer Model' not in df.columns:
        print("ERROR: 'Trainer Model' column not found")
        return None
    
    # ===========================================
    # AGGREGATE RESULTS
    # ===========================================
    
    recommendations = (
        df.groupby('Trainer Model')
        .agg(
            Avg_Score=('Score', 'mean'),
            Num_Reviews=('Score', 'count'),
            Avg_5k_Time=('5k_Time_Numeric', 'mean')
        )
        .reset_index()
        .sort_values(by=['Avg_Score', 'Num_Reviews'], ascending=[False, False])
    )
    
    # Round for display
    recommendations['Avg_Score'] = recommendations['Avg_Score'].round(1)
    recommendations['Avg_5k_Time'] = recommendations['Avg_5k_Time'].round(0)
    
    print(f"\n✅ Found {len(recommendations)} matching trainers:")
    print(recommendations.to_string(index=False))
    
    return recommendations


# ===========================================
# TEST
# ===========================================
if __name__ == "__main__":
    # Example: Female runner, sub 25 5k, long runs on road, regular foot width
    result = get_recommendations(
        five_k_time=25,
        run_type="Long run",
        terrain="Road",
        gender="Female",
        foot_width="Regular"
    )