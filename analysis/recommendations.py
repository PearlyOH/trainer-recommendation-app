print("=== RECOMMENDATIONS MODULE LOADED ===")
import pandas as pd
from services.sheets_service import SheetsService
from config.settings import SHEET_CLEAN_DATA

def parse_5k_time(time_str):
    """Convert 'sub 20', 'sub 25' etc to numeric value"""
    if pd.isna(time_str) or time_str == '':
        return None
    
    time_str = str(time_str).lower().strip()
    
    # Extract number from formats like "sub 20", "sub20", "20"
    if 'sub' in time_str:
        time_str = time_str.replace('sub', '').strip()
    
    try:
        return float(time_str)
    except:
        return None

def get_recommendations(five_k_time, run_type, terrain, gender):
    """
    Get trainer recommendations based on user inputs
    
    Parameters:
    - five_k_time: int (minutes, e.g., 25 for "sub 25")
    - run_type: str (e.g., "Easy run", "Long run", "Tempo", "Race")
    - terrain: str (e.g., "Road", "Trail", "Track", "Mixed")
    - gender: str (e.g., "Male", "Female", "Non-binary")
    
    Returns:
    - DataFrame with recommended trainers
    """
    print(f"Finding trainers for: {gender}, sub {five_k_time} 5k, {run_type} on {terrain}")
    
    sheets = SheetsService()
    df = sheets.read_to_dataframe(SHEET_CLEAN_DATA)
    
    # Convert Score to numeric
    df['Score'] = pd.to_numeric(df['Score'], errors='coerce')
    
    # Parse 5k times
    df['5k_Time_Numeric'] = df['Average 5k Time'].apply(parse_5k_time)
    
    print(f"\nTotal records in database: {len(df)}")
    
    # Filter by run type
    if run_type:
        df = df[df['Run Type'].str.lower().str.contains(run_type.lower(), na=False)]
        print(f"After filtering by run type '{run_type}': {len(df)} records")
    
    # Filter by terrain
    if terrain:
        df = df[df['Terrain'].str.lower().str.contains(terrain.lower(), na=False)]
        print(f"After filtering by terrain '{terrain}': {len(df)} records")
    
    # Filter by gender (optional)
    if gender and gender.lower() not in ["prefer not to say", "non-binary"]:
        df = df[df['What gender do you identify with?'].str.lower().str.contains(gender.lower(), na=False)]
        print(f"After filtering by gender '{gender}': {len(df)} records")
    
    # Filter by exact 5k time bracket match
    df = df[df['5k_Time_Numeric'] == five_k_time]
    print(f"After filtering by 5k time (sub {five_k_time}): {len(df)} records")
    
    if df.empty:
        print("\n❌ No trainers found matching all criteria")
        print("Try broadening your search criteria")
        return None
    
    # Group by trainer and calculate stats
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

# Test the function
if __name__ == "__main__":
    # Example: Female runner, sub 25 5k, wants to do long runs on road
    result = get_recommendations(
        five_k_time=25,
        run_type="Long run",
        terrain="Road",
        gender="Female"
    )