print("=== RECOMMENDATIONS MODULE LOADED ===")
import pandas as pd
from services.sheets_service import SheetsService
from config.settings import SHEET_CLEAN_DATA

TRAINER_MODEL_COL_ALT = "What's the brand and model of this trainer? e.g. Nike Pegasus 40 or Adidas Adizero Pro 4"
COMFORT_COL_ALT = "How would you rate the overall comfort of the trainer?"
CUSHIONING_COL_ALT = "Please rate the cushioning of the trainer"
RESPONSIVENESS_COL_ALT = "Please rate the responsiveness of the trainer (how quickly it adapts and returns energy with each step)"


def _resolve_column(df, preferred_name, *keywords):
    if preferred_name in df.columns:
        return preferred_name
    key = [k.lower() for k in keywords]
    for col in df.columns:
        c = str(col).lower()
        if all(k in c for k in key):
            return col
    return None


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


def get_recommendations(run_goal, run_type, terrain, foot_width=None, weight=None, pain=None):
    """
    Get trainer recommendations based on user inputs
    
    Parameters:
    - run_goal: str ("Beginner/Walk", "First 5k", "Comfy/Long Run", "Speed/Tempo")
    - run_type: str (e.g., "Easy run", "Long run", "Tempo", "Race")
    - terrain: str (e.g., "Road", "Trail", "Track", "Mixed")
    - foot_width: str (e.g., "Narrow", "Regular", "Wide")
    - weight: str (e.g., "Under 65kg", "Between 65kg - 85kg")
    - pain: str (e.g., "heel pain", "knee pain", "no pain")
    
    Returns:
    - DataFrame with recommended trainers, or None if no matches
    """
    print(f"\n{'='*50}")
    print("STRIDELY RECOMMENDATION ENGINE")
    print(f"{'='*50}")
    print(
        f"Finding trainers for: goal={run_goal}, {run_type} on {terrain}, "
        f"foot width: {foot_width}, weight: {weight}, pain: {pain}"
    )
    
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
    
    run_type_col = _resolve_column(df, "Run Type", "run", "type")
    if run_type_col is None:
        run_type_col = _resolve_column(df, "Type of Run", "type", "run")
    terrain_col = _resolve_column(df, "Terrain", "terrain")
    weight_col = _resolve_column(df, "Weight", "weight")
    pain_col = _resolve_column(df, "Pain Experienced", "pain", "discomfort")
    if pain_col is None:
        pain_col = _resolve_column(df, "Pain Experienced", "pain")
    if pain_col is None:
        pain_col = _resolve_column(df, "Pain Experienced", "discomfort")
    # Rating columns: prefer exact Clean Live Data headers, then fallback to short names/keyword matching
    comfort_col = COMFORT_COL_ALT if COMFORT_COL_ALT in df.columns else _resolve_column(df, "Comfort", "comfort")
    cushioning_col = CUSHIONING_COL_ALT if CUSHIONING_COL_ALT in df.columns else _resolve_column(df, "Cushioning", "cushioning")
    responsiveness_col = (
        RESPONSIVENESS_COL_ALT
        if RESPONSIVENESS_COL_ALT in df.columns
        else _resolve_column(df, "Responsiveness", "responsiveness")
    )
    five_k_col = _resolve_column(df, "Average 5k Time", "average", "5k", "time")
    if five_k_col is None:
        five_k_col = _resolve_column(df, "Average 5K Time", "average", "5k", "time")
    trainer_col = "Trainer Model" if "Trainer Model" in df.columns else (
        TRAINER_MODEL_COL_ALT if TRAINER_MODEL_COL_ALT in df.columns else _resolve_column(df, "Trainer Model", "brand", "model")
    )

    # Filter by run type
    if run_type:
        if run_type_col is None:
            print("WARNING: 'Run Type' column not found, skipping filter")
        else:
            df = df[df[run_type_col].astype(str).str.lower().str.contains(run_type.lower(), na=False)]
            print(f"After filtering by run type '{run_type}': {len(df)} records")
    
    # Filter by terrain
    if terrain:
        if terrain_col is None:
            print("WARNING: 'Terrain' column not found, skipping filter")
        else:
            df = df[df[terrain_col].astype(str).str.lower().str.contains(terrain.lower(), na=False)]
            print(f"After filtering by terrain '{terrain}': {len(df)} records")
    
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

    # Filter by weight (optional, exact-match like foot width)
    if weight and weight.lower() not in ["", "any"]:
        if weight_col is None:
            print("WARNING: 'Weight' column not found, skipping filter")
        else:
            weight_series = df[weight_col].astype(str).str.lower().str.strip()
            df = df[weight_series == weight.lower().strip()]
            print(f"After filtering by weight '{weight}': {len(df)} records")

    # Pain/discomfort filter (optional):
    # - If user says "No Pain", INCLUDE only rows that indicate no pain/discomfort.
    # - Otherwise, EXCLUDE rows where reviewer pain text contains the user's pain type.
    if pain and pain.lower() not in ["", "any"]:
        if pain_col is None:
            print("WARNING: 'Pain Experienced' column not found, skipping filter")
        else:
            pain_series = df[pain_col].astype(str).str.lower().str.strip()
            pain_query = pain.lower().strip()
            if pain_query in ["no pain", "no discomfort", "none"]:
                no_pain_mask = (
                    pain_series.str.contains("no pain", na=False)
                    | pain_series.str.contains("no discomfort", na=False)
                    | pain_series.str.fullmatch("none", na=False)
                )
                df = df[no_pain_mask]
                print(f"After including no-pain reviews '{pain}': {len(df)} records")
            else:
                df = df[~pain_series.str.contains(pain_query, na=False)]
                print(f"After excluding pain '{pain}': {len(df)} records")
    
    # ===========================================
    # CHECK RESULTS
    # ===========================================
    
    if df.empty:
        print("\n[WARN] No trainers found matching all criteria")
        print("Suggestions:")
        print("  - Try a different 5k time bracket")
        print("  - Try 'Mixed' terrain")
        print("  - Remove gender filter")
        print("  - Try 'Regular' foot width")
        return None
    
    if trainer_col is None:
        print("ERROR: 'Trainer Model' column not found")
        return None
    
    # ===========================================
    # MATCH SCORE CALCULATION
    # ===========================================
    if comfort_col is None or cushioning_col is None or responsiveness_col is None:
        print("ERROR: Comfort/Cushioning/Responsiveness columns not found")
        return None

    # Convert rating columns to numeric
    df[comfort_col] = pd.to_numeric(df[comfort_col], errors='coerce')
    df[cushioning_col] = pd.to_numeric(df[cushioning_col], errors='coerce')
    df[responsiveness_col] = pd.to_numeric(df[responsiveness_col], errors='coerce')

    # Base score
    df["Base_Score"] = (df[comfort_col] + df[cushioning_col] + df[responsiveness_col]) / 3.0

    # Weight bonus (+2 exact match)
    if weight and weight_col is not None and weight.lower() not in ["", "any"]:
        weight_series = df[weight_col].astype(str).str.lower().str.strip()
        df["Weight_Bonus"] = (weight_series == weight.lower().strip()).astype(int) * 2
    else:
        df["Weight_Bonus"] = 0

    # Run goal bonus (+3 based on run_goal logic)
    run_goal_norm = (run_goal or "").strip().lower()
    run_type_series = df[run_type_col].astype(str).str.lower().str.strip() if run_type_col else pd.Series("", index=df.index)
    five_k_series = df[five_k_col].astype(str).str.lower().str.strip() if five_k_col else pd.Series("", index=df.index)

    df["Run_Goal_Bonus"] = 0
    if run_goal_norm == "speed/tempo":
        df.loc[df[responsiveness_col] > 8, "Run_Goal_Bonus"] = 3
    elif run_goal_norm == "comfy/long run":
        comfy_mask = (
            run_type_series.str.contains("long run", na=False)
            | run_type_series.str.contains("easy", na=False)
            | run_type_series.str.contains("recovery", na=False)
        )
        df.loc[comfy_mask, "Run_Goal_Bonus"] = 3
    elif run_goal_norm == "first 5k":
        first_5k_mask = (
            five_k_series.str.contains("sub 30", na=False)
            | five_k_series.str.contains("sub 35", na=False)
            | five_k_series.str.contains("sub 40", na=False)
        )
        df.loc[first_5k_mask, "Run_Goal_Bonus"] = 3
    elif run_goal_norm == "beginner/walk":
        beginner_mask = (
            five_k_series.str.contains("sub 35", na=False)
            | five_k_series.str.contains("sub 40", na=False)
        )
        df.loc[beginner_mask, "Run_Goal_Bonus"] = 3

    # Pain penalty (-3 if reviewer pain contains user's pain type)
    if pain and pain_col is not None and pain.lower() not in ["", "any"]:
        pain_query = pain.lower().strip()
        if pain_query in ["no pain", "no discomfort", "none"]:
            df["Pain_Penalty"] = 0
        else:
            pain_series_all = df[pain_col].astype(str).str.lower().str.strip()
            df["Pain_Penalty"] = pain_series_all.str.contains(pain_query, na=False).astype(int) * 3
    else:
        df["Pain_Penalty"] = 0

    # Final match score and percentage
    df["Match_Score"] = (df["Base_Score"] + df["Weight_Bonus"] + df["Run_Goal_Bonus"] - df["Pain_Penalty"]).clip(lower=0)
    df["Match_Percentage"] = ((df["Match_Score"] / 15.0) * 100).round(0)

    # ===========================================
    # AGGREGATE RESULTS
    # ===========================================
    
    recommendations = (
        df.groupby(trainer_col)
        .agg(
            Avg_Score=('Score', 'mean'),
            Num_Reviews=('Score', 'count'),
            Match_Percentage=('Match_Percentage', 'mean')
        )
        .reset_index()
        .rename(columns={trainer_col: "Trainer Model"})
        .sort_values(by=['Match_Percentage', 'Avg_Score'], ascending=[False, False])
    )
    
    # Round for display
    recommendations['Avg_Score'] = recommendations['Avg_Score'].round(1)
    recommendations['Match_Percentage'] = recommendations['Match_Percentage'].round(0)
    
    print(f"\n[OK] Found {len(recommendations)} matching trainers:")
    print(recommendations.to_string(index=False))
    
    return recommendations


# ===========================================
# TEST
# ===========================================
if __name__ == "__main__":
    # Example test
    result = get_recommendations(
        run_goal="Comfy/Long Run",
        run_type="Long run",
        terrain="Road",
        foot_width="Regular",
        weight="Between 65kg - 85kg",
        pain="knee"
    )