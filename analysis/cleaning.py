import pandas as pd
from services.sheets_service import SheetsService
from config.settings import SHEET_RAW_DATA, SHEET_CLEAN_DATA

def clean_data():
    """Clean raw data and write to Clean Live Data sheet"""
    print("Starting data cleaning...")
    
    sheets = SheetsService()
    
    # Read raw data manually to handle duplicate column names
    sheet = sheets.get_sheet(SHEET_RAW_DATA)
    all_values = sheet.get_all_values()
    
    if not all_values:
        print("Error: No data found")
        return None
    
    # Get headers and data
    headers = all_values[0]
    data_rows = all_values[1:]
    
    # Create DataFrame with original headers
    df_raw = pd.DataFrame(data_rows, columns=headers)
    
    # Print ALL headers with positions for debugging
    print(f"\nFound {len(headers)} columns in raw data:")
    print("=" * 80)
    for i, col in enumerate(headers):
        print(f"  [{i}] {col}")
    print("=" * 80)
    
    import re
    
    # Define column mapping with more specific patterns
    # Order matters - more specific patterns should come first
    column_patterns = [
        # Exact or very specific matches first
        (r'^.*submission.*id.*$', "Submission ID", False),
        (r'^.*submitted.*at.*$', "Submitted at", False),
        (r'^.*name.*$', "Name", False),
        (r'^.*what.*gender.*do.*you.*identify.*with.*\?$', "Gender", False),  # Main gender question
        (r'^.*foot.*width.*$', "Foot Width", False),
        (r'^.*trainer.*model.*$', "Trainer Model", False),  # Column K
        (r'^.*run.*type.*$', "Run Type", False),
        (r'^.*distance.*trainer.*km.*$', "Total Distance", False),
        (r'^.*distance.*trainer.*$', "Total Distance", False),
        (r'^.*terrain.*$', "Terrain", False),
        (r'^.*months.*wear.*$', "Months Wearing", False),
        (r'^.*post.*run.*feel.*$', "Post Run Feel", False),
        (r'^.*pain.*experience.*$', "Pain Experienced", False),
        (r'^.*more.*information.*$', "More Information", False),  # Column S
        (r'^.*comfort.*rating.*$', "Comfort Rating", False),
        (r'^.*cushioning.*rating.*$', "Cushioning Rating", False),
        (r'^.*responsiveness.*rating.*$', "Responsiveness Rating", False),
        (r'^.*easy.*run.*pace.*$', "Easy Run Pace", False),
        (r'^.*average.*5k.*race.*time.*minutes.*$', None, True),  # Drop this duplicate
        (r'^.*average.*5k.*time.*$', "Average 5k Time", False),
        (r'^.*5k.*time.*$', "Average 5k Time", False),
        (r'^.*improvement.*suggestion.*$', "Improvement Suggestions", False),
        (r'^.*magic.*wand.*change.*one.*thing.*$', None, True),  # Drop this duplicate
        (r'^.*how.*do.*your.*trainers.*feel.*after.*typical.*run.*$', None, True),  # Drop this duplicate
        (r'^.*would.*you.*recommend.*trainer.*friend.*\?.*$', "Would Recommend", False),
        (r'^.*would.*recommend.*$', "Would Recommend", False),
        (r'^.*score.*$', "Score", False),
    ]
    
    # Columns to exclude from mapping (these will be dropped)
    exclude_patterns = [
        r'respondent.*id',
        r'gender.*male',
        r'gender.*female',
        r'gender.*non-binary',
        r'gender.*prefer.*not',
        r'recommend.*yes',
        r'recommend.*no',
        r'recommend.*depends',
    ]
    
    # Create mapping dictionary
    column_mapping = {}
    matched_clean_names = set()  # Track which clean names we've already mapped
    columns_to_drop = []  # Track columns to drop
    
    # First, check if column should be excluded
    def should_exclude(col_name):
        col_lower = str(col_name).lower().strip()
        for pattern in exclude_patterns:
            if re.search(pattern, col_lower, re.IGNORECASE):
                return True
        return False
    
    # Match columns using patterns
    print("\nMapping columns:")
    print("-" * 80)
    for col in headers:
        col_lower = str(col).lower().strip()
        
        # Skip if should be excluded
        if should_exclude(col):
            print(f"  Excluding: '{col}'")
            columns_to_drop.append(col)
            continue
        
        # Skip if already mapped
        if col in column_mapping:
            continue
        
        # Try to match against patterns (in order)
        matched = False
        for pattern, clean_name, should_drop in column_patterns:
            # Check pattern match
            if re.search(pattern, col_lower, re.IGNORECASE):
                if should_drop or clean_name is None:
                    # This column should be dropped
                    columns_to_drop.append(col)
                    print(f"  Dropping duplicate: '{col}'")
                    matched = True
                    break
                elif clean_name not in matched_clean_names:
                    # Map to clean name
                    column_mapping[col] = clean_name
                    matched_clean_names.add(clean_name)
                    print(f"  Mapped: '{col}' -> '{clean_name}'")
                    matched = True
                    break
        
        if not matched:
            print(f"  No match: '{col}'")
    
    print("-" * 80)
    
    # Apply the mapping
    df_raw = df_raw.rename(columns=column_mapping)
    
    # Drop excluded and duplicate columns
    all_cols_to_drop = list(set(columns_to_drop))
    
    # Also drop exact matches
    exact_drops = [
        'Respondent ID',
        'What gender do you identify with? (Male)',
        'What gender do you identify with? (Female)',
        'What gender do you identify with? (Non-binary)',
        'What gender do you identify with? (Prefer not to say\n)',
        'Would you recommend this trainer to a friend?\n (Yes)',
        'Would you recommend this trainer to a friend?\n (No)',
        'Would you recommend this trainer to a friend?\n (Depends)'
    ]
    
    for col in exact_drops:
        if col in df_raw.columns:
            all_cols_to_drop.append(col)
    
    existing_cols_to_drop = [col for col in all_cols_to_drop if col in df_raw.columns]
    df_processed = df_raw.drop(columns=existing_cols_to_drop) if existing_cols_to_drop else df_raw.copy()
    
    # Show unmapped columns for debugging
    unmapped = [col for col in df_processed.columns if col not in column_mapping.values()]
    if unmapped:
        print(f"\n  Warning: {len(unmapped)} columns not mapped:")
        for col in unmapped:
            print(f"    - '{col}'")
    
    # Convert numeric columns
    numeric_cols = ['Comfort Rating', 'Cushioning Rating', 'Responsiveness Rating', 'Score']
    
    for col in numeric_cols:
        if col in df_processed.columns:
            df_processed[col] = pd.to_numeric(df_processed[col].astype(str), errors='coerce')
    
    # Convert distance to numeric
    if 'Total Distance' in df_processed.columns:
        df_processed['Total Distance'] = pd.to_numeric(df_processed['Total Distance'], errors='coerce')
    
    # Write to Clean Live Data sheet
    sheets.write_dataframe(SHEET_CLEAN_DATA, df_processed)
    
    print("\n[OK] Data cleaning complete")
    print(f"Final columns ({len(df_processed.columns)}): {list(df_processed.columns)}")
    return df_processed

if __name__ == "__main__":
    clean_data()