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
    
    # Manually rename columns by position to handle duplicates
    new_columns = df_raw.columns.tolist()
    new_columns[3] = "Name"
    new_columns[9] = "Trainer Model"
    new_columns[10] = "Run Type"
    new_columns[11] = "Distance in Trainers (km)"
    new_columns[12] = "Terrain"  # This is the duplicate that's actually terrain
    new_columns[13] = "Months Wearing"  # Adding this column
    new_columns[14] = "Post Run Feel"
    new_columns[15] = "Pain Experienced"
    new_columns[17] = "Comfort Rating"
    new_columns[18] = "Cushioning Rating"
    new_columns[19] = "Responsiveness Rating"
    new_columns[20] = "Easy Run Pace"
    new_columns[21] = "Average 5k Time"
    new_columns[22] = "Improvement Suggestions"
    
    df_raw.columns = new_columns
    
    # Drop unnecessary columns
    cols_to_drop = [
        'Respondent ID',
        'What gender do you identify with? (Male)',
        'What gender do you identify with? (Female)',
        'What gender do you identify with? (Non-binary)',
        'What gender do you identify with? (Prefer not to say\n)',
        'Would you recommend this trainer to a friend?\n (Yes)',
        'Would you recommend this trainer to a friend?\n (No)',
        'Would you recommend this trainer to a friend?\n (Depends)'
    ]
    
    existing_cols_to_drop = [col for col in cols_to_drop if col in df_raw.columns]
    df_processed = df_raw.drop(columns=existing_cols_to_drop) if existing_cols_to_drop else df_raw.copy()
    
    # Convert numeric columns
    numeric_cols = ['Comfort Rating', 'Cushioning Rating', 'Responsiveness Rating', 'Score']
    
    for col in numeric_cols:
        if col in df_processed.columns:
            df_processed[col] = pd.to_numeric(df_processed[col].astype(str), errors='coerce')
    
    # Convert distance to numeric
    if 'Distance in Trainers (km)' in df_processed.columns:
        df_processed['Distance in Trainers (km)'] = pd.to_numeric(df_processed['Distance in Trainers (km)'], errors='coerce')
    
    # Write to Clean Live Data sheet
    sheets.write_dataframe(SHEET_CLEAN_DATA, df_processed)
    
    print("[OK] Data cleaning complete")
    return df_processed

if __name__ == "__main__":
    clean_data()