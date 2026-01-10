import gspread
from google.oauth2.service_account import Credentials
from config.settings import SCOPES, CREDENTIALS_FILE, SPREADSHEET_NAME

class SheetsService:
    def __init__(self):
    """Initialize Google Sheets connection"""
    import os
    import json
    
    # Check if running in production (Railway) or locally
    if os.getenv('GOOGLE_CREDENTIALS'):
        # Production: use environment variable
        creds_dict = json.loads(os.getenv('GOOGLE_CREDENTIALS'))
        self.creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    else:
        # Local: use credentials.json file
        self.creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    
    self.client = gspread.authorize(self.creds)
    self.spreadsheet = self.client.open(SPREADSHEET_NAME)
    
    def get_sheet(self, sheet_name):
        """Get a specific worksheet by name"""
        try:
            return self.spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            print(f"Warning: Sheet '{sheet_name}' not found. Creating it...")
            return self.spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
    
    def read_to_dataframe(self, sheet_name):
        """Read sheet data into a pandas DataFrame"""
        import pandas as pd
        sheet = self.get_sheet(sheet_name)
        
        # Get all values to handle potential duplicate headers
        all_values = sheet.get_all_values()
        if not all_values:
            return pd.DataFrame()
        
        headers = all_values[0]
        data_rows = all_values[1:]
        
        # Create DataFrame with original headers
        df = pd.DataFrame(data_rows, columns=headers)
        
        return df
    
    def write_dataframe(self, sheet_name, df, clear_first=True):
        """Write a DataFrame to a sheet"""
        sheet = self.get_sheet(sheet_name)
        
        if clear_first:
            sheet.clear()
        
        # Replace NaN values with empty strings for Google Sheets compatibility
        df_clean = df.fillna('')
        
        # Prepare data with headers
        output_data = [df_clean.columns.tolist()] + df_clean.astype(str).values.tolist()
        sheet.update('A1', output_data)
        print(f"✅ Data written to '{sheet_name}' sheet")