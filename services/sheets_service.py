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
        
        # Get all values
        all_values = sheet.get_all_values()
        
        if not all_values:
            return pd.DataFrame()
        
        # First row is headers, rest is data
        headers = all_values[0]
        data = all_values[1:]
        
        return pd.DataFrame(data, columns=headers)
    
    def write_dataframe(self, sheet_name, df, clear_first=True):
        """Write a pandas DataFrame to a sheet"""
        sheet = self.get_sheet(sheet_name)
        
        # Clear existing content
        if clear_first:
            sheet.clear()
        
        # Write headers and data
        import pandas as pd
        import numpy as np
        
        # Replace NaN, None, and inf values with empty strings for JSON compatibility
        df_clean = df.fillna('').replace([np.inf, -np.inf], '')
        
        headers = df_clean.columns.tolist()
        values = df_clean.values.tolist()
        
        # Convert any remaining non-serializable values to strings
        def clean_value(val):
            if pd.isna(val) or val is None:
                return ''
            if isinstance(val, (float, np.floating)) and (np.isnan(val) or np.isinf(val)):
                return ''
            return val
        
        cleaned_values = [[clean_value(val) for val in row] for row in values]
        
        # Use batch operation instead of row-by-row to reduce API calls
        all_rows = [headers] + cleaned_values
        if all_rows:
            sheet.append_rows(all_rows)