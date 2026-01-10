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
    
    def write_dataframe(self, sheet_name, df):
        """Write a pandas DataFrame to a sheet"""
        sheet = self.get_sheet(sheet_name)
        
        # Clear existing content
        sheet.clear()
        
        # Write headers and data
        headers = df.columns.tolist()
        values = df.values.tolist()
        
        sheet.append_row(headers)
        for row in values:
            sheet.append_row(row)