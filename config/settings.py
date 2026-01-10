# Google Sheets Configuration
SPREADSHEET_NAME = "The Perfect Shoe Project"

# Sheet names
SHEET_RAW_DATA = "rawdata"
SHEET_CLEAN_DATA = "Clean Live Data"
SHEET_QUANT_ANALYSIS = "Quant Analysis"
SHEET_LEADERBOARD = "Leaderboard"
SHEET_USAGE_PATTERNS = "Usage Patterns"
SHEET_KEYWORD_FREQ = "Keyword Frequency"
SHEET_SENTIMENT = "Sentiment Analysis"

# Google API scopes
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]

# Credentials file path
CREDENTIALS_FILE = "credentials.json"