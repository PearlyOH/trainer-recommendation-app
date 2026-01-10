import sys
import importlib

# Force reload of all modules
if 'services.sheets_service' in sys.modules:
    importlib.reload(sys.modules['services.sheets_service'])

from analysis.cleaning import clean_data

if __name__ == "__main__":
    clean_data()