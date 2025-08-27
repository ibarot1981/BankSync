import gspread
import requests
import json
from datetime import datetime, timedelta
import time
import logging
from logging.handlers import RotatingFileHandler
from typing import List, Dict, Any, Optional
import os
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
LOG_FILE = os.getenv('LOG_FILE', 'banksync.log')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
LOG_MAX_BYTES = int(os.getenv('LOG_MAX_BYTES', 5 * 1024 * 1024))  # Default to 5 MB
LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', 5)) # Default to 5 backup files

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

# Create a rotating file handler
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=LOG_MAX_BYTES,
    backupCount=LOG_BACKUP_COUNT
)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

# Also add a console handler for immediate feedback
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

class BankTransactionSyncer:
    def __init__(self):
        """
        Initialize the syncer with Google Sheets and Grist configurations from environment variables
        """
        # Load configuration from environment variables
        self.gsheet_credentials_path = os.getenv('GSHEET_CREDENTIALS_PATH')
        self.gsheet_id = os.getenv('GSHEET_ID')
        self.worksheet_name = os.getenv('WORKSHEET_NAME', 'Sheet1')
        self.data_dir = os.getenv('DATA_DIR', 'data') # Directory to store TXT files
        
        # Validate required environment variables
        required_vars = {
            'GSHEET_CREDENTIALS_PATH': self.gsheet_credentials_path,
            'GSHEET_ID': self.gsheet_id,
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)
        logger.info(f"Data directory set to: {self.data_dir}")
        
        # Initialize Google Sheets client
        self.gc: gspread.Client = self._setup_google_sheets()

    def _setup_google_sheets(self) -> gspread.Client:
        """Setup Google Sheets client with service account credentials"""
        try:
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
            
            creds = Credentials.from_service_account_file(self.gsheet_credentials_path, scopes=scope)
            return gspread.authorize(creds)
        except Exception as e:
            logger.error(f"Failed to setup Google Sheets client: {e}")
            raise
    
    def get_sheet_data(self) -> List[Dict[str, Any]]:
        """Read data from Google Sheets, filtering for specific fields and adding row numbers"""
        try:
            sheet = self.gc.open_by_key(self.gsheet_id)
            worksheet = sheet.worksheet(self.worksheet_name)
            
            # Fields we want to extract
            required_fields = [
                'Transaction Date', 
                'Transaction Description', 
                'Transaction Amount', 
                'Bank', 
                'Reference No.', 
                'Value Date', 
                'Running Balance'
            ]
            
            # Get all values as a list of lists
            all_values = worksheet.get_all_values()
            
            if not all_values:
                logger.warning("No data found in worksheet")
                return []
            
            # Get header row and find column indices for required fields
            headers = all_values[0]
            field_indices = {}
            
            for field in required_fields:
                try:
                    field_indices[field] = headers.index(field)
                    logger.debug(f"Found '{field}' at column {field_indices[field]}")
                except ValueError:
                    logger.warning(f"Field '{field}' not found in headers. Available headers: {headers}")
                    # Try to find similar field names
                    similar_fields = [h for h in headers if field.lower() in h.lower() or h.lower() in field.lower()]
                    if similar_fields:
                        logger.debug(f"Similar fields found: {similar_fields}")
            
            if not field_indices:
                raise ValueError("None of the required fields were found in the sheet")
            
            # Extract only the required fields from data rows and add actual Google Sheets row numbers
            records = []
            
            for row_index, row_values in enumerate(all_values[1:], 2):  # Skip header row, row_index starts from 2
                # Skip empty rows
                if not any(str(value).strip() for value in row_values):
                    continue
                
                # Create record with only required fields
                record = {}
                
                # Add actual Google Sheets row number as the first field
                record['Row_Num'] = row_index
                
                # Add other required fields
                for field, col_index in field_indices.items():
                    value = row_values[col_index] if col_index < len(row_values) else ''
                    record[field] = value.strip() if value else None
                
                records.append(record)
            
            logger.info(f"Retrieved {len(records)} records from Google Sheets with actual row numbers.")
            return records
            
        except Exception as e:
            logger.error(f"Failed to read Google Sheets data: {e}")
            raise
    
    def _get_current_date_filename(self) -> str:
        """Generate filename based on current date in ddmmyy.txt format"""
        return datetime.now().strftime("%d%m%y") + ".txt"

    def write_records_to_file(self, records: List[Dict[str, Any]]):
        """Write records to a text file in the data directory"""
        file_name = self._get_current_date_filename()
        file_path = os.path.join(self.data_dir, file_name)
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                for record in records:
                    f.write(json.dumps(record) + '\n')
            logger.info(f"Successfully wrote {len(records)} records to {file_path}.")
        except Exception as e:
            logger.error(f"Failed to write records to file {file_path}: {e}")
            raise
    
    def diagnose_sheet_structure(self):
        """Diagnose and display sheet structure for debugging"""
        try:
            sheet = self.gc.open_by_key(self.gsheet_id)
            worksheet = sheet.worksheet(self.worksheet_name)
            
            # Get first few rows
            values = worksheet.get_all_values()
            
            if not values:
                logger.warning("No data found in worksheet")
                return
            
            logger.debug("=== SHEET STRUCTURE DIAGNOSIS ===")
            logger.debug(f"Total rows: {len(values)}")
            logger.debug(f"Total columns: {len(values[0]) if values else 0}")
            
            # Show header row
            headers = values[0]
            logger.debug(f"All headers: {headers}")
            
            # Check for required fields
            required_fields = [
                'Transaction Date', 
                'Transaction Description', 
                'Transaction Amount', 
                'Bank', 
                'Reference No.', 
                'Value Date', 
                'Running Balance'
            ]
            
            logger.debug("=== REQUIRED FIELDS CHECK ===")
            found_fields = []
            missing_fields = []
            
            for field in required_fields:
                if field in headers:
                    found_fields.append(field)
                    logger.debug(f"✓ Found: '{field}' at column {headers.index(field)}")
                else:
                    missing_fields.append(field)
                    logger.warning(f"✗ Missing: '{field}'")
                    # Try to find similar field names
                    similar_fields = [h for h in headers if field.lower() in h.lower() or h.lower() in field.lower()]
                    if similar_fields:
                        logger.debug(f"  Similar fields found: {similar_fields}")
            
            logger.debug(f"Found {len(found_fields)} of {len(required_fields)} required fields")
            
            # Show first few data rows with only required fields and actual row numbers
            if found_fields:
                logger.debug("=== SAMPLE DATA (Required Fields + Actual Row Numbers) ===")
                field_indices = {field: headers.index(field) for field in found_fields}
                
                for i, row in enumerate(values[1:3], 1):  # Show first 2 data rows
                    # Skip empty rows in diagnosis too
                    if not any(str(value).strip() for value in row):
                        continue
                        
                    actual_row_num = i + 1  # Actual Google Sheets row number (header is row 1)
                    sample_data = {'Row_Num': actual_row_num}
                    for field, col_index in field_indices.items():
                        value = row[col_index] if col_index < len(row) else ''
                        sample_data[field] = value
                    logger.debug(f"Google Sheets Row {actual_row_num}: {sample_data}")
            
        except Exception as e:
            logger.error(f"Failed to diagnose sheet structure: {e}")
            raise

    def fetch_and_save_transactions_to_file(self):
        """
        Fetches transactions from Google Sheets and saves them to a dated TXT file.
        """
        try:
            logger.info("Starting transaction fetch and save to file.")
            
            # Get data from Google Sheets
            sheet_data = self.get_sheet_data()
            
            if not sheet_data:
                logger.info("No transactions found in Google Sheets to save.")
                return
            
            # Write data to file
            self.write_records_to_file(sheet_data)
            
            logger.info(f"Fetch and save completed. {len(sheet_data)} records saved.")
            
        except Exception as e:
            logger.error(f"Failed to fetch and save transactions: {e}")
            raise

def main():
    """Main function to run the fetch and save process"""
    try:
        # Initialize syncer (configuration loaded from environment variables)
        syncer = BankTransactionSyncer()
        
        # First, diagnose the sheet structure
        logger.debug("Running sheet structure diagnosis...")
        syncer.diagnose_sheet_structure()
        
        # Run fetch and save
        logger.info("Running fetch and save to file...")
        syncer.fetch_and_save_transactions_to_file()
        
    except Exception as e:
        logger.error(f"Fetch and save failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())