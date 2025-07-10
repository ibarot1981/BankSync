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
        self.grist_api_key = os.getenv('GRIST_API_KEY')
        self.grist_doc_id = os.getenv('GRIST_DOC_ID')
        self.grist_table_name = os.getenv('GRIST_TABLE_NAME')
        self.grist_base_host = os.getenv('GRIST_BASE_HOST', 'http://safcost.duckdns.org:8484')
        
        # Validate required environment variables
        required_vars = {
            'GSHEET_CREDENTIALS_PATH': self.gsheet_credentials_path,
            'GSHEET_ID': self.gsheet_id,
            'GRIST_API_KEY': self.grist_api_key,
            'GRIST_DOC_ID': self.grist_doc_id,
            'GRIST_TABLE_NAME': self.grist_table_name,
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        # Initialize Google Sheets client
        self.gc: gspread.Client = self._setup_google_sheets()

        # Grist API base URL (for self-hosted Grist)
        self.grist_base_url = f"{self.grist_base_host}/api/docs/{self.grist_doc_id}/tables/{self.grist_table_name}"
        
        logger.info(f"Using Grist server: {self.grist_base_host}")
        logger.info(f"Grist API URL: {self.grist_base_url}")

        # Headers for Grist API
        self.grist_headers: Dict[str, str] = {
            "Authorization": f"Bearer {self.grist_api_key}",
            "Content-Type": "application/json"
        }
    
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
    
    def get_grist_table_structure(self) -> Dict[str, Any]:
        """Get Grist table structure to understand expected field types"""
        try:
            response = requests.get(
                f"{self.grist_base_host}/api/docs/{self.grist_doc_id}/tables/{self.grist_table_name}/columns",
                headers=self.grist_headers
            )
            response.raise_for_status()
            
            data = response.json()
            columns = data.get('columns', [])
            
            logger.info("=== GRIST TABLE STRUCTURE ===")
            structure = {}
            for col in columns:
                col_id = col.get('id')
                col_type = col.get('type')
                col_label = col.get('label', col_id)
                structure[col_id] = {
                    'type': col_type,
                    'label': col_label
                }
                logger.info(f"Column: {col_id} ('{col_label}') - Type: {col_type}")
            
            return structure
            
        except Exception as e:
            logger.error(f"Failed to get Grist table structure: {e}")
            return {}
    
    def get_sheet_data(self, after_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Read data from Google Sheets, filtering for specific fields and date range"""
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
                    logger.info(f"Found '{field}' at column {field_indices[field]}")
                except ValueError:
                    logger.warning(f"Field '{field}' not found in headers. Available headers: {headers}")
                    # Try to find similar field names
                    similar_fields = [h for h in headers if field.lower() in h.lower() or h.lower() in field.lower()]
                    if similar_fields:
                        logger.info(f"Similar fields found: {similar_fields}")
            
            if not field_indices:
                raise ValueError("None of the required fields were found in the sheet")
            
            # Convert after_date to datetime for comparison
            after_datetime = None
            if after_date:
                after_datetime = self._parse_date_string(after_date)
                logger.info(f"Filtering records after: {after_datetime}")
            
            # Extract only the required fields from data rows
            records = []
            for row_num, row_values in enumerate(all_values[1:], 2):  # Skip header row
                # Skip empty rows
                if not any(str(value).strip() for value in row_values):
                    continue
                
                # Create record with only required fields
                record = {}
                for field, col_index in field_indices.items():
                    value = row_values[col_index] if col_index < len(row_values) else ''
                    record[field] = value.strip() if value else None
                
                # Filter by date if specified
                if after_datetime and 'Transaction Date' in record:
                    transaction_date = self._parse_date_string(record['Transaction Date'])
                    if transaction_date and transaction_date <= after_datetime:
                        continue  # Skip this record as it's not newer than the filter date
                
                records.append(record)
            
            logger.info(f"Retrieved {len(records)} records from Google Sheets (after date filtering)")
            return records
            
        except Exception as e:
            logger.error(f"Failed to read Google Sheets data: {e}")
            raise
    
    def get_last_transaction_date(self) -> Optional[str]:
        """Get the latest transaction date from Grist table"""
        try:
            # Get records sorted by Transaction Date descending
            response = requests.get(
                f"{self.grist_base_url}/records?sort=-Transaction%20Date&limit=1",
                headers=self.grist_headers
            )
            response.raise_for_status()
            
            data = response.json()
            records = data.get('records', [])
            
            if records:
                last_record = records[0]
                last_date = last_record.get('fields', {}).get('Transaction Date')
                logger.info(f"Last transaction date in Grist: {last_date}")
                return last_date
            else:
                logger.info("No existing records found in Grist")
                return None
            
        except Exception as e:
            logger.error(f"Failed to get last transaction date from Grist: {e}")
            return None
    
    def _parse_date_string(self, date_string: str) -> Optional[datetime]:
        """Parse date string into datetime object"""
        if not date_string:
            return None
        
        # Common date formats to try
        date_formats = [
            '%Y-%m-%d',      # 2025-07-02
            '%d/%m/%Y',      # 02/07/2025
            '%m/%d/%Y',      # 07/02/2025
            '%d-%m-%Y',      # 02-07-2025
            '%Y-%m-%d %H:%M:%S',  # 2025-07-02 14:30:00
            '%d/%m/%Y %H:%M:%S',  # 02/07/2025 14:30:00
            '%m/%d/%Y %H:%M:%S',  # 07/02/2025 14:30:00
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(str(date_string).strip(), fmt)
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {date_string}")
        return None
    
    def normalize_date(self, date_value: Any) -> Optional[str]:
        """Normalize date values to ISO format"""
        if not date_value:
            return None
        
        try:
            # Handle different date formats
            if isinstance(date_value, str):
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y']:
                    try:
                        dt = datetime.strptime(date_value, fmt)
                        return dt.strftime('%Y-%m-%d')
                    except ValueError:
                        continue
            
            # If it's already a datetime object
            if hasattr(date_value, 'strftime'):
                return date_value.strftime('%Y-%m-%d')
            
            return str(date_value)
            
        except Exception as e:
            logger.warning(f"Failed to normalize date {date_value}: {e}")
            return str(date_value)
    
    def normalize_amount(self, amount_value: Any) -> Optional[float]:
        """Normalize amount values to float"""
        if not amount_value:
            return None
        
        try:
            # Remove currency symbols and commas
            if isinstance(amount_value, str):
                amount_str = amount_value.replace('$', '').replace(',', '').replace('₹', '').strip()
                if not amount_str:
                    return None
                return float(amount_str)
            
            return float(amount_value)
            
        except Exception as e:
            logger.warning(f"Failed to normalize amount {amount_value}: {e}")
            return None
    
    def prepare_grist_record(self, sheet_record: Dict[str, Any], grist_structure: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a record for Grist based on the table structure"""
        normalized_record = {}
        
        # Define the mapping from Google Sheets field names to Grist field names
        google_to_grist_map = {
            'Transaction Date': 'Transaction_Date',
            'Transaction Description': 'Transaction_Description',
            'Transaction Amount': 'Transaction_Amount',
            'Reference No.': 'Reference_No',
            'Value Date': 'Value_Date'
        }

        for field_name, field_value in sheet_record.items():
            if field_value is None or field_value == '':
                continue
                
            grist_field = None
            # Check if the field is in our explicit mapping
            if field_name in google_to_grist_map:
                grist_field = google_to_grist_map[field_name]
            else:
                # If not in explicit mapping, try to find it in Grist structure by label or ID
                for grist_col_id, grist_col_info in grist_structure.items():
                    if grist_col_info['label'] == field_name or grist_col_id == field_name:
                        grist_field = grist_col_id
                        break
            
            if not grist_field:
                logger.warning(f"Field '{field_name}' not found in Grist structure or explicit mapping, skipping")
                continue
            
            # Get Grist field type from the structure (assuming grist_field is now valid)
            # We need to ensure grist_field exists in grist_structure before accessing its type
            if grist_field not in grist_structure:
                logger.warning(f"Mapped Grist field '{grist_field}' for Google Sheets field '{field_name}' not found in Grist structure, skipping")
                continue

            grist_type = grist_structure[grist_field]['type']
            
            if grist_type == 'Date':
                normalized_value = self.normalize_date(field_value)
            elif grist_type == 'Numeric':
                normalized_value = self.normalize_amount(field_value)
            else:
                # Text or other types
                normalized_value = str(field_value) if field_value else None
            
            if normalized_value is not None:
                normalized_record[grist_field] = normalized_value
        
        return normalized_record
    
    def create_grist_record(self, record_data: Dict[str, Any]) -> bool:
        """Create a new record in Grist with enhanced error handling"""
        try:
            payload = {
                "records": [
                    {
                        "fields": record_data
                    }
                ]
            }
            
            # Log the payload for debugging
            logger.debug(f"Sending payload to Grist: {json.dumps(payload, indent=2)}")
            
            response = requests.post(
                f"{self.grist_base_url}/records",
                headers=self.grist_headers,
                json=payload
            )
            
            # Enhanced error handling
            if response.status_code != 200:
                logger.error(f"Grist API error: {response.status_code}")
                logger.error(f"Response headers: {response.headers}")
                logger.error(f"Response content: {response.text}")
                
                # Try to parse error details
                try:
                    error_data = response.json()
                    logger.error(f"Error details: {json.dumps(error_data, indent=2)}")
                except:
                    pass
                
                return False
            
            response.raise_for_status()
            
            logger.info(f"Successfully created record in Grist")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create Grist record: {e}")
            return False
    
    def test_grist_connection(self):
        """Test the Grist connection with enhanced debugging"""
        try:
            logger.info("Testing Grist connection...")
            logger.info(f"Grist server: {self.grist_base_host}")
            logger.info(f"Document ID: {self.grist_doc_id}")
            logger.info(f"Table name: {self.grist_table_name}")
            
            # Test basic connection
            response = requests.get(
                f"{self.grist_base_host}/api/docs/{self.grist_doc_id}",
                headers=self.grist_headers
            )
            
            if response.status_code == 200:
                logger.info("✓ Grist connection successful")
                
                # Test table access
                table_response = requests.get(
                    f"{self.grist_base_url}/records?limit=1",
                    headers=self.grist_headers
                )
                
                if table_response.status_code == 200:
                    logger.info("✓ Table access successful")
                    return True
                else:
                    logger.error(f"✗ Table access failed: {table_response.status_code}")
                    logger.error(f"Response: {table_response.text}")
                    return False
                    
            else:
                logger.error(f"✗ Grist connection failed: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"✗ Grist connection test failed: {e}")
            return False
    
    def sync_transactions(self, test_mode: bool = False):
        """
        Sync new transactions from Google Sheets to Grist
        Only syncs transactions newer than the last transaction date in Grist
        """
        try:
            logger.info("Starting incremental transaction sync...")
            
            # Test connections first
            if not self.test_grist_connection():
                raise Exception("Grist connection test failed")
            
            # Get Grist table structure
            grist_structure = self.get_grist_table_structure()
            if not grist_structure:
                raise Exception("Failed to get Grist table structure")
            
            # Get the last transaction date from Grist
            last_transaction_date = self.get_last_transaction_date()
            
            # Get new data from Google Sheets
            sheet_data = self.get_sheet_data(after_date=last_transaction_date)
            
            if not sheet_data:
                logger.info("No new transactions found in Google Sheets")
                return
            
            # In test mode, only process first few records
            if test_mode:
                sheet_data = sheet_data[:3]
                logger.info(f"Test mode: Processing only {len(sheet_data)} records")
            
            created_count = 0
            error_count = 0
            
            for i, sheet_record in enumerate(sheet_data):
                try:
                    logger.info(f"Processing record {i+1}/{len(sheet_data)}")
                    logger.debug(f"Original record: {sheet_record}") # Changed to debug
                    
                    # Prepare record for Grist
                    normalized_record = self.prepare_grist_record(sheet_record, grist_structure)
                    logger.debug(f"Normalized record: {normalized_record}") # Changed to debug
                    
                    if not normalized_record:
                        logger.warning("No valid fields found in record, skipping")
                        continue
                    
                    # Create new record in Grist
                    if self.create_grist_record(normalized_record):
                        created_count += 1
                    else:
                        error_count += 1
                        if test_mode:
                            logger.info("Test mode: Stopping after first error")
                            break
                    
                    # Add small delay to avoid rate limiting
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Error processing record {sheet_record}: {e}")
                    error_count += 1
                    if test_mode:
                        logger.info("Test mode: Stopping after error")
                        break
                    continue
            
            logger.info(f"Sync completed - Created: {created_count}, Errors: {error_count}")
            
        except Exception as e:
            logger.error(f"Failed to sync transactions: {e}")
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
            
            logger.info("=== SHEET STRUCTURE DIAGNOSIS ===")
            logger.info(f"Total rows: {len(values)}")
            logger.info(f"Total columns: {len(values[0]) if values else 0}")
            
            # Show header row
            headers = values[0]
            logger.info(f"All headers: {headers}")
            
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
            
            logger.info("=== REQUIRED FIELDS CHECK ===")
            found_fields = []
            missing_fields = []
            
            for field in required_fields:
                if field in headers:
                    found_fields.append(field)
                    logger.info(f"✓ Found: '{field}' at column {headers.index(field)}")
                else:
                    missing_fields.append(field)
                    logger.warning(f"✗ Missing: '{field}'")
                    # Try to find similar field names
                    similar_fields = [h for h in headers if field.lower() in h.lower() or h.lower() in field.lower()]
                    if similar_fields:
                        logger.info(f"  Similar fields found: {similar_fields}")
            
            logger.info(f"Found {len(found_fields)} of {len(required_fields)} required fields")
            
            # Show first few data rows with only required fields
            if found_fields:
                logger.info("=== SAMPLE DATA (Required Fields Only) ===")
                field_indices = {field: headers.index(field) for field in found_fields}
                
                for i, row in enumerate(values[1:3], 1):  # Show first 2 data rows
                    sample_data = {}
                    for field, col_index in field_indices.items():
                        value = row[col_index] if col_index < len(row) else ''
                        sample_data[field] = value
                    logger.info(f"Row {i}: {sample_data}")
            
        except Exception as e:
            logger.error(f"Failed to diagnose sheet structure: {e}")
            raise

def main():
    """Main function to run the sync"""
    try:
        # Initialize syncer (configuration loaded from environment variables)
        syncer = BankTransactionSyncer()
        
        # First, diagnose the sheet structure
        logger.info("Running sheet structure diagnosis...")
        syncer.diagnose_sheet_structure()
        
        # Get Grist table structure
        logger.info("Getting Grist table structure...")
        syncer.get_grist_table_structure()
        
        # Test Grist connection
        logger.info("Testing Grist connection...")
        if not syncer.test_grist_connection():
            logger.error("Grist connection test failed. Please check your configuration.")
            return 1
        
        # Run sync without asking for user input (always full sync)
        logger.info("Running full sync...")
        syncer.sync_transactions(test_mode=False) # Always run in full sync mode
        
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
