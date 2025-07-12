import requests
import json
from datetime import datetime, timedelta
import time
import logging
from logging.handlers import RotatingFileHandler
from typing import List, Dict, Any, Optional
import os
import csv # Added import for CSV handling
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
LOG_FILE = os.getenv('LOG_FILE', 'gristbankupdater.log')
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

class GristBankUpdater:
    def __init__(self):
        """
        Initialize the updater with Grist configurations from environment variables
        """
        self.grist_api_key = os.getenv('GRIST_API_KEY')
        self.grist_doc_id = os.getenv('GRIST_DOC_ID')
        self.grist_table_name = os.getenv('GRIST_TABLE_NAME')
        self.grist_base_host = os.getenv('GRIST_BASE_HOST', 'http://safcost.duckdns.org:8484')
        self.data_dir = os.getenv('DATA_DIR', 'data')
        self.archive_dir = os.getenv('ARCHIVE_DIR', 'archive')
        
        # Validate required environment variables
        required_vars = {
            'GRIST_API_KEY': self.grist_api_key,
            'GRIST_DOC_ID': self.grist_doc_id,
            'GRIST_TABLE_NAME': self.grist_table_name,
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        # Ensure data and archive directories exist
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.archive_dir, exist_ok=True)
        logger.info(f"Data directory set to: {self.data_dir}")
        logger.info(f"Archive directory set to: {self.archive_dir}")

        # Grist API base URL (for self-hosted Grist)
        self.grist_base_url = f"{self.grist_base_host}/api/docs/{self.grist_doc_id}/tables/{self.grist_table_name}"
        
        logger.info(f"Using Grist server: {self.grist_base_host}")
        logger.info(f"Grist API URL: {self.grist_base_url}")

        # Headers for Grist API
        self.grist_headers: Dict[str, str] = {
            "Authorization": f"Bearer {self.grist_api_key}",
            "Content-Type": "application/json"
        }

    def _get_current_date_filename(self) -> str:
        """Generate filename based on current date in ddmmyy.txt format"""
        return datetime.now().strftime("%d%m%y") + ".txt"

    def read_records_from_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Read records from a text file"""
        records = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        records.append(json.loads(line.strip()))
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding JSON from line: {line.strip()} - {e}")
            logger.info(f"Successfully read {len(records)} records from {file_path}")
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
        except Exception as e:
            logger.error(f"Failed to read records from file {file_path}: {e}")
        return records

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
            
            logger.debug("=== GRIST TABLE STRUCTURE ===")
            structure = {}
            for col in columns:
                col_id = col.get('id')
                col_type = col.get('type')
                col_label = col.get('label', col_id)
                structure[col_id] = {
                    'type': col_type,
                    'label': col_label
                }
                logger.debug(f"Column: {col_id} ('{col_label}') - Type: {col_type}")
            
            return structure
            
        except Exception as e:
            logger.error(f"Failed to get Grist table structure: {e}")
            return {}

    def get_recent_grist_records(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get a list of recent transaction records from Grist"""
        try:
            # Get records sorted by Transaction Date descending
            response = requests.get(
                f"{self.grist_base_url}/records?sort=-Transaction_Date&limit={limit}",
                headers=self.grist_headers
            )
            response.raise_for_status()
            
            data = response.json()
            records = [rec.get('fields', {}) for rec in data.get('records', [])]
            
            logger.debug(f"Retrieved {len(records)} recent records from Grist for comparison.")
            if records:
                # Assuming records are sorted by date descending, the first one is the latest
                last_grist_record = records[0]
                logger.info(f"Last record found in Grist: Transaction Date: {last_grist_record.get('Transaction_Date')}, Description: {last_grist_record.get('Transaction_Description')}, Amount: {last_grist_record.get('Transaction_Amount')}")
            else:
                logger.info("No existing records found in Grist.")
            return records
            
        except Exception as e:
            logger.error(f"Failed to get recent Grist records: {e}")
            return []

    def _record_matches(self, file_record: Dict[str, Any], grist_record: Dict[str, Any]) -> bool:
        """
        Compares a record from the file with a record from Grist based on key fields.
        """
        # Get bank name from the file record for proper date formatting
        bank_name = file_record.get('Bank')
        
        file_date = self.normalize_date(file_record.get('Transaction Date'), bank_name)
        file_desc = file_record.get('Transaction Description')
        file_amount = self.normalize_amount(file_record.get('Transaction Amount'))

        grist_date = self.normalize_date(grist_record.get('Transaction_Date'), bank_name)
        grist_desc = grist_record.get('Transaction_Description')
        grist_amount = self.normalize_amount(grist_record.get('Transaction_Amount'))
        
        # Basic check for None values before comparison
        if any(val is None for val in [file_date, file_desc, file_amount, grist_date, grist_desc, grist_amount]):
            return False

        # Compare normalized values
        return (file_date == grist_date and
                file_desc == grist_desc and
                file_amount == grist_amount)

    def _parse_date_string(self, date_string: str, bank_name: Optional[str] = None) -> Optional[datetime]:
        if not date_string:
            return None
        
        cleaned_date_string = str(date_string).strip()
        
        # ADD THIS DEBUG CODE TO TRACE THE ISSUE
        logger.debug(f"=== DATE PARSING DEBUG ===")
        logger.debug(f"Input date_string: {repr(date_string)}")
        logger.debug(f"Input type: {type(date_string)}")
        logger.debug(f"Cleaned date_string: {repr(cleaned_date_string)}")
        logger.debug(f"Is digit check: {cleaned_date_string.isdigit()}")
        
        # If it's a suspicious timestamp, log the stack trace
        if cleaned_date_string.isdigit() and len(cleaned_date_string) == 10:
            import traceback
            logger.warning(f"SUSPICIOUS TIMESTAMP DETECTED: {cleaned_date_string}")
            logger.warning(f"Call stack: {traceback.format_stack()}")
        
        # Try to parse as Unix timestamp first if it looks like a number
        try:
            # Check if it's a string that can be converted to an integer
            if cleaned_date_string.isdigit():
                timestamp = int(cleaned_date_string)
                logger.debug(f"Parsed as integer timestamp: {timestamp}")
                
                # Basic sanity check for timestamp range
                if 0 <= timestamp <= 4102444800: # Approx. end of 2100
                    parsed_dt = datetime.fromtimestamp(timestamp)
                    logger.warning(f"CONVERTING TIMESTAMP TO DATE: {timestamp} -> {parsed_dt}")
                    return parsed_dt
                else:
                    logger.debug(f"Timestamp {timestamp} outside valid range, treating as string")
        except ValueError:
            logger.debug("Not a valid integer, proceeding to string parsing")
        except (ValueError, OSError) as e:
            logger.warning(f"Error parsing '{cleaned_date_string}' as timestamp: {e}")

        # Continue with your existing string parsing logic...
        logger.debug(f"Proceeding with string date parsing for: {cleaned_date_string}")


        # Existing string parsing logic
        if 'am' in cleaned_date_string:
            cleaned_date_string = cleaned_date_string.replace('am', 'AM')
        if 'pm' in cleaned_date_string:
            cleaned_date_string = cleaned_date_string.replace('pm', 'PM')

        # Define formats - FIXED: Added missing %d-%m-%Y %H:%M:%S format
        mm_dd_yyyy_formats = [
            '%m-%d-%Y %I:%M:%S %p', '%m-%d-%Y %H:%M:%S', '%m-%d-%Y',
            '%m/%d/%Y %H:%M:%S', '%m/%d/%Y'
        ]
        dd_mm_yyyy_formats = [
            '%d-%m-%Y %H:%M:%S',        # ADDED: This was missing!
            '%d/%m/%Y %H:%M:%S', 
            '%d-%m-%Y %I:%M:%S %p', 
            '%d-%m-%Y %I:%M%p',
            '%d-%m-%Y'
        ]
        yyyy_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']

        parsed_dt = None

        # Log the bank name and date string for debugging
        logger.debug(f"Parsing date '{cleaned_date_string}' for bank '{bank_name}'")

        if bank_name and bank_name.upper() == 'ICICI':
            logger.debug("ICICI bank detected. Trying MM-DD-YYYY formats first.")
            for fmt in mm_dd_yyyy_formats:
                try:
                    logger.debug(f"Attempting to parse '{cleaned_date_string}' with format '{fmt}'")
                    parsed_dt = datetime.strptime(cleaned_date_string, fmt)
                    logger.debug(f"Successfully parsed '{cleaned_date_string}' with format '{fmt}'")
                    break # Found a match
                except ValueError:
                    logger.debug(f"Failed to parse '{cleaned_date_string}' with format '{fmt}'")
                    continue
            
            if not parsed_dt: # If MM-DD-YYYY failed, try DD-MM-YYYY as fallback for ICICI
                logger.debug("MM-DD-YYYY formats failed for ICICI. Trying DD-MM-YYYY formats.")
                for fmt in dd_mm_yyyy_formats:
                    try:
                        logger.debug(f"Attempting to parse '{cleaned_date_string}' with format '{fmt}'")
                        parsed_dt = datetime.strptime(cleaned_date_string, fmt)
                        logger.debug(f"Successfully parsed '{cleaned_date_string}' with format '{fmt}'")
                        break
                    except ValueError:
                        logger.debug(f"Failed to parse '{cleaned_date_string}' with format '{fmt}'")
                        continue
        else:
            logger.debug("Non-ICICI bank or no bank specified.")
            
            # Prioritize the specific format 'DD/MM/YYYY HH:MM:SS' for non-ICICI banks
            specific_format = '%d/%m/%Y %H:%M:%S'
            try:
                logger.debug(f"Attempting to parse '{cleaned_date_string}' with specific format '{specific_format}'")
                parsed_dt = datetime.strptime(cleaned_date_string, specific_format)
                logger.debug(f"Successfully parsed '{cleaned_date_string}' with format '{specific_format}'")
            except ValueError:
                logger.debug(f"Failed to parse '{cleaned_date_string}' with specific format '{specific_format}'. Trying other formats.")
                parsed_dt = None # Ensure parsed_dt is None if specific format fails

            # If specific format failed, try other formats
            if not parsed_dt:
                logger.debug("Trying DD-MM-YYYY formats.")
                for fmt in dd_mm_yyyy_formats:
                    try:
                        logger.debug(f"Attempting to parse '{cleaned_date_string}' with format '{fmt}'")
                        parsed_dt = datetime.strptime(cleaned_date_string, fmt)
                        logger.debug(f"Successfully parsed '{cleaned_date_string}' with format '{fmt}'")
                        break # Found a match
                    except ValueError:
                        logger.debug(f"Failed to parse '{cleaned_date_string}' with format '{fmt}'")
                        continue
            
            if not parsed_dt: # If DD-MM-YYYY failed, try MM-DD-YYYY
                logger.debug("DD-MM-YYYY formats failed. Trying MM-DD-YYYY formats.")
                for fmt in mm_dd_yyyy_formats:
                    try:
                        logger.debug(f"Attempting to parse '{cleaned_date_string}' with format '{fmt}'")
                        parsed_dt = datetime.strptime(cleaned_date_string, fmt)
                        logger.debug(f"Successfully parsed '{cleaned_date_string}' with format '{fmt}'")
                        break
                    except ValueError:
                        logger.debug(f"Failed to parse '{cleaned_date_string}' with format '{fmt}'")
                        continue

        # Try YYYY formats as a last resort if not parsed yet
        if not parsed_dt:
            for fmt in yyyy_formats:
                try:
                    logger.debug(f"Attempting to parse '{cleaned_date_string}' with format '{fmt}'")
                    parsed_dt = datetime.strptime(cleaned_date_string, fmt)
                    logger.debug(f"Successfully parsed '{cleaned_date_string}' with format '{fmt}'")
                    break
                except ValueError:
                    logger.debug(f"Failed to parse '{cleaned_date_string}' with format '{fmt}'")
                    continue

        if not parsed_dt:
            logger.warning(f"Could not parse date: {date_string}")
        
        return parsed_dt
    
    def normalize_date(self, date_value: Any, bank_name: Optional[str] = None) -> Optional[datetime]: # Return type changed
        """Parse date value into a datetime object.""" # Docstring updated
        if not date_value:
            return None
        # ADD DEBUG LOGGING HERE
        logger.debug(f"=== NORMALIZE_DATE DEBUG ===")
        logger.debug(f"Input date_value: {repr(date_value)}")
        logger.debug(f"Input type: {type(date_value)}")
        logger.debug(f"Bank name: {bank_name}")
        try:
            dt = None
            # If it's already a datetime object
            if isinstance(date_value, datetime):
                dt = date_value
            # If it's a string, try to parse it using _parse_date_string
            elif isinstance(date_value, str):
                dt = self._parse_date_string(date_value, bank_name)
            
            if dt:
                # Logging remains for context, but we return the datetime object.
                if bank_name and bank_name.upper() == 'ICICI':
                    formatted_date_for_log = dt.strftime('%d/%m/%Y %H:%M:%S')
                    logger.info(f"ICICI bank: Converting date {date_value} -> {formatted_date_for_log}")
                
                return dt # Return the datetime object
            else:
                logger.warning(f"Could not parse or process date value: {date_value}")
                return None
            
        except Exception as e:
            logger.warning(f"Failed to normalize date {date_value}: {e}")
            return None
    
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

        bank_name = sheet_record.get('Bank') # Get the bank name
        logger.info(f"--- prepare_grist_record ---")
        logger.info(f"Processing record for Bank: {bank_name}")
        logger.info(f"Original sheet_record: {sheet_record}")

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
            if grist_field not in grist_structure:
                logger.warning(f"Mapped Grist field '{grist_field}' for Google Sheets field '{field_name}' not found in Grist structure, skipping")
                continue

            grist_type = grist_structure[grist_field]['type']
            logger.debug(f"Field '{field_name}' -> Grist field '{grist_field}' (type: {grist_type})")
            
            normalized_value = None
            if grist_type == 'Date':
                # Pass bank_name to normalize_date
                normalized_value = self.normalize_date(field_value, bank_name)
                logger.info(f"Date field '{field_name}': {field_value} -> {normalized_value} (Bank: {bank_name})")
            elif grist_type == 'Numeric':
                normalized_value = self.normalize_amount(field_value)
            elif field_name in ['Transaction Date', 'Value Date']:
                # Force date normalization for known date fields regardless of Grist type
                normalized_value = self.normalize_date(field_value, bank_name)
                logger.info(f"Forced date normalization for '{field_name}': {field_value} -> {normalized_value} (Bank: {bank_name})")
            else:
                # Text or other types
                normalized_value = str(field_value) if field_value else None
            
            if normalized_value is not None:
                normalized_record[grist_field] = normalized_value
        
        logger.info(f"Final normalized record: {normalized_record}")
        return normalized_record
    
    def _create_grist_record(self, record_data: Dict[str, Any]) -> bool:
        """Helper to create a single record in Grist with enhanced error handling"""
        return self.create_grist_records_bulk([record_data])

    def create_grist_records_bulk(self, records_data: List[Dict[str, Any]]) -> bool:
        """Create multiple new records in Grist with enhanced error handling"""
        if not records_data:
            logger.info("No records to insert in bulk.")
            return True # Nothing to do, consider it successful

        try:
            payload = {
                "records": [
                    {"fields": record} for record in records_data
                ]
            }
            
            # Log the payload for debugging
            logger.debug(f"Sending bulk payload to Grist: {json.dumps(payload, indent=2)}")
            
            response = requests.post(
                f"{self.grist_base_url}/records",
                headers=self.grist_headers,
                json=payload
            )
            
            # Enhanced error handling
            if response.status_code != 200:
                logger.error(f"Grist API error during bulk insert: {response.status_code}")
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
            
            logger.info(f"Successfully created {len(records_data)} records in Grist via bulk insert.")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create Grist records in bulk: {e}")
            return False
    
    def test_grist_connection(self):
        """Test the Grist connection with enhanced debugging"""
        try:
            logger.debug("Testing Grist connection...")
            logger.debug(f"Grist server: {self.grist_base_host}")
            logger.debug(f"Document ID: {self.grist_doc_id}")
            logger.debug(f"Table name: {self.grist_table_name}")
            
            # Test basic connection
            response = requests.get(
                f"{self.grist_base_host}/api/docs/{self.grist_doc_id}",
                headers=self.grist_headers
            )
            
            if response.status_code == 200:
                logger.debug("✓ Grist connection successful")
                
                # Test table access
                table_response = requests.get(
                    f"{self.grist_base_url}/records?limit=1",
                    headers=self.grist_headers
                )
                
                if table_response.status_code == 200:
                    logger.debug("✓ Table access successful")
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
        
    def get_last_processed_datetime_and_records(self, limit: int = 500):
        """
        Get the last processed datetime and ALL records that share this datetime.
        Returns (last_datetime, list_of_records_with_that_datetime)
        """
        try:
            # Get records sorted by Transaction Date descending
            response = requests.get(
                f"{self.grist_base_url}/records?sort=-Transaction_Date&limit={limit}",
                headers=self.grist_headers
            )
            response.raise_for_status()
            
            data = response.json()
            all_records = [rec.get('fields', {}) for rec in data.get('records', [])]
            
            if not all_records:
                logger.info("No existing records found in Grist.")
                return None, []
            
            # Get the most recent datetime
            last_datetime_raw = all_records[0].get('Transaction_Date') # Keep raw for comparison logic
            if not last_datetime_raw:
                logger.warning("Most recent record has no Transaction_Date")
                return None, []
            
            # Parse the raw datetime for logging purposes
            parsed_last_datetime_obj = self._parse_date_string(last_datetime_raw, None) # Use None for bank_name
            
            # Format the parsed datetime for logging
            if parsed_last_datetime_obj:
                formatted_last_datetime_str = parsed_last_datetime_obj.strftime('%d-%m-%Y %H:%M:%S')
            else:
                formatted_last_datetime_str = last_datetime_raw # Fallback to raw if parsing fails

            # Find ALL records that share this same datetime
            records_with_last_datetime = []
            for record in all_records:
                record_datetime = record.get('Transaction_Date')
                if record_datetime == last_datetime_raw: # Compare with raw for logic
                    records_with_last_datetime.append(record)
                else:
                    # Since records are sorted by date desc, we can break here
                    break
            
            # Log with the formatted datetime
            logger.info(f"Found {len(records_with_last_datetime)} records with the last processed datetime: {formatted_last_datetime_str}")
            
            # Log details of these records for debugging
            for i, record in enumerate(records_with_last_datetime):
                logger.debug(f"Last datetime record {i+1}: {record.get('Transaction_Description')} - Amount: {record.get('Transaction_Amount')}")
            
            return last_datetime_raw, records_with_last_datetime # Return raw for comparison logic
            
        except Exception as e:
            logger.error(f"Failed to get last processed datetime and records: {e}")
            return None, []

    def should_process_record(self, file_record: Dict[str, Any], file_dt_obj: Optional[datetime], last_dt_obj: Optional[datetime], last_datetime_records: List[Dict[str, Any]]) -> bool:
        """
        Determine if a file record should be processed based on datetime and duplicate checking.
        Uses pre-parsed datetime objects for efficiency.
        """
        bank_name = file_record.get('Bank')
        # file_datetime is no longer needed here as we use file_dt_obj
        # file_datetime = self.normalize_date(file_record.get('Transaction Date'), bank_name)
        
        if not file_dt_obj: # Check the passed-in datetime object
            logger.warning(f"File record has no valid transaction date: {file_record}")
            return False
        
        # If we have no last datetime from Grist, process all records
        if not last_dt_obj:
            return True
        
        # Compare datetimes - we need to be careful about datetime comparison
        try:
            # Use the passed-in datetime objects directly
            
            # If file record is newer than last processed datetime, definitely process it
            if file_dt_obj > last_dt_obj:
                logger.debug(f"Record is newer than last processed datetime: {file_dt_obj} > {last_dt_obj}")
                return True
            
            # If file record is older than last processed datetime, skip it
            if file_dt_obj < last_dt_obj:
                logger.debug(f"Record is older than last processed datetime: {file_dt_obj} < {last_dt_obj}")
                return False
            
            # If file record has the same datetime as last processed datetime,
            # check if it's already in the Grist records with that datetime
            if file_dt_obj == last_dt_obj:
                logger.debug(f"Record has same datetime as last processed: {file_dt_obj}")
                
                # Check if this record already exists in the last_datetime_records
                for grist_record in last_datetime_records:
                    if self._record_matches(file_record, grist_record):
                        logger.debug(f"Record already exists in Grist: {file_record.get('Transaction Description')}")
                        return False
                
                # Same datetime but not a duplicate, so process it
                logger.debug(f"Record has same datetime but is not a duplicate, processing: {file_record.get('Transaction Description')}")
                return True
            
        except Exception as e:
            logger.error(f"Error comparing datetimes: {e}")
            return True  # Process it to be safe
        
        return True  # Default to processing

    def update_grist_from_file(self):
        """
        Reads records from the daily TXT file, identifies new transactions,
        writes them to output.csv, and then archives the file.
        Enhanced version that properly handles multiple records with the same datetime.
        """
        file_name = self._get_current_date_filename()
        file_path = os.path.join(self.data_dir, file_name)

        if not os.path.exists(file_path):
            logger.error(f"Data file not found: {file_path}. Cannot process records.")
            return

        try:
            logger.info(f"Starting record processing from file: {file_path}.")

            # Test connections first (optional for CSV output, but good practice)
            # if not self.test_grist_connection():
            #     raise Exception("Grist connection test failed")
            
            # Get Grist table structure (optional for CSV output, but might be useful for context)
            # grist_structure = self.get_grist_table_structure()
            # if not grist_structure:
            #     raise Exception("Failed to get Grist table structure")

            # Read records from the file
            file_records = self.read_records_from_file(file_path)
            if not file_records:
                logger.info("No records found in the data file. Nothing to process.")
                self.archive_file(file_path)
                return

            # Log sample record to check Bank field
            if file_records:
                logger.info(f"Sample record from file: {file_records[0]}")
                logger.info(f"Bank field value: '{file_records[0].get('Bank')}'")

            # Get the last processed datetime and all records with that datetime
            # This is still needed for duplicate matching logic
            last_datetime, last_datetime_records = self.get_last_processed_datetime_and_records(limit=500)
            
            # Filter records that need to be processed
            records_to_insert = []
            for file_record in file_records:
                # Parse file date once
                file_dt_obj = self.normalize_date(file_record.get('Transaction Date'), file_record.get('Bank'))
                
                # Parse last_datetime once. Note: last_datetime is the raw string from Grist.
                last_dt_obj = self.normalize_date(last_datetime, None) # bank_name is not available here

                if self.should_process_record(file_record, file_dt_obj, last_dt_obj, last_datetime_records):
                    records_to_insert.append(file_record)
                else:
                    # Use file_dt_obj for logging if available, otherwise fall back to original date string
                    log_date = file_dt_obj if file_dt_obj else file_record.get('Transaction Date')
                    logger.debug(f"Skipping record: {log_date} - {file_record.get('Transaction Description')}")

            logger.info(f"Identified {len(records_to_insert)} new records to process out of {len(file_records)} total file records.")

            # --- MODIFICATION START ---
            # Write new records to output.csv
            output_csv_path = 'output.csv'
            if records_to_insert:
                try:
                    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                        # Get fieldnames from the first record, assuming all records have the same keys
                        # Ensure fieldnames are consistent and handle potential missing keys gracefully
                        # A more robust approach would be to collect all unique keys from all records
                        all_keys = set()
                        for record in records_to_insert:
                            all_keys.update(record.keys())
                        fieldnames = sorted(list(all_keys)) # Sort for consistent column order

                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                        writer.writeheader()
                        for record in records_to_insert:
                            # Ensure all keys are present in the row, fill missing with empty string
                            row_to_write = {key: record.get(key, '') for key in fieldnames}
                            writer.writerow(row_to_write)
                    logger.info(f"Successfully wrote {len(records_to_insert)} records to {output_csv_path}")
                except Exception as e:
                    logger.error(f"Failed to write records to {output_csv_path}: {e}")
                    # Continue to archiving even if CSV writing fails, as the core logic is to process records.
            else:
                logger.info("No new records to process, skipping CSV output.")
            # --- MODIFICATION END ---
            
            # Original Grist insertion logic (commented out as per user request)
            # if not records_to_insert:
            #     logger.info("No new records to insert into Grist.")
            #     self.archive_file(file_path)
            #     return

            # # Prepare records for Grist insertion
            # records_to_insert_grist_format = []
            # for i, sheet_record in enumerate(records_to_insert):
            #     try:
            #         logger.debug(f"Preparing record {i+1}/{len(records_to_insert)}")
            #         logger.debug(f"Original record: {sheet_record}")
                    
            #         # Prepare record for Grist
            #         normalized_record = self.prepare_grist_record(sheet_record, grist_structure)
            #         logger.debug(f"Normalized record: {normalized_record}")
                    
            #         if not normalized_record:
            #             logger.warning("No valid fields found in record, skipping")
            #             continue
                    
            #         records_to_insert_grist_format.append(normalized_record)
                    
            #     except Exception as e:
            #         logger.error(f"Error preparing record {sheet_record}: {e}")
            #         continue
            
            # # Insert records into Grist
            # if records_to_insert_grist_format:
            #     logger.info(f"Attempting to bulk insert {len(records_to_insert_grist_format)} records into Grist.")
            #     if self.create_grist_records_bulk(records_to_insert_grist_format):
            #         logger.info(f"Successfully bulk inserted {len(records_to_insert_grist_format)} records.")
            #     else:
            #         logger.error("Bulk insert failed. Check logs for details.")
            # else:
            #     logger.info("No records prepared for insertion into Grist.")
            
            # logger.info("Grist update process completed.")
            
            # Archive the processed file
            self.archive_file(file_path)

        except Exception as e:
            logger.error(f"Record processing script failed: {e}")
            # Re-raise the exception to be caught by the main handler if necessary
            raise

def main():
    """Main function to run the Grist update from file"""
    try:
        updater = GristBankUpdater()
        updater.update_grist_from_file()
    except Exception as e:
        logger.error(f"Grist update script failed: {e}")
        return 1
    return 0

if __name__ == "__main__":
    exit(main())
