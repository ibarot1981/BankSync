import os
import json
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
from typing import List, Dict, Any, Optional
import csv
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()

# Configure logging
LOG_FILE = os.getenv('LOG_FILE', 'createGristRecords.log')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
LOG_MAX_BYTES = int(os.getenv('LOG_MAX_BYTES', 5 * 1024 * 1024))  # Default to 5 MB
LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', 5)) # Default to 5 backup files

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Force DEBUG level for debugging

# Create a rotating file handler
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=LOG_MAX_BYTES,
    backupCount=LOG_BACKUP_COUNT
)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
file_handler.setLevel(logging.DEBUG) # Ensure file handler also logs DEBUG
logger.addHandler(file_handler)

# Also add a console handler for immediate feedback
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
console_handler.setLevel(logging.DEBUG) # Ensure console handler also logs DEBUG
logger.addHandler(console_handler)

class GristRecordCreator:
    def __init__(self):
        """
        Initialize the creator with Grist configurations from environment variables
        and set up directories.
        """
        self.grist_api_key = os.getenv('GRIST_API_KEY')
        self.grist_doc_id = os.getenv('GRIST_DOC_ID')
        self.grist_table_name = os.getenv('GRIST_TABLE_NAME')
        self.grist_base_host = os.getenv('GRIST_BASE_HOST', 'http://safcost.duckdns.org:8484')
        self.data_dir = os.getenv('DATA_DIR', 'data')
        self.upload_grist_dir = os.getenv('UPLOAD_GRIST_DIR', 'UploadGrist') # New directory for CSV output
        self.archive_dir = os.getenv('ARCHIVE_DIR', 'archive') # For archiving processed .txt files

        # Validate required environment variables
        required_vars = {
            'GRIST_API_KEY': self.grist_api_key,
            'GRIST_DOC_ID': self.grist_doc_id,
            'GRIST_TABLE_NAME': self.grist_table_name,
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        # Ensure data, upload and archive directories exist
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.upload_grist_dir, exist_ok=True)
        os.makedirs(self.archive_dir, exist_ok=True)
        logger.info(f"Data directory set to: {self.data_dir}")
        logger.info(f"Upload Grist directory set to: {self.upload_grist_dir}")
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

    def _format_datetime_for_output(self, dt_obj: datetime) -> str:
        """Formats a datetime object to MM/DD/YYYY HH:MM:SS string."""
        return dt_obj.strftime("%m/%d/%Y %H:%M:%S")

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

    def _parse_unix_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse Unix timestamp string to datetime object"""
        try:
            timestamp = int(timestamp_str)
            # More lenient timestamp range - roughly 1970 to 2100
            if 0 <= timestamp <= 4102444800:
                parsed_dt = datetime.fromtimestamp(timestamp)
                logger.debug(f"Successfully converted Unix timestamp {timestamp} to datetime: {parsed_dt}")
                return parsed_dt
            else:
                logger.warning(f"Unix timestamp {timestamp} is outside reasonable range (1970-2100)")
                return None
        except (ValueError, OSError) as e:
            logger.error(f"Error parsing '{timestamp_str}' as Unix timestamp: {e}")
            return None

    def _parse_date_string(self, date_string: str, bank_name: Optional[str] = None) -> Optional[datetime]:
        """Parse various date string formats to datetime object"""
        if not date_string:
            return None
        
        cleaned_date_string = str(date_string).strip()
        
        # Try to parse as Unix timestamp first if it's a digit string
        if cleaned_date_string.isdigit():
            return self._parse_unix_timestamp(cleaned_date_string)

        # If not a digit string, proceed with string date parsing
        logger.debug(f"Attempting string date parsing for: {cleaned_date_string}")

        # Normalize AM/PM case
        if 'am' in cleaned_date_string.lower():
            cleaned_date_string = cleaned_date_string.replace('am', 'AM').replace('AM', 'AM')
        if 'pm' in cleaned_date_string.lower():
            cleaned_date_string = cleaned_date_string.replace('pm', 'PM').replace('PM', 'PM')

        # Define date format patterns
        mm_dd_yyyy_formats = [
            '%m-%d-%Y %I:%M:%S %p', '%m-%d-%Y %H:%M:%S', '%m-%d-%Y',
            '%m/%d/%Y %H:%M:%S', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y'
        ]
        dd_mm_yyyy_formats = [
            '%d-%m-%Y %H:%M:%S', '%d/%m/%Y %H:%M:%S', 
            '%d-%m-%Y %I:%M:%S %p', '%d-%m-%Y %I:%M %p',
            '%d-%m-%Y', '%d/%m/%Y'
        ]
        yyyy_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']

        parsed_dt = None

        # Choose format priority based on bank
        if bank_name and bank_name.upper() == 'ICICI':
            # For ICICI, prioritize MM/DD/YYYY formats
            format_priority = mm_dd_yyyy_formats + dd_mm_yyyy_formats
        else:
            # For non-ICICI, prioritize DD/MM/YYYY formats
            format_priority = dd_mm_yyyy_formats + mm_dd_yyyy_formats

        # Try the prioritized formats first
        for fmt in format_priority:
            try:
                parsed_dt = datetime.strptime(cleaned_date_string, fmt)
                logger.debug(f"Successfully parsed '{cleaned_date_string}' using format '{fmt}'")
                break
            except ValueError:
                continue

        # Try YYYY formats as last resort
        if not parsed_dt:
            for fmt in yyyy_formats:
                try:
                    parsed_dt = datetime.strptime(cleaned_date_string, fmt)
                    logger.debug(f"Successfully parsed '{cleaned_date_string}' using format '{fmt}'")
                    break
                except ValueError:
                    continue

        if not parsed_dt:
            logger.warning(f"Could not parse date string: {date_string}")
        
        return parsed_dt
    
    def normalize_date(self, date_value: Any, bank_name: Optional[str] = None) -> Optional[datetime]:
        """Parse date value into a datetime object"""
        if not date_value:
            return None
        
        try:
            if isinstance(date_value, datetime):
                return date_value
            elif isinstance(date_value, (int, float)):
                # Handle numeric timestamps
                return self._parse_unix_timestamp(str(int(date_value)))
            elif isinstance(date_value, str):
                logger.debug(f"Parsing date string: '{date_value}' for bank: {bank_name}")
                return self._parse_date_string(date_value, bank_name)
            else:
                logger.warning(f"Unsupported date value type: {type(date_value)} - {date_value}")
                return None
            
        except Exception as e:
            logger.warning(f"Failed to normalize date {date_value}: {e}")
            return None
    
    def normalize_amount(self, amount_value: Any) -> Optional[float]:
        """Normalize amount values to float"""
        if not amount_value:
            return None
        
        try:
            if isinstance(amount_value, str):
                amount_str = amount_value.replace('$', '').replace(',', '').replace('₹', '').strip()
                if not amount_str:
                    return None
                return float(amount_str)
            
            return float(amount_value)
            
        except Exception as e:
            logger.warning(f"Failed to normalize amount {amount_value}: {e}")
            return None

    def _record_matches(self, file_record: Dict[str, Any], grist_record: Dict[str, Any]) -> bool:
        """Compare a record from the file with a record from Grist based on key fields"""
        bank_name = file_record.get('Bank')
        
        file_date = self.normalize_date(file_record.get('Transaction Date'), bank_name)
        file_desc = file_record.get('Transaction Description')
        file_amount = self.normalize_amount(file_record.get('Transaction Amount'))

        grist_date = self.normalize_date(grist_record.get('Transaction_Date'), None)
        grist_desc = grist_record.get('Transaction_Description')
        grist_amount = self.normalize_amount(grist_record.get('Transaction_Amount'))
        
        if any(val is None for val in [file_date, file_desc, file_amount, grist_date, grist_desc, grist_amount]):
            logger.debug(f"Skipping record comparison due to missing/invalid data")
            return False

        matches = (file_date == grist_date and
                  file_desc == grist_desc and
                  file_amount == grist_amount)
        
        if matches:
            logger.debug(f"Record match found: {file_desc} - {file_date} - {file_amount}")
        
        return matches

    def get_last_processed_datetime_and_records(self, limit: int = 500):
        """
        Get the last processed datetime and ALL records that share this datetime from Grist.
        Returns (last_datetime_obj, list_of_records_with_that_datetime)
        """
        try:
            url = f"{self.grist_base_url}/records?sort=-Transaction_Date&limit={limit}"
            logger.debug(f"Fetching records from Grist URL: {url}")
            
            response = requests.get(url, headers=self.grist_headers)
            
            logger.info(f"Grist API Response Status Code: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"Grist API returned error: {response.status_code} - {response.text}")
                return None, []

            response.raise_for_status()
            
            data = response.json()
            all_records = [rec.get('fields', {}) for rec in data.get('records', [])]
            
            if not all_records:
                logger.info("No existing records found in Grist")
                return None, []
            
            # Get the most recent record
            latest_record = all_records[0]
            latest_date_raw = latest_record.get('Transaction_Date')
            
            logger.debug(f"Latest Grist record raw date: {latest_date_raw}")
            
            if not latest_date_raw:
                logger.warning("Most recent record in Grist has no 'Transaction_Date' field")
                return None, []
            
            # Parse the latest datetime
            latest_datetime_obj = self.normalize_date(latest_date_raw, None)
            if not latest_datetime_obj:
                logger.error(f"Could not parse latest Grist transaction date: {latest_date_raw}")
                return None, []
            
            logger.info(f"Latest Grist transaction datetime: {latest_datetime_obj}")
            
            # Log sample record for debugging
            formatted_date = self._format_datetime_for_output(latest_datetime_obj)
            logger.debug(f"Sample Grist record: Transaction_Date='{formatted_date}', Description='{latest_record.get('Transaction_Description', 'N/A')}'")

            # Find all records with the same datetime
            records_with_same_datetime = []
            for record in all_records:
                record_date_raw = record.get('Transaction_Date')
                record_datetime_obj = self.normalize_date(record_date_raw, None)
                
                if record_datetime_obj and record_datetime_obj == latest_datetime_obj:
                    records_with_same_datetime.append(record)
                elif record_datetime_obj and record_datetime_obj < latest_datetime_obj:
                    # Records are sorted by date desc, so we can break here
                    break
            
            logger.info(f"Found {len(records_with_same_datetime)} records with the latest datetime")
            
            return latest_datetime_obj, records_with_same_datetime
            
        except requests.RequestException as e:
            logger.error(f"Network error while fetching from Grist: {e}")
            return None, []
        except Exception as e:
            logger.error(f"Unexpected error while fetching from Grist: {e}")
            return None, []

    def should_process_record(self, file_record: Dict[str, Any], file_dt_obj: Optional[datetime], last_dt_obj: Optional[datetime], last_datetime_records: List[Dict[str, Any]]) -> bool:
        """
        Determine if a file record should be processed based on datetime and duplicate checking.
        """
        if not file_dt_obj:
            logger.warning(f"File record has no valid transaction date, skipping: {file_record.get('Transaction Description', 'Unknown')}")
            return False
        
        # If no last datetime from Grist, process all records
        if not last_dt_obj:
            logger.debug(f"No last Grist date found. Processing record: {file_record.get('Transaction Date')}")
            return True
        
        try:
            # If file record is newer than last processed datetime, process it
            if file_dt_obj > last_dt_obj:
                logger.debug(f"Record is newer ({file_dt_obj} > {last_dt_obj}). Processing: {file_record.get('Transaction Description', 'Unknown')}")
                return True
            
            # If file record is older than last processed datetime, skip it
            if file_dt_obj < last_dt_obj:
                logger.debug(f"Record is older ({file_dt_obj} < {last_dt_obj}). Skipping: {file_record.get('Transaction Description', 'Unknown')}")
                return False
            
            # If file record has the same datetime, check for duplicates
            if file_dt_obj == last_dt_obj:
                logger.debug(f"Record has same datetime ({file_dt_obj}). Checking for duplicates: {file_record.get('Transaction Description', 'Unknown')}")
                
                for grist_record in last_datetime_records:
                    if self._record_matches(file_record, grist_record):
                        logger.debug(f"Duplicate found. Skipping: {file_record.get('Transaction Description', 'Unknown')}")
                        return False
                
                logger.debug(f"Same datetime but not duplicate. Processing: {file_record.get('Transaction Description', 'Unknown')}")
                return True
            
        except Exception as e:
            logger.error(f"Error comparing datetimes for record {file_record.get('Transaction Description', 'Unknown')}: {e}")
            return True  # Process it to be safe
        
        return True

    def archive_file(self, file_path: str):
        """Move the processed file to the archive directory"""
        if not os.path.exists(file_path):
            logger.warning(f"File to archive not found: {file_path}")
            return

        archive_path = os.path.join(self.archive_dir, os.path.basename(file_path))
        try:
            # If archive file already exists, add timestamp to make it unique
            if os.path.exists(archive_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                name, ext = os.path.splitext(archive_path)
                archive_path = f"{name}_{timestamp}{ext}"
            
            os.rename(file_path, archive_path)
            logger.info(f"Successfully archived {file_path} to {archive_path}")
        except Exception as e:
            logger.error(f"Failed to archive file {file_path}: {e}")

    def create_grist_records_from_file(self):
        """
        Read records from the daily TXT file, identify new transactions based on Grist's
        last transaction date, and save them to a CSV file in ./UploadGrist.
        """
        file_name = self._get_current_date_filename()
        file_path = os.path.join(self.data_dir, file_name)

        if not os.path.exists(file_path):
            logger.error(f"Data file not found: {file_path}")
            return

        try:
            logger.info(f"Starting record processing from file: {file_path}")

            # Fetch latest records from Grist
            last_grist_dt_obj, last_grist_records = self.get_last_processed_datetime_and_records(limit=500)
            
            # Read records from file
            file_records = self.read_records_from_file(file_path)
            if not file_records:
                logger.info("No records found in the data file")
                self.archive_file(file_path)
                return

            # Process records
            records_to_output = []
            for file_record in file_records:
                bank_name = file_record.get('Bank')
                file_dt_obj = self.normalize_date(file_record.get('Transaction Date'), bank_name)
                
                if self.should_process_record(file_record, file_dt_obj, last_grist_dt_obj, last_grist_records):
                    records_to_output.append(file_record)

            logger.info(f"Identified {len(records_to_output)} new records to save to CSV")

            # Save to CSV if we have records to output
            if records_to_output:
                input_file_base_name = os.path.splitext(os.path.basename(file_path))[0]
                output_csv_file_name = f"{input_file_base_name}.csv"
                output_csv_path = os.path.join(self.upload_grist_dir, output_csv_file_name)

                try:
                    # Get all unique field names
                    all_keys = set()
                    for record in records_to_output:
                        all_keys.update(record.keys())
                    fieldnames = sorted(list(all_keys))

                    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writeheader()
                        
                        for record in records_to_output:
                            row_to_write = {key: record.get(key, '') for key in fieldnames}
                            
                            # Format Transaction Date for output
                            if 'Transaction Date' in row_to_write:
                                original_date = record.get('Transaction Date')
                                if original_date:
                                    dt_obj = self.normalize_date(original_date, record.get('Bank'))
                                    if dt_obj:
                                        row_to_write['Transaction Date'] = self._format_datetime_for_output(dt_obj)

                            writer.writerow(row_to_write)
                    
                    logger.info(f"Successfully wrote {len(records_to_output)} records to {output_csv_path}")
                    
                except Exception as e:
                    logger.error(f"Failed to write CSV file {output_csv_path}: {e}")
            else:
                logger.info("No new records to output to CSV")
            
            # Archive the processed file
            self.archive_file(file_path)
            logger.info("Record processing completed successfully")

        except Exception as e:
            logger.error(f"Error in create_grist_records_from_file: {e}")
            raise

def main():
    """Main function to run the Grist record creation script"""
    try:
        creator = GristRecordCreator()
        creator.create_grist_records_from_file()
        logger.info("Script completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())