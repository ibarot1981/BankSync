import os
import csv
import logging
from datetime import datetime, timedelta
import requests
import json
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
LOG_FILE = os.getenv('LOG_FILE', 'uploadToGrist.log') # Changed log file name
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
LOG_MAX_BYTES = int(os.getenv('LOG_MAX_BYTES', 5 * 1024 * 1024))
LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', 5))

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

# Ensure handlers are not duplicated if this script is run multiple times
if not logger.handlers:
    from logging.handlers import RotatingFileHandler
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT
    )
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)

class GristCSVUploader:
    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        
        # Grist configurations from environment variables
        self.grist_api_key = os.getenv('GRIST_API_KEY')
        self.grist_doc_id = os.getenv('GRIST_DOC_ID')
        self.grist_table_name = os.getenv('GRIST_TABLE_NAME')
        self.grist_base_host = os.getenv('GRIST_BASE_HOST', 'http://safcost.duckdns.org:8484')
        self.archive_dir = os.getenv('ARCHIVE_DIR', 'archive')
        self.upload_grist_dir = os.getenv('UPLOAD_GRIST_DIR', 'UploadGrist') # Ensure this is defined

        # Validate required environment variables
        required_vars = {
            'GRIST_API_KEY': self.grist_api_key,
            'GRIST_DOC_ID': self.grist_doc_id,
            'GRIST_TABLE_NAME': self.grist_table_name,
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        # Ensure archive directory exists
        os.makedirs(self.archive_dir, exist_ok=True)
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

    def _parse_unix_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse Unix timestamp string to datetime object"""
        try:
            timestamp = int(timestamp_str)
            if 0 <= timestamp <= 4102444800: # Roughly 1970 to 2100
                parsed_dt = datetime.fromtimestamp(timestamp)
                logger.debug(f"Successfully converted Unix timestamp {timestamp} to datetime: {parsed_dt.strftime('%d/%m/%Y %H:%M:%S')}")
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
        
        if cleaned_date_string.isdigit():
            return self._parse_unix_timestamp(cleaned_date_string)

        if 'am' in cleaned_date_string.lower():
            cleaned_date_string = cleaned_date_string.replace('am', 'AM').replace('AM', 'AM')
        if 'pm' in cleaned_date_string.lower():
            cleaned_date_string = cleaned_date_string.replace('pm', 'PM').replace('PM', 'PM')

        mm_dd_yyyy_formats = [
            '%m-%d-%Y %I:%M:%S %p', '%m-%d-%Y %H:%M:%S', '%m-%d-%Y',
            '%m/%d/%Y %H:%M:%S', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y'
        ]
        dd_mm_yyyy_formats = [
            '%d-%m-%Y %H:%M:%S', '%d/%m/%Y %H:%M:%S', 
            '%d-%m-%Y %I:%M:%S %p', '%d-%m-%Y %I:%M %p',
            '%d-%m-%Y', '%d/%m/%Y',
            '%d/%m/%y' # Added for DD/MM/YY format like 11/7/25
        ]
        yyyy_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']

        parsed_dt = None

        if bank_name and bank_name.upper() == 'ICICI':
            format_priority = mm_dd_yyyy_formats + dd_mm_yyyy_formats
        else:
            format_priority = dd_mm_yyyy_formats + mm_dd_yyyy_formats

        for fmt in format_priority:
            try:
                parsed_dt = datetime.strptime(cleaned_date_string, fmt)
                logger.debug(f"Successfully parsed '{cleaned_date_string}' using format '{fmt}'")
                break
            except ValueError:
                continue

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
        """Parse date value into a datetime object."""
        if not date_value:
            return None
        
        try:
            if isinstance(date_value, datetime):
                return date_value
            elif isinstance(date_value, (int, float)):
                parsed_dt = self._parse_unix_timestamp(str(int(date_value)))
                if parsed_dt:
                    logger.debug(f"Converted numeric timestamp {date_value} to datetime: {parsed_dt.strftime('%d/%m/%Y %H:%M:%S')}")
                return parsed_dt
            elif isinstance(date_value, str):
                logger.debug(f"Parsing date string: '{date_value}' for bank: {bank_name}")
                parsed_dt = self._parse_date_string(date_value, bank_name)
                if parsed_dt:
                    logger.debug(f"Converted date string '{date_value}' to datetime: {parsed_dt.strftime('%d/%m/%Y %H:%M:%S')}")
                return parsed_dt
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

    def normalize_integer(self, int_value: Any) -> Optional[int]:
        """Normalize integer values to int"""
        if not int_value:
            return None
        
        try:
            if isinstance(int_value, str):
                int_str = int_value.strip()
                if not int_str:
                    return None
                return int(int_str)
            
            return int(int_value)
            
        except Exception as e:
            logger.warning(f"Failed to normalize integer {int_value}: {e}")
            return None

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

    def prepare_grist_record(self, sheet_record: Dict[str, Any], grist_structure: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a record for Grist based on the table structure"""
        normalized_record = {}
        
        # Define the mapping from Google Sheets field names to Grist field names
        google_to_grist_map = {
            'Transaction Date': 'Transaction_Date',
            'Transaction Description': 'Transaction_Description',
            'Transaction Amount': 'Transaction_Amount',
            'Reference No.': 'Reference_No',
            'Value Date': 'Value_Date',
            'GSheets_RowNum': 'GSheets_RowNum'
        }

        bank_name = sheet_record.get('Bank')
        logger.info(f"--- prepare_grist_record ---")
        logger.info(f"Processing record for Bank: {bank_name}")
        logger.info(f"Original sheet_record: {sheet_record}")

        for field_name, field_value in sheet_record.items():
            if field_value is None or field_value == '':
                continue
                
            grist_field = None
            if field_name in google_to_grist_map:
                grist_field = google_to_grist_map[field_name]
            else:
                for grist_col_id, grist_col_info in grist_structure.items():
                    if grist_col_info['label'] == field_name or grist_col_id == field_name:
                        grist_field = grist_col_id
                        break
            
            if not grist_field:
                logger.warning(f"Field '{field_name}' not found in Grist structure or explicit mapping, skipping")
                continue
            
            if grist_field not in grist_structure:
                logger.warning(f"Mapped Grist field '{grist_field}' for Google Sheets field '{field_name}' not found in Grist structure, skipping")
                continue

            grist_type = grist_structure[grist_field]['type']
            logger.debug(f"Field '{field_name}' -> Grist field '{grist_field}' (type: {grist_type})")
            
            normalized_value = None
            if grist_type == 'Date' or field_name in ['Transaction Date', 'Value Date']:
                dt_obj = self.normalize_date(field_value, bank_name)
                if dt_obj:
                    if grist_field == 'Value_Date': # Specific handling for Value_Date as per user feedback
                        normalized_value = dt_obj.strftime('%Y-%m-%d') # Format as YYYY-MM-DD
                    else:
                        normalized_value = dt_obj.strftime('%Y-%m-%d %H:%M:%S') # Default for other date/datetime fields
                logger.info(f"Date field '{field_name}': {field_value} -> {normalized_value} (Bank: {bank_name})")
            elif grist_type == 'Numeric':
                normalized_value = self.normalize_amount(field_value)
            elif grist_type == 'Int' or field_name == 'GSheets_RowNum':
                normalized_value = self.normalize_integer(field_value)
                logger.info(f"Integer field '{field_name}': {field_value} -> {normalized_value}")
            else:
                normalized_value = str(field_value) if field_value else None
            
            if normalized_value is not None:
                normalized_record[grist_field] = normalized_value
        
        logger.info(f"Final normalized record: {normalized_record}")
        return normalized_record
    
    def create_grist_records_bulk(self, records_data: List[Dict[str, Any]]) -> bool:
        """Create multiple new records in Grist with enhanced error handling"""
        if not records_data:
            logger.info("No records to insert in bulk.")
            return True

        try:
            payload = {
                "records": [
                    {"fields": record} for record in records_data
                ]
            }
            
            logger.debug(f"Sending bulk payload to Grist: {json.dumps(payload, indent=2)}")
            
            response = requests.post(
                f"{self.grist_base_url}/records",
                headers=self.grist_headers,
                json=payload
            )
            
            if response.status_code != 200:
                logger.error(f"Grist API error during bulk insert: {response.status_code}")
                logger.error(f"Response headers: {response.headers}")
                logger.error(f"Response content: {response.text}")
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
            
            response = requests.get(
                f"{self.grist_base_host}/api/docs/{self.grist_doc_id}",
                headers=self.grist_headers
            )
            
            if response.status_code == 200:
                logger.debug("✓ Grist connection successful")
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

    def read_csv_records(self) -> list[dict]:
        """Reads records from the specified CSV file."""
        records = []
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    records.append(row)
            logger.info(f"Successfully read {len(records)} records from {self.csv_file_path}")
        except FileNotFoundError:
            logger.error(f"CSV file not found: {self.csv_file_path}")
            raise
        except Exception as e:
            logger.error(f"Failed to read records from CSV file {self.csv_file_path}: {e}")
            raise
        return records

    def upload_records_to_grist(self, records: list[dict]) -> bool:
        """
        Uploads records to Grist using the GristBankUpdater's bulk insert method.
        The CSV records need to be mapped to Grist's expected field names.
        """
        if not records:
            logger.info("No records to upload to Grist.")
            return True

        logger.info(f"Preparing {len(records)} records for Grist upload.")
        
        grist_structure = self.get_grist_table_structure()
        if not grist_structure:
            logger.error("Failed to retrieve Grist table structure. Cannot proceed with upload.")
            return False

        grist_formatted_records = []
        for i, record in enumerate(records):
            try:
                if 'Bank' not in record:
                    logger.warning(f"Record {i+1} missing 'Bank' field. Date normalization might be affected.")
                    record['Bank'] = 'UNKNOWN'

                prepared_record = self.prepare_grist_record(record, grist_structure)
                if prepared_record:
                    grist_formatted_records.append(prepared_record)
                else:
                    logger.warning(f"Skipping record {i+1} due to preparation issues: {record}")
            except Exception as e:
                logger.error(f"Error preparing record {i+1} for Grist: {record} - {e}")
                continue

        if not grist_formatted_records:
            logger.warning("No records were successfully prepared for Grist upload.")
            return False

        logger.info(f"Attempting to bulk insert {len(grist_formatted_records)} records into Grist.")
        success = self.create_grist_records_bulk(grist_formatted_records)
        if success:
            logger.info("Grist bulk upload completed successfully.")
        else:
            logger.error("Grist bulk upload failed.")
        return success

    def archive_csv_file(self) -> bool:
        """Moves the processed CSV file to the archive directory and renames it."""
        if not os.path.exists(self.csv_file_path):
            logger.warning(f"CSV file not found for archiving: {self.csv_file_path}")
            return False

        original_filename = os.path.basename(self.csv_file_path)
        name_without_ext, ext = os.path.splitext(original_filename)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        uploaded_filename = f"Uploaded-{name_without_ext}_{timestamp}{ext}"
        
        os.makedirs(self.archive_dir, exist_ok=True)
        
        destination_path = os.path.join(self.archive_dir, uploaded_filename)

        try:
            os.rename(self.csv_file_path, destination_path)
            logger.info(f"Successfully moved '{original_filename}' to '{destination_path}'")
            return True
        except Exception as e:
            logger.error(f"Failed to move and rename CSV file '{self.csv_file_path}' to '{destination_path}': {e}")
            return False

def main():
    """Main function to run the Grist CSV upload process."""
    
    upload_grist_dir = os.getenv('UPLOAD_GRIST_DIR', 'UploadGrist')
    
    csv_files = [f for f in os.listdir(upload_grist_dir) if f.endswith('.csv')]
    if not csv_files:
        logger.info(f"No CSV files found in '{upload_grist_dir}'. Nothing to upload.")
        return 0

    csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(upload_grist_dir, x)), reverse=True)
    latest_csv_file_name = csv_files[0]
    csv_file_path = os.path.join(upload_grist_dir, latest_csv_file_name)

    try:
        uploader = GristCSVUploader(csv_file_path)
        
        if not uploader.test_grist_connection():
            logger.error("Grist connection failed. Aborting upload.")
            return 1

        records_to_upload = uploader.read_csv_records()
        
        if records_to_upload:
            if uploader.upload_records_to_grist(records_to_upload):
                logger.info("CSV records successfully uploaded to Grist.")
                if uploader.archive_csv_file():
                    logger.info("CSV file archived successfully.")
                else:
                    logger.error("Failed to archive CSV file.")
                    return 1
            else:
                logger.error("Failed to upload CSV records to Grist.")
                return 1
        else:
            logger.info(f"No records found in CSV '{csv_file_path}' to upload. Archiving it if it exists.")
            if os.path.exists(csv_file_path):
                uploader.archive_csv_file()

    except Exception as e:
        logger.critical(f"An unhandled error occurred during the Grist CSV upload process: {e}", exc_info=True)
        return 1
    return 0

if __name__ == "__main__":
    exit(main())