import gspread
import requests
import json
from datetime import datetime, timedelta
import time
import logging
from typing import List, Dict, Any, Optional
import os
from google.oauth2.service_account import Credentials

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BankTransactionSyncer:
    def __init__(self, 
                 gsheet_credentials_path: str,
                 gsheet_id: str,
                 worksheet_name: str,
                 grist_api_key: str,
                 grist_doc_id: str,
                 grist_table_name: str):
        """
        Initialize the syncer with Google Sheets and Grist configurations
        
        Args:
            gsheet_credentials_path: Path to Google service account JSON file
            gsheet_id: Google Sheets document ID
            worksheet_name: Name of the worksheet containing transactions
            grist_api_key: Grist API key
            grist_doc_id: Grist document ID
            grist_table_name: Name of the Grist table to update
        """
        self.gsheet_id = gsheet_id
        self.worksheet_name = worksheet_name
        self.grist_api_key = grist_api_key
        self.grist_doc_id = grist_doc_id
        self.grist_table_name = grist_table_name
        
        # Initialize Google Sheets client
        self.gc = self._setup_google_sheets(gsheet_credentials_path)
        
        # Grist API base URL
        self.grist_base_url = f"https://docs.getgrist.com/api/docs/{grist_doc_id}/tables/{grist_table_name}"
        
        # Headers for Grist API
        self.grist_headers = {
            "Authorization": f"Bearer {grist_api_key}",
            "Content-Type": "application/json"
        }
    
    def _setup_google_sheets(self, credentials_path: str) -> gspread.Client:
        """Setup Google Sheets client with service account credentials"""
        try:
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
            
            creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
            return gspread.authorize(creds)
        except Exception as e:
            logger.error(f"Failed to setup Google Sheets client: {e}")
            raise
    
    def get_sheet_data(self) -> List[Dict[str, Any]]:
        """Read data from Google Sheets"""
        try:
            sheet = self.gc.open_by_key(self.gsheet_id)
            worksheet = sheet.worksheet(self.worksheet_name)
            
            # Get all records as dictionaries
            records = worksheet.get_all_records()
            
            # Clean and validate records
            cleaned_records = []
            for record in records:
                # Skip empty rows
                if not any(str(value).strip() for value in record.values()):
                    continue
                
                # Convert empty strings to None
                cleaned_record = {k: (v if str(v).strip() else None) for k, v in record.items()}
                cleaned_records.append(cleaned_record)
            
            logger.info(f"Retrieved {len(cleaned_records)} records from Google Sheets")
            return cleaned_records
            
        except Exception as e:
            logger.error(f"Failed to read Google Sheets data: {e}")
            raise
    
    def get_grist_data(self) -> List[Dict[str, Any]]:
        """Get existing data from Grist table"""
        try:
            response = requests.get(
                f"{self.grist_base_url}/records",
                headers=self.grist_headers
            )
            response.raise_for_status()
            
            data = response.json()
            records = data.get('records', [])
            
            logger.info(f"Retrieved {len(records)} records from Grist")
            return records
            
        except Exception as e:
            logger.error(f"Failed to get Grist data: {e}")
            raise
    
    def create_grist_record(self, record_data: Dict[str, Any]) -> bool:
        """Create a new record in Grist"""
        try:
            payload = {
                "records": [
                    {
                        "fields": record_data
                    }
                ]
            }
            
            response = requests.post(
                f"{self.grist_base_url}/records",
                headers=self.grist_headers,
                json=payload
            )
            response.raise_for_status()
            
            logger.info(f"Created record in Grist: {record_data}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create Grist record: {e}")
            return False
    
    def update_grist_record(self, record_id: int, record_data: Dict[str, Any]) -> bool:
        """Update an existing record in Grist"""
        try:
            payload = {
                "records": [
                    {
                        "id": record_id,
                        "fields": record_data
                    }
                ]
            }
            
            response = requests.patch(
                f"{self.grist_base_url}/records",
                headers=self.grist_headers,
                json=payload
            )
            response.raise_for_status()
            
            logger.info(f"Updated record {record_id} in Grist")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update Grist record {record_id}: {e}")
            return False
    
    def find_matching_record(self, sheet_record: Dict[str, Any], grist_records: List[Dict[str, Any]], 
                           key_fields: List[str]) -> Optional[Dict[str, Any]]:
        """Find a matching record in Grist based on key fields"""
        for grist_record in grist_records:
            match = True
            for key_field in key_fields:
                sheet_value = sheet_record.get(key_field)
                grist_value = grist_record.get('fields', {}).get(key_field)
                
                if str(sheet_value) != str(grist_value):
                    match = False
                    break
            
            if match:
                return grist_record
        
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
                amount_str = amount_value.replace('$', '').replace(',', '').strip()
                return float(amount_str)
            
            return float(amount_value)
            
        except Exception as e:
            logger.warning(f"Failed to normalize amount {amount_value}: {e}")
            return None
    
    def sync_transactions(self, key_fields: List[str] = ['Date', 'Amount', 'Description']):
        """
        Sync transactions from Google Sheets to Grist
        
        Args:
            key_fields: Fields to use for matching existing records
        """
        try:
            logger.info("Starting transaction sync...")
            
            # Get data from both sources
            sheet_data = self.get_sheet_data()
            grist_data = self.get_grist_data()
            
            if not sheet_data:
                logger.warning("No data found in Google Sheets")
                return
            
            created_count = 0
            updated_count = 0
            error_count = 0
            
            for sheet_record in sheet_data:
                try:
                    # Normalize data
                    normalized_record = {}
                    for key, value in sheet_record.items():
                        if 'date' in key.lower():
                            normalized_record[key] = self.normalize_date(value)
                        elif 'amount' in key.lower():
                            normalized_record[key] = self.normalize_amount(value)
                        else:
                            normalized_record[key] = value
                    
                    # Find matching record in Grist
                    matching_record = self.find_matching_record(
                        normalized_record, grist_data, key_fields
                    )
                    
                    if matching_record:
                        # Update existing record
                        record_id = matching_record['id']
                        if self.update_grist_record(record_id, normalized_record):
                            updated_count += 1
                        else:
                            error_count += 1
                    else:
                        # Create new record
                        if self.create_grist_record(normalized_record):
                            created_count += 1
                        else:
                            error_count += 1
                    
                    # Add small delay to avoid rate limiting
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Error processing record {sheet_record}: {e}")
                    error_count += 1
                    continue
            
            logger.info(f"Sync completed - Created: {created_count}, Updated: {updated_count}, Errors: {error_count}")
            
        except Exception as e:
            logger.error(f"Failed to sync transactions: {e}")
            raise

def main():
    """Main function to run the sync"""
    # Configuration - Update these values
    config = {
        'gsheet_credentials_path': 'path/to/your/service-account-credentials.json',
        'gsheet_id': 'your-google-sheets-id',
        'worksheet_name': 'Sheet1',  # or your worksheet name
        'grist_api_key': 'your-grist-api-key',
        'grist_doc_id': 'your-grist-document-id',
        'grist_table_name': 'Transactions'  # or your table name
    }
    
    # Key fields to match records (customize based on your data structure)
    key_fields = ['Date', 'Amount', 'Description']
    
    try:
        # Initialize syncer
        syncer = BankTransactionSyncer(**config)
        
        # Run sync
        syncer.sync_transactions(key_fields)
        
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())