import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_grist_connection():
    """Test Grist API connection for self-hosted Grist"""
    
    # Load from environment variables
    GRIST_BASE_HOST = os.getenv('GRIST_BASE_HOST', 'http://safcost.duckdns.org:8484')
    GRIST_API_KEY = os.getenv('GRIST_API_KEY')
    GRIST_DOC_ID = os.getenv('GRIST_DOC_ID')
    GRIST_TABLE_NAME = os.getenv('GRIST_TABLE_NAME')
    
    if not all([GRIST_API_KEY, GRIST_DOC_ID, GRIST_TABLE_NAME]):
        print("✗ Missing required environment variables. Please check your .env file.")
        print("Required: GRIST_API_KEY, GRIST_DOC_ID, GRIST_TABLE_NAME")
        return False
    
    # Grist API URL (self-hosted)
    base_url = f"{GRIST_BASE_HOST}/api/docs/{GRIST_DOC_ID}"
    
    # Headers
    headers = {
        "Authorization": f"Bearer {GRIST_API_KEY}",
        "Content-Type": "application/json"
    }
    
    print("Testing Self-hosted Grist API connection...")
    print(f"Grist Server: {GRIST_BASE_HOST}")
    print(f"Document ID: {GRIST_DOC_ID}")
    print(f"Table Name: {GRIST_TABLE_NAME}")
    print(f"API Key: {GRIST_API_KEY[:10]}..." if GRIST_API_KEY else "No API key provided")
    print("-" * 50)
    
    # Test 1: Check if document exists
    print("Test 1: Checking document access...")
    try:
        response = requests.get(f"{base_url}", headers=headers)
        if response.status_code == 200:
            print("✓ Document access successful")
            doc_info = response.json()
            print(f"  Document name: {doc_info.get('name', 'Unknown')}")
        else:
            print(f"✗ Document access failed: {response.status_code}")
            print(f"  Error: {response.text}")
            return False
    except Exception as e:
        print(f"✗ Document access error: {e}")
        return False
    
    # Test 2: List all tables
    print("\nTest 2: Listing all tables...")
    try:
        response = requests.get(f"{base_url}/tables", headers=headers)
        if response.status_code == 200:
            tables = response.json()
            print("✓ Tables retrieved successfully")
            table_names = [table['id'] for table in tables.get('tables', [])]
            print(f"  Available tables: {table_names}")
            
            if GRIST_TABLE_NAME not in table_names:
                print(f"✗ Table '{GRIST_TABLE_NAME}' not found!")
                print(f"  Available tables: {table_names}")
                return False
            else:
                print(f"✓ Table '{GRIST_TABLE_NAME}' found")
        else:
            print(f"✗ Failed to list tables: {response.status_code}")
            print(f"  Error: {response.text}")
            return False
    except Exception as e:
        print(f"✗ Tables listing error: {e}")
        return False
    
    # Test 3: Check table structure
    print(f"\nTest 3: Checking table '{GRIST_TABLE_NAME}' structure...")
    try:
        response = requests.get(f"{base_url}/tables/{GRIST_TABLE_NAME}/columns", headers=headers)
        if response.status_code == 200:
            columns = response.json()
            print("✓ Table structure retrieved successfully")
            column_names = [col['id'] for col in columns.get('columns', [])]
            print(f"  Available columns: {column_names}")
            
            # Check for expected columns
            expected_columns = ['Transaction Date', 'Transaction Description', 'Transaction Amount', 'Bank', 'Reference No.', 'Value Date', 'Running Balance']
            missing_columns = [col for col in expected_columns if col not in column_names]
            if missing_columns:
                print(f"⚠ Missing expected columns: {missing_columns}")
            else:
                print("✓ All expected columns found")
        else:
            print(f"✗ Failed to get table structure: {response.status_code}")
            print(f"  Error: {response.text}")
            return False
    except Exception as e:
        print(f"✗ Table structure error: {e}")
        return False
    
    # Test 4: Try to read records
    print(f"\nTest 4: Reading records from '{GRIST_TABLE_NAME}'...")
    try:
        response = requests.get(f"{base_url}/tables/{GRIST_TABLE_NAME}/records?limit=1", headers=headers)
        if response.status_code == 200:
            records = response.json()
            record_count = len(records.get('records', []))
            print(f"✓ Records retrieved successfully")
            print(f"  Sample records found: {record_count}")
            
            if record_count > 0:
                sample_record = records['records'][0]
                print(f"  Sample record fields: {list(sample_record.get('fields', {}).keys())}")
        else:
            print(f"✗ Failed to read records: {response.status_code}")
            print(f"  Error: {response.text}")
            return False
    except Exception as e:
        print(f"✗ Records reading error: {e}")
        return False
    
    print("\n" + "="*50)
    print("✓ All tests passed! Your Grist connection is working.")
    return True

if __name__ == "__main__":
    test_grist_connection()