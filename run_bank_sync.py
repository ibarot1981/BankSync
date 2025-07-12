import os
import subprocess
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
LOG_FILE = os.getenv('LOG_FILE', 'run_bank_sync.log')
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

def _get_current_date_filename() -> str:
    """Generate filename based on current date in ddmmyy.txt format"""
    return datetime.now().strftime("%d%m%y") + ".txt"

def run_script(script_name: str) -> bool:
    """Helper function to run a Python script using the virtual environment's python"""
    venv_python_path = os.path.join(os.path.dirname(__file__), 'venv', 'Scripts', 'python.exe')
    
    logger.info(f"Executing {script_name}...")
    try:
        result = subprocess.run([venv_python_path, script_name], check=True, capture_output=True, text=True)
        logger.info(f"{script_name} completed.")
        logger.debug(f"{script_name} stdout:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"{script_name} stderr:\n{result.stderr}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing {script_name}: {e}")
        logger.error(f"{script_name} stdout:\n{e.stdout}")
        logger.error(f"{script_name} stderr:\n{e.stderr}")
        return False
    except FileNotFoundError:
        logger.error(f"Python executable not found at {venv_python_path}. Ensure virtual environment is set up correctly.")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while executing {script_name}: {e}")
        return False

def main():
    data_dir = os.getenv('DATA_DIR', 'data')
    current_date_filename = _get_current_date_filename()
    file_path = os.path.join(data_dir, current_date_filename)

    logger.info(f"Starting BankSync wrapper script.")

    if os.path.exists(file_path):
        logger.info(f"Data file '{current_date_filename}' already exists. Proceeding with Grist record creation.")
        if not run_script('createGristRecords.py'):
            logger.error("createGristRecords.py failed. Exiting wrapper script.")
            return 1
    else:
        logger.info(f"Data file '{current_date_filename}' does not exist. Fetching data from Google Sheets and then creating Grist records.")
        if not run_script('BankSync.py'):
            logger.error("BankSync.py failed. Exiting wrapper script.")
            return 1
        
        # Check if BankSync.py successfully created the file
        if not os.path.exists(file_path):
            logger.error(f"BankSync.py did not create the expected file: {file_path}. Exiting wrapper script.")
            return 1
        
        if not run_script('createGristRecords.py'):
            logger.error("createGristRecords.py failed. Exiting wrapper script.")
            return 1
    
    # After createGristRecords.py, run uploadToGrist.py
    logger.info("createGristRecords.py completed. Now running uploadToGrist.py...")
    if not run_script('uploadToGrist.py'):
        logger.error("uploadToGrist.py failed. Exiting wrapper script.")
        return 1

    logger.info("BankSync wrapper script completed successfully.")
    return 0

if __name__ == "__main__":
    exit(main())
