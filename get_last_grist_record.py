import os
from dotenv import load_dotenv
from gristbankupdater import GristBankUpdater, logger

# Load environment variables
load_dotenv()

def main():
    """
    Script to fetch and print the last record from the Grist table.
    """
    try:
        updater = GristBankUpdater()
        
        logger.info("Fetching recent records from Grist to identify the last one...")
        recent_records = updater.get_recent_grist_records(limit=1) # Only need the very last one

        if recent_records:
            last_record = recent_records[0]
            logger.info("\n--- Last Record in Grist ---")
            for key, value in last_record.items():
                logger.info(f"{key}: {value}")
            logger.info("----------------------------")
        else:
            logger.info("No records found in the Grist table.")
            
    except ValueError as ve:
        logger.error(f"Configuration error: {ve}")
        logger.error("Please ensure GRIST_API_KEY, GRIST_DOC_ID, and GRIST_TABLE_NAME are set in your .env file.")
        return 1
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return 1
    return 0

if __name__ == "__main__":
    exit(main())
