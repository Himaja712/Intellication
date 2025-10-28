from asyncio.log import logger
from logic import *
from apscheduler.schedulers.background import BackgroundScheduler
import time
import config

# Feteching required variables from the config file
CAMPAIGN_IDS = config.CAMPAIGN_IDS
url = config.URL 
status_completed = config.STATUS_COMPLETED
upload_excel_true = config.upload_excel_true
upload_excel_false = config.upload_excel_false
est = config.est

# logger called
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schedular declared
# Set EST timezone
scheduler = BackgroundScheduler(timezone=est)  

# Runs the status completed api for Expense, Mileage and Conference
# data processing and creates the Merged Sheet and AP-Reimbursement Upload CSV files
def run_script_completed():
    """Runs every hour to process completed data."""
    logger.info("Executing hourly scheduled task for completed data...")
    try:
        GOOGLE_DRIVE_FOLDER_ID2 = config.GOOGLE_DRIVE_FOLDER_ID_COMPLETED
        headers = {"accept": "application/json", "X-Authorization": X_Authorization}
        
        old_expense_url = f"{url}{CAMPAIGN_IDS['expense']['old']}{status_completed}"
        new_expense_url = f"{url}{CAMPAIGN_IDS['expense']['new']}{status_completed}"
        old_expense_data = fetch_api_data_completed(old_expense_url, headers)
        new_expense_data = fetch_api_data_completed(new_expense_url, headers)
        
        old_mileage_url = f"{url}{CAMPAIGN_IDS['mileage']['old']}{status_completed}"
        new_mileage_url = f"{url}{CAMPAIGN_IDS['mileage']['new']}{status_completed}"
        old_mileage_data = fetch_api_data_completed(old_mileage_url, headers)
        new_mileage_data = fetch_api_data_completed(new_mileage_url, headers)

        old_conference_url = f"{url}{CAMPAIGN_IDS['conference']['old']}{status_completed}"
        new_conference_url = f"{url}{CAMPAIGN_IDS['conference']['new']}{status_completed}"
        old_conference_data = fetch_api_data_completed(old_conference_url, headers)
        new_conference_data = fetch_api_data_completed(new_conference_url, headers)
        
        new_excel_name = f"Merged Data {get_current_timestamp()}.xlsx"
        new_csv_name = f"AP-Reimbursement Upload {get_current_timestamp()}.xlsx"
        result = process_and_upload_files(old_expense_data, new_expense_data, old_mileage_data, new_mileage_data, old_conference_data, new_conference_data, drive_service, GOOGLE_DRIVE_FOLDER_ID1, GOOGLE_DRIVE_FOLDER_ID2, FILE_NAME_TO_DOWNLOAD, X_Authorization, new_excel_name, new_csv_name, upload_excel_true)
        logger.info("Uploaded Files: " + str(result))
    except Exception as e:
        logger.error("Error in run_script_completed: " + str(e))

# Runs the status archived api for Expense, Mileage and Conference
# data processing and creates the Merged Sheet and AP-Reimbursement Upload CSV files
def run_script_archived():
    """Runs every Sunday at midnight EST to process archived data."""
    logger.info("Executing weekly scheduled task for archived data...")
    try:
        start_date_str, end_date_str = get_date_range_filename()
        GOOGLE_DRIVE_FOLDER_ID2 = config.GOOGLE_DRIVE_FOLDER_ID_ARCHIVED
        headers = {"accept": "application/json", "X-Authorization": X_Authorization}

        old_expense_data = fetch_api_data_archived(url,f"{CAMPAIGN_IDS['expense']['old']}", headers)
        new_expense_data = fetch_api_data_archived(url,f"{CAMPAIGN_IDS['expense']['new']}", headers)

        old_mileage_data = fetch_api_data_archived(url,f"{CAMPAIGN_IDS['mileage']['old']}", headers)
        new_mileage_data= fetch_api_data_archived(url,f"{CAMPAIGN_IDS['mileage']['new']}", headers)

        old_conference_data = fetch_api_data_archived(url,f"{CAMPAIGN_IDS['conference']['old']}", headers)
        new_conference_data = fetch_api_data_archived(url,f"{CAMPAIGN_IDS['conference']['new']}", headers)

        new_excel_name = f"Merged Data {start_date_str}-{end_date_str}.xlsx"
        new_csv_name = f"AP-Reimbursement Upload {start_date_str}-{end_date_str}.xlsx"
        result = process_and_upload_files(old_expense_data, new_expense_data, old_mileage_data, new_mileage_data, old_conference_data, new_conference_data, drive_service, GOOGLE_DRIVE_FOLDER_ID1, GOOGLE_DRIVE_FOLDER_ID2, FILE_NAME_TO_DOWNLOAD, X_Authorization, new_excel_name, new_csv_name, upload_excel_true)
        logger.info("Uploaded Files: " + str(result))
    except Exception as e:
        logger.error("Error in run_script_archived: " + str(e))

# Schedular declared for completed status every 1 hour and archived status on every Sunday midnight.
def start_scheduler():
    """Schedules jobs: hourly for completed data and weekly for archived data."""
    # Hourly job
    # scheduler.add_job(run_script_completed, "interval", hours=1, misfire_grace_time=60, max_instances=1, coalesce=True)
    scheduler.add_job(run_script_completed, "interval", minutes=1, misfire_grace_time=60, max_instances=1, coalesce=True)
    scheduler.add_job(run_script_archived, "interval", minutes=7, misfire_grace_time=60, max_instances=1, coalesce=True)
    # Weekly job (every Sunday at 00:10 EST). Delay of 10 minutes. 
    # scheduler.add_job(run_script_archived, "cron", day_of_week="sun", hour=0, minute=10, misfire_grace_time=60, max_instances=1, coalesce=True)
    scheduler.start()
    
    logger.info("Scheduler started successfully.")

# Starting point of the script where schedular start and stops.
if __name__ == "__main__":
    start_scheduler()
    try:
        while True:
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        scheduler.shutdown()
        logger.info("Scheduler stopped.")
