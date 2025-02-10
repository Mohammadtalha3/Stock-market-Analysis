from ingestion_pipeline.pipeline import load_daily_Data,load_intraday_Data
from  celery_app import  celery





@celery.task(name="celery_worker.fetch_and_process_daily_data")
def fetch_and_process_daily_data():
    load_daily_Data()


@celery.task(name="celery_worker.fetch_and_process_intraday_data")
def fetch_and_process_intraday_data():
    load_intraday_Data()
