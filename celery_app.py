from celery import Celery
from celery.schedules import crontab
# import celery_worker

celery = Celery("celery_app", broker="redis://localhost:6379/0", include=['celery_worker'])

celery.conf.beat_schedule = {
    "fetch-and-process-intraday-data": {
        "task": "celery_worker.fetch_and_process_intraday_data",  # Fix Task Name
        "schedule": crontab(minute="0",hour= "9"),  # Every 5 minutes minute="*/5"
    },
    "fetch-and-process-daily-data": {
        "task": "celery_worker.fetch_and_process_daily_data",  # Fix Task Name
        "schedule": crontab(minute="0", hour="9") #day at 9 AM
    },
}
 
celery.conf.timezone = "UTC"
# celery.autodiscover_tasks(['celery_worker'])


