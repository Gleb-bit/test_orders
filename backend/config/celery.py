from celery import Celery

from config import settings

celery_app = Celery("config", broker=settings.CELERY_BROKER_URL)

celery_app.config_from_object(settings)
celery_app.autodiscover_tasks([])
