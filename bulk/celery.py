import os
from celery import Celery
import logging

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'bulk.settings')

app = Celery('bulk')

# Use Redis as the broker
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

# # Configure logging
# celery_log_format = '[%(asctime)s: %(levelname)s/%(processName)s] %(message)s'
# logging.basicConfig(format=celery_log_format, level=logging.INFO)

# # Create a logger
# logger = logging.getLogger(__name__)

# # Ensure Celery logs use the same configuration
# app.conf.update({  # Corrected this line
#     'worker_hijack_root_logger': False,
#     'log_level': 'DEBUG'  # Set to DEBUG or INFO according to your needs
# })
