from django.urls import path
from .views import  upload_files
# check_task_status

urlpatterns = [
    path('upload/', upload_files, name='upload'),
    # path('display/', display_data, name='display_data'),
    # path('task_status/', check_task_status, name='check_task_status'),

]