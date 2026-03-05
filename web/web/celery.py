from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from django.conf import settings

# Configurar el entorno de Django para Celery
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'web.settings')

app = Celery('web')

# Leer configuraciones desde settings.py con prefijo CELERY_
app.config_from_object('django.conf:settings', namespace='CELERY')

# discover and load tasks.py from from all registered Django apps
#app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

app.autodiscover_tasks()
#@app.task(bind=True)
#def debug_task(self):
#    print(f'Request: {self.request!r}')