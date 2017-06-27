#!/usr/bin/env python
from flaskexample import app
app.run(host = '0.0.0.0', debug = True)

from celery import current_app
from celery.bin import worker

application = current_app._get_current_object()

worker = worker.worker(app=application)

options = {
    'broker': app.config['redis://localhost:6379/0'],
    'loglevel': 'INFO',
    'traceback': True,
}

worker.run(**options)