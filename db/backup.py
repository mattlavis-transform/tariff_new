#!/usr/bin/env python
import sys
from application import application

app = application()
app.get_config()
app.db_backup()
