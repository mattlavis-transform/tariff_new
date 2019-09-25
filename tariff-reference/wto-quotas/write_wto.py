from application import application

app = application()
app.connect()
app.get_quotas()
app.write_quotas()

