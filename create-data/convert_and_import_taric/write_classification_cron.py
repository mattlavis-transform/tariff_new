from crontab import CronTab

cron = CronTab(user='matt.admin')
job = cron.new(command='python /Users/matt.admin/projects/tariffs/create-data/convert_and_import_taric/write_classification.py')
job.hour.every(1)
job.minute.on(35)

cron.write()