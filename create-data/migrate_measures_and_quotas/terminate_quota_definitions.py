import classes.globals as g
import os
import sys

from classes.application import application

app = g.app
app.get_templates()
app.get_envelope()

# Get all the definitions, associations, suspension and blocking periods that exist in the database
# that do not start until after Brexit
app.get_future_quota_definitions()
app.get_future_quota_associations()
app.get_future_quota_suspension_periods()
app.get_future_quota_blocking_periods()

"""
# Then create delete instructions for them
app.kill_future_quota_definitions()

# Then truncate the definitions that straddle Brexit
app.get_straddling_quota_definitions("truncate")
app.truncate_straddling_quota_definitions()
"""

# Then create delete instructions for them
app.kill_future_associations()

# Then create delete instructions for them
app.kill_future_quota_definitions()

# Then truncate the definitions that straddle Brexit
app.get_straddling_quota_definitions("truncate")
app.truncate_straddling_quota_definitions()


app.write_content()
app.validate(app.output_filename)
app.copy_to_custom_import_folder()
