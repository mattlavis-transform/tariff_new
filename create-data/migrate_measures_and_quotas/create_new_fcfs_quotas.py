import classes.globals as g
import os
import sys

from classes.application import application

app = g.app
app.get_templates()
app.get_envelope()

# Get all the definitions, associations, suspension and blocking periods that exist in the database
# that do not start until after Brexit

app.get_all_quota_order_numbers()
app.get_quota_balances()
app.get_quota_descriptions()

app.get_current_quota_definitions()
app.get_current_quota_associations()

app.define_future_quota_definitions()
app.write_uk_future_quota_definitions()
app.write_uk_future_quota_associations()

app.write_content()
app.validate(app.output_filename)