"""
.. module: historical_reports.common.models
    :platform: Unix
    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import logging

from datetime import datetime
from marshmallow import Schema, fields
from marshmallow.fields import Field

from historical.constants import LOGGING_LEVEL
from historical_reports.common.constants import REPORTS_VERSION, STACK, EXCLUDE_FIELDS
from historical_reports.common.model_table import INDEX, NAME_FIELDS

logging.basicConfig()
log = logging.getLogger('historical-reports-{}'.format(STACK))
log.setLevel(LOGGING_LEVEL)


def get_generated_time(*args):
    return datetime.utcnow().replace(tzinfo=None, microsecond=0).isoformat() + "Z"


def _serialize_item(item):
    # Remove the redundant name field:
    item.pop(NAME_FIELDS[STACK], None)

    # Remove fields in the exclusion list:
    for e in EXCLUDE_FIELDS:
        item.pop(e, None)

    return item


class HistoricalField(Field):
    def _serialize(self, value, attr=None, data=None):
        # Check if the TECHNOLOGY field has any items
        items = data.get('{}'.format(INDEX[STACK]), {})

        # Loop over ALL the TECHNOLOGY items:
        for i in data['all_{}'.format(INDEX[STACK])]:
            log.debug("[ ] Fetched details for {}: {}".format(STACK, i.arn))

            name = getattr(i, NAME_FIELDS[STACK])

            # Add the bucket:
            items[name] = _serialize_item(i.configuration.attribute_values)

        return items

    def _deserialize(self, value, attr, data):
        return {name: details for name, details in data[INDEX[STACK]].items()}


class ReportSchema(Schema):
    report_version = fields.Int(dump_only=True, required=True, default=REPORTS_VERSION[STACK])
    generated_date = fields.Function(get_generated_time, required=True)

