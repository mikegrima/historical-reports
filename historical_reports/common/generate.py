"""
.. module: historical_reports.common.generate
    :platform: Unix
    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import json
import logging

from historical.constants import LOGGING_LEVEL

from historical_reports.common.constants import STACK
from historical_reports.common.model_table import CURRENT_TABLE, INDEX
from historical_reports.common.models import ReportSchema, HistoricalField
from historical_reports.common.util import dump_to_s3

logging.basicConfig()
log = logging.getLogger('historical-reports-{}'.format(STACK))
log.setLevel(LOGGING_LEVEL)


def dump_report(dump_buckets, dump_prefix, account_id, region, commit=True):
    # Get all the data from DynamoDB:
    log.debug("[ ] Beginning DynamoDB scan...")
    all_items = CURRENT_TABLE[STACK].scan(
        (CURRENT_TABLE[STACK].accountId == account_id) & (CURRENT_TABLE[STACK].Region == region))

    # Need to dynamically generate the fields. This is similar to:
    # https://stackoverflow.com/questions/42231334/define-fields-programmatically-in-marshmallow-schema
    # This will serialize everything from the `all_TECHNOLOGY` field.
    Schema = type('ReportSchema', (ReportSchema,), {
        'all_{}'.format(INDEX[STACK]): HistoricalField(required=True, dump_to=INDEX[STACK]),
        INDEX[STACK]: HistoricalField(required=True, load_from=INDEX[STACK], load_only=True)
    })

    generated_file = Schema(strict=True).dump({'all_{}'.format(INDEX[STACK]): all_items}).data

    # Dump to S3:
    if commit:
        log.debug("[-->] Saving to S3.")

        # Replace <empty> with "" <-- Due to Pynamo/Dynamo issues...
        file = json.dumps(generated_file, indent=4).replace("\"<empty>\"", "\"\"").encode("utf-8")
        dump_to_s3(dump_buckets, file, dump_prefix)
    else:
        log.debug("[/] Commit flag not set, not saving.")

    log.debug("[@] Completed {} report generation.".format(STACK))
