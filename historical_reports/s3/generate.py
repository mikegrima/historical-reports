"""
.. module: historical_reports.s3.generate
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import json
import logging

from historical.constants import LOGGING_LEVEL
from historical.s3.models import CurrentS3Model

from historical_reports.common.util import dump_to_s3
from historical_reports.s3.models import S3ReportSchema

logging.basicConfig()
log = logging.getLogger('historical-reports-s3')
log.setLevel(LOGGING_LEVEL)


def dump_report(dump_buckets, dump_prefix, commit=True):
    # Get all the data from DynamoDB:
    log.debug("[ ] Beginning DynamoDB scan...")
    all_buckets = CurrentS3Model.scan()

    generated_file = S3ReportSchema(strict=True).dump({"all_buckets": all_buckets}).data

    # Dump to S3:
    if commit:
        log.debug("[-->] Saving to S3.")

        # Replace <empty> with "" <-- Due to Pynamo/Dynamo issues...
        # Need to add the file, and the prefix:
        file = json.dumps(generated_file, indent=4).replace("\"<empty>\"", "\"\"").encode("utf-8")
        dump_to_s3(dump_buckets, file, dump_prefix)
    else:
        log.debug("[/] Commit flag not set, not saving.")

    log.debug("[@] Completed S3 report generation.")
