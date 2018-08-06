"""
.. module: historical_reports.s3.entrypoints
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
from raven_python_lambda import RavenLambdaWrapper

from historical.common.util import deserialize_records

from historical_reports.common.constants import DUMP_TO_BUCKETS, DUMP_TO_PREFIX, IMPORT_BUCKET, IMPORT_PREFIX
from historical_reports.s3.generate import dump_report
from historical_reports.s3.update import update_records


@RavenLambdaWrapper()
def handler(event, context):
    """
    Historical S3 report generator lambda handler. This will handle both scheduled events as well as DynamoDB stream
    events.
    """
    if event.get("Records"):
        # Deserialize the records:
        records = deserialize_records(event["Records"])

        # Update event:
        update_records(records, IMPORT_BUCKET, IMPORT_PREFIX)

    else:
        # Generate event:
        dump_report(DUMP_TO_BUCKETS, DUMP_TO_PREFIX)
