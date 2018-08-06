"""
.. module: historical_reports.common.update
    :platform: Unix
    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import json
import logging

import boto3
from historical.attributes import decimal_default
from historical.common.dynamodb import deserialize_durable_record_to_current_model
from raven_python_lambda import RavenLambdaWrapper

from historical.constants import LOGGING_LEVEL, CURRENT_REGION
from historical.common.util import deserialize_records

from historical_reports.common.constants import DUMP_TO_BUCKETS, DUMP_TO_PREFIX, IMPORT_BUCKET, IMPORT_PREFIX, STACK
from historical_reports.common.generate import dump_report
from historical_reports.common.model_table import INDEX
from historical_reports.common.models import ReportSchema
from historical_reports.common.util import fetch_from_s3, dump_to_s3

logging.basicConfig()
log = logging.getLogger('historical-reports-{}'.format(STACK))
log.setLevel(LOGGING_LEVEL)


def process_dynamodb_record(record, report):
    """
    Processes a group of DynamoDB NewImage records.

    This logic is largely copy and pasted from Historical with few modifications.
    """
    if record['eventName'] == 'REMOVE':
        # This logic is copied and pasted from Historical. The Durable table does not (yet? maybe? never?) have
        # TTLs. As such, this should never happen here:
        # if record.get('userIdentity'):
        #     if record['userIdentity']['type'] == 'Service':
        #         if record['userIdentity']['principalId'] == 'dynamodb.amazonaws.com':
        #             s3_report["buckets"].pop(record['dynamodb']['OldImage']["BucketName"]["S"], None)
        #             log.error("[?] Processing TTL deletion for ARN/Event Time: {}/{} in the "
        #                       "Durable table. This is odd...".format(
        #                         record['dynamodb']['Keys']['arn']['S'],
        #                         record['dynamodb']['OldImage']['eventTime']['S']))

        # This should **NOT** be happening in the Durable table... If it does, we need to raise an exception:
        # else:
        report["buckets"].pop(record['dynamodb']['OldImage']["BucketName"]["S"], None)
        log.error('[?] Item with ARN/Event Time: {}/{} was deleted from the Durable table.'
                  ' This is odd...'.format(record['dynamodb']['Keys']['arn']['S'],
                                           record['dynamodb']['OldImage']['eventTime']['S']))

    if record['eventName'] in ['INSERT', 'MODIFY']:
        # Serialize that specific report:
        modified_bucket = deserialize_durable_record_to_current_model(record, CurrentS3Model)

        # If the current object is too big for SNS, and it's not in the current table, then delete it.
        # -- OR -- if this a soft-deletion? (Config set to {})
        if not modified_bucket or not modified_bucket.configuration.attribute_values:
            report["buckets"].pop(record['dynamodb']['NewImage']["BucketName"]["S"], None)
        else:
            report["all_buckets"].append(modified_bucket)


def update_records(records, load_bucket, load_prefix, commit=True, export_missing=True):
    log.debug("[@] Starting Record Update.")

    # First, grab the existing JSON from S3:
    client = boto3.client("s3", region_name=CURRENT_REGION)

    # The prefix is going to be: load_prefix + INDEX[STACK] + ACCOUNTID_region.json
    file_prefix = load_prefix + '/' + INDEX[STACK] + '/'

    existing_json = fetch_from_s3(client, load_bucket, load_prefix)
    log.debug("[ ] Grabbed all the existing data from S3.")

    # The dump bucket and the dump prefix are the same as the input bucket and the input prefix.

    # If the existing JSON is not present for some reason, then...
    if not existing_json:
        if commit and export_missing:
            log.info("[!] The report does not exist. Dumping the full report to {}/{}".format(load_prefix, load_prefix))
            dump_report([load_bucket], load_prefix)

        else:
            log.error("[X] The existing log was not present and the `EXPORT_IF_MISSING` env var was "
                      "not set so exiting.")

        return

    # Deserialize the report:
    report = ReportSchema().loads(existing_json).data
    report["all_buckets"] = []

    for record in records:
        process_dynamodb_record(record, report)

    # Serialize the data:
    generated_file = ReportSchema(strict=True).dump(report).data

    # Dump to S3:
    if commit:
        log.debug("[-->] Saving to S3.")

        # Replace <empty> with "" <-- Due to Pynamo/Dynamo issues...
        file = json.dumps(generated_file, indent=4, default=decimal_default).replace("\"<empty>\"", "\"\"").encode(
            "utf-8")
        dump_to_s3([load_bucket], file, load_prefix)
    else:
        log.debug("[/] Commit flag not set, not saving.")

    log.debug("[@] Completed S3 report update.")


@RavenLambdaWrapper()
def handler(event, context):
    """
    Historical report lambda handler for individual item updates. This will only handle DynamoDB stream events.
    """
    # Deserialize the records:
    records = deserialize_records(event["Records"])

    # Update event:
    update_records(records, IMPORT_BUCKET, IMPORT_PREFIX)



# @RavenLambdaWrapper()
# def handler(event, context):
#     """
#     Historical S3 report generator lambda handler. This will handle both scheduled events as well as DynamoDB stream
#     events.
#     """
#     if event.get("Records"):
#         # Deserialize the records:
#         records = deserialize_records(event["Records"])
#
#         # Update event:
#         update_records(records, IMPORT_BUCKET, IMPORT_PREFIX)
#
#     else:
#         # Generate event:
#         dump_report(DUMP_TO_BUCKETS, DUMP_TO_PREFIX)
