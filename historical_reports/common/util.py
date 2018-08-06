"""
.. module: historical_reports.common.util
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import logging
import boto3
from retrying import retry
from botocore.exceptions import ClientError

from historical.constants import LOGGING_LEVEL

from historical_reports.common.constants import STACK

logging.basicConfig()
log = logging.getLogger('historical-reports-{}'.format(STACK))
log.setLevel(LOGGING_LEVEL)


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def _upload_to_s3(file, client, bucket, prefix, content_type="application/json"):
    client.put_object(Bucket=bucket, Key=prefix, Body=file, ContentType=content_type)


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def _get_from_s3(client, bucket, prefix):
    try:
        return client.get_object(Bucket=bucket, Key=prefix)["Body"].read().decode()
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'NoSuchKey':
            return None


def dump_to_s3(buckets, file, prefix):
    """
    This will dump the generated schema to S3.
    :param file:
    :param prefix:
    :return:
    """
    client = boto3.client("s3")

    # Loop over each bucket and dump:
    for bucket in buckets:
        log.debug("[ ] Dumping to {}/{}".format(bucket, prefix))
        _upload_to_s3(file, client, bucket, prefix)
        log.debug("[+] Complete")

    log.debug("[+] Completed dumping to all buckets.")


def fetch_from_s3(client, bucket, prefix):
    """
    This will fetch the report object from S3.
    :param bucket:
    :param prefix:
    :return:
    """
    return _get_from_s3(client, bucket, prefix)
