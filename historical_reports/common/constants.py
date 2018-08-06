"""
.. module: historical_reports.common.constants
    :platform: Unix
    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import os

STACK = os.environ.get('STACK', None)

REPORTS_VERSION = {
    'securitygroup': 1,
    's3': 1
}


EXCLUDE_FIELDS = os.environ.get("EXCLUDE_FIELDS", "Name,_version").split(",")
DUMP_TO_BUCKETS = os.environ.get("DUMP_TO_BUCKETS", "").split(",")
IMPORT_BUCKET = os.environ.get("IMPORT_BUCKET", None)
EXPORT_IF_MISSING = os.environ.get("EXPORT_IF_MISSING", False)

# For S3, these are the full prefix including the file name - otherwise, this is just the path to where objects will
# be dumped:
# i.e. For S3, this should be: 'historical-s3-report.json'
DUMP_TO_PREFIX = os.environ.get("DUMP_TO_PREFIX", None)
IMPORT_PREFIX = os.environ.get("IMPORT_PREFIX", None)
