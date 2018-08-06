"""
.. module: historical_reports.tests.test_common
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import json

import pytest
from historical.security_group.models import CurrentSecurityGroupModel

from historical_reports.tests.conftest import MockContext


def test_historical_sg_table_fixture(historical_sg_table):
    results = list(CurrentSecurityGroupModel.scan())

    assert len(results) == 10

    count_map = {
        '111111111111': 0,
        '222222222222': 0,
        'us-east-1': 0,
        'us-west-2': 0,
        'eu-west-1': 0
    }

    for sg in results:
        count_map[sg.accountId] += 1
        count_map[sg.Region] += 1

    # There should be the following:
    assert count_map['111111111111'] == 4
    assert count_map['222222222222'] == 6
    assert count_map['us-east-1'] == 4
    assert count_map['us-west-2'] == 3
    assert count_map['eu-west-1'] == 3

    assert count_map['111111111111'] + count_map['222222222222'] == 10
    assert count_map['us-east-1'] + count_map['us-west-2'] + count_map['eu-west-1'] == 10


def test_generated_file_fixture_is_json(generated_sg_report_file):
    assert type(generated_sg_report_file) is bytes
    assert len(json.loads(generated_sg_report_file.decode("utf-8"))["securitygroups"]) == 3


def test_existing_s3_report_fixture(existing_sg_report, dump_buckets, generated_sg_report_file):
    assert generated_sg_report_file == \
           dump_buckets.get_object(Bucket="dump0", Key="222222222222_us-east-1.json")["Body"].read()


# @pytest.mark.parametrize("lambda_entry", [False, True])
# def test_dump_report(dump_buckets, historical_sg_table, lambda_entry):
#     from historical_reports.common.constants import DUMP_TO_BUCKETS, REPORTS_VERSION, DUMP_TO_PREFIX
#     from historical_reports.common.entrypoints import handler
#     from historical_reports.common.generate import dump_report
#
#     if lambda_entry:
#         handler({}, MockContext())
#     else:
#         dump_report(DUMP_TO_BUCKETS, DUMP_TO_PREFIX)
#
#     # Verify all the info:
#     for bucket in DUMP_TO_BUCKETS:
#         file = json.loads(dump_buckets.get_object(Bucket=bucket, Key=DUMP_TO_PREFIX)["Body"].read().decode())
#
#         assert file["generated_date"]
#         assert file["report_version"] == REPORTS_VERSION['s3']
#         assert not file.get("all_buckets")
#
#         for name, value in file["buckets"].items():
#             assert value["AccountId"] == "123456789012"
#             assert value["Region"] == "us-east-1"
#             assert value["Tags"]["theBucketName"] == name
#             assert not value.get("_version")
#             assert not value.get("Name")
