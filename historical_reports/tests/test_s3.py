"""
.. module: historical_reports.tests.test_s3
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import json

import pytest
from historical.common.util import deserialize_records
from historical.s3.models import CurrentS3Model

from historical_reports.tests.conftest import MockContext


def test_historical_s3_table_fixture(historical_s3_table):
    assert CurrentS3Model.count() == 10


def test_dump_buckets_fixture(dump_buckets):
    assert len(dump_buckets.list_buckets()["Buckets"]) == 10


def test_generated_file_fixture_is_json(generated_s3_report_file):
    assert type(generated_s3_report_file) is bytes
    assert len(json.loads(generated_s3_report_file.decode("utf-8"))["buckets"]) == 10


def test_existing_s3_report_fixture(existing_s3_report, dump_buckets, generated_s3_report_file):
    assert generated_s3_report_file == \
           dump_buckets.get_object(Bucket="dump0", Key="historical-report.json")["Body"].read()


def test_bucket_schema(historical_s3_table):
    from historical_reports.s3.models import S3ReportSchema
    from historical_reports.common.constants import REPORTS_VERSION

    all_buckets = CurrentS3Model.scan()
    generated_file = S3ReportSchema(strict=True).dump({"all_buckets": all_buckets}).data

    assert generated_file["generated_date"]
    assert generated_file["report_version"] == REPORTS_VERSION['s3']
    assert not generated_file.get("all_buckets")

    for name, value in generated_file["buckets"].items():
        assert value["AccountId"] == "123456789012"
        assert value["Region"] == "us-east-1"
        assert value["Tags"]["theBucketName"] == name
        assert not value.get("_version")
        assert not value.get("Name")


def test_light_bucket_schema(historical_s3_table):
    import historical_reports.s3.models
    from historical_reports.common.constants import REPORTS_VERSION
    from historical_reports.s3.models import S3ReportSchema

    old_fields = historical_reports.s3.models.EXCLUDE_FIELDS
    historical_reports.s3.models.EXCLUDE_FIELDS = \
        "Name,_version,Grants,LifecycleRules,Logging,Policy,Tags,Versioning,Website,Cors," \
        "Notifications,Acceleration,Replication,CreationDate,AnalyticsConfigurations," \
        "MetricsConfigurations,InventoryConfigurations".split(",")

    all_buckets = CurrentS3Model.scan()
    generated_file = S3ReportSchema(strict=True).dump({"all_buckets": all_buckets}).data

    assert generated_file["generated_date"]
    assert generated_file["report_version"] == REPORTS_VERSION['s3']
    assert len(generated_file["buckets"]) == 10
    assert not generated_file.get("all_buckets")

    for bucket in generated_file["buckets"].values():
        keys = bucket.keys()
        for excluded in historical_reports.s3.models.EXCLUDE_FIELDS:
            assert excluded not in keys

        assert bucket["AccountId"] == "123456789012"
        assert bucket["Region"] == "us-east-1"

    # Clean-up:
    historical_reports.s3.models.EXCLUDE_FIELDS = old_fields


def test_dump_to_s3(dump_buckets, generated_s3_report_file):
    from historical_reports.common.util import dump_to_s3
    from historical_reports.common.constants import DUMP_TO_BUCKETS, DUMP_TO_PREFIX

    dump_to_s3(DUMP_TO_BUCKETS, generated_s3_report_file, DUMP_TO_PREFIX)

    # Check that it's all good:
    for bucket in DUMP_TO_BUCKETS:
        assert dump_buckets.get_object(Bucket=bucket, Key=DUMP_TO_PREFIX)["Body"].read() == generated_s3_report_file


@pytest.mark.parametrize("lambda_entry", [False, True])
def test_dump_report(dump_buckets, historical_s3_table, lambda_entry):
    from historical_reports.common.constants import DUMP_TO_BUCKETS, REPORTS_VERSION, DUMP_TO_PREFIX
    from historical_reports.s3.entrypoints import handler
    from historical_reports.s3.generate import dump_report

    if lambda_entry:
        handler({}, MockContext())
    else:
        dump_report(DUMP_TO_BUCKETS, DUMP_TO_PREFIX)

    # Verify all the info:
    for bucket in DUMP_TO_BUCKETS:
        file = json.loads(dump_buckets.get_object(Bucket=bucket, Key=DUMP_TO_PREFIX)["Body"].read().decode())

        assert file["generated_date"]
        assert file["report_version"] == REPORTS_VERSION['s3']
        assert not file.get("all_buckets")

        for name, value in file["buckets"].items():
            assert value["AccountId"] == "123456789012"
            assert value["Region"] == "us-east-1"
            assert value["Tags"]["theBucketName"] == name
            assert not value.get("_version")
            assert not value.get("Name")


@pytest.mark.parametrize("change_type", ["INSERT", "MODIFY"])
def test_process_dynamodb_record(bucket_event, generated_s3_report, change_type):
    from historical_reports.s3.update import process_dynamodb_record

    bucket_event["Records"][0]["body"] = bucket_event["Records"][0]["body"].replace(
        '\"eventName\": \"INSERT\"', '\"eventName\": \"{}\"'.format(change_type))
    generated_s3_report["all_buckets"] = []
    records = deserialize_records(bucket_event["Records"])

    process_dynamodb_record(records[0], generated_s3_report)

    assert len(generated_s3_report["all_buckets"]) == 1
    assert generated_s3_report["all_buckets"][0].Region == "us-east-1"


def test_process_dynamodb_record_deletion(delete_bucket_event, generated_s3_report):
    from historical_reports.s3.update import process_dynamodb_record

    generated_s3_report["all_buckets"] = []
    records = deserialize_records(delete_bucket_event["Records"])
    process_dynamodb_record(records[0], generated_s3_report)

    # Should not do anything -- since not present in the list:
    assert not generated_s3_report["all_buckets"]

    # Check if removal logic works:
    generated_s3_report["buckets"]["testbucketNEWBUCKET"] = {"some configuration": "this should be deleted"}

    # Standard "MODIFY" for deletion:
    delete_bucket_event["Records"][0]["eventName"] = "MODIFY"
    records = deserialize_records(delete_bucket_event["Records"])
    process_dynamodb_record(records[0], generated_s3_report)
    assert not generated_s3_report["buckets"].get("testbucketNEWBUCKET")


def test_process_dynamodb_deletion_event(delete_bucket_event, generated_s3_report):
    from historical_reports.s3.update import process_dynamodb_record

    generated_s3_report["all_buckets"] = []
    generated_s3_report["buckets"]["testbucketNEWBUCKET"] = {"some configuration": "this should be deleted"}
    delete_bucket_event["Records"][0]["body"] = delete_bucket_event["Records"][0]["body"].replace(
        '\"eventName\": \"MODIFY\"', '\"eventName\": \"{}\"'.format("REMOVE"))
    records = deserialize_records(delete_bucket_event["Records"])
    process_dynamodb_record(records[0], generated_s3_report)

    # Should not do anything -- since not present in the list:
    assert not generated_s3_report["all_buckets"]

    # If we receive a removal event that is NOT from a TTL, that should remove the bucket.
    delete_bucket_event["Records"][0]["eventName"] = "REMOVE"
    records = deserialize_records(delete_bucket_event["Records"])
    process_dynamodb_record(records[0], generated_s3_report)
    assert not generated_s3_report["buckets"].get("testbucketNEWBUCKET")


def test_process_dynamodb_record_ttl(ttl_event, generated_s3_report):
    from historical_reports.s3.update import process_dynamodb_record

    generated_s3_report["all_buckets"] = []
    records = deserialize_records(ttl_event["Records"])
    process_dynamodb_record(records[0], generated_s3_report)

    # Should not do anything -- since not present in the list:
    assert not generated_s3_report["all_buckets"]

    generated_s3_report["buckets"]["testbucketNEWBUCKET"] = {"some configuration": "this should be deleted"}
    process_dynamodb_record(records[0], generated_s3_report)
    assert not generated_s3_report["buckets"].get("testbucketNEWBUCKET")


def test_bucket_schema_for_events(historical_s3_table, generated_s3_report, bucket_event):
    from historical_reports.s3.models import S3ReportSchema
    from historical_reports.common.constants import REPORTS_VERSION
    from historical_reports.s3.update import process_dynamodb_record

    generated_s3_report["all_buckets"] = []
    records = deserialize_records(bucket_event["Records"])
    process_dynamodb_record(records[0], generated_s3_report)

    full_report = S3ReportSchema(strict=True).dump(generated_s3_report).data

    assert full_report["generated_date"]
    assert full_report["report_version"] == REPORTS_VERSION['s3']
    assert not full_report.get("all_buckets")

    assert full_report["buckets"]["testbucketNEWBUCKET"]
    assert len(full_report["buckets"]) == 11

    for name, value in full_report["buckets"].items():
        assert value["AccountId"] == "123456789012"
        assert value["Region"] == "us-east-1"
        assert value["Tags"]["theBucketName"] == name
        assert not value.get("_version")
        assert not value.get("Name")


def test_lite_bucket_schema_for_events(historical_s3_table, bucket_event):
    import historical_reports.s3.models
    from historical_reports.s3.models import S3ReportSchema
    from historical_reports.common.constants import REPORTS_VERSION
    from historical_reports.s3.update import process_dynamodb_record

    old_fields = historical_reports.s3.models.EXCLUDE_FIELDS
    historical_reports.s3.models.EXCLUDE_FIELDS = "Name,Owner,_version,Grants,LifecycleRules,Logging," \
                                                  "Policy,Tags,Versioning,Website,Cors," \
                                                  "Notifications,Acceleration,Replication,CreationDate," \
                                                  "AnalyticsConfigurations," \
                                                  "MetricsConfigurations,InventoryConfigurations".split(",")

    all_buckets = CurrentS3Model.scan()
    generated_report = S3ReportSchema(strict=True).dump({"all_buckets": all_buckets}).data

    generated_report["all_buckets"] = []
    records = deserialize_records(bucket_event["Records"])
    process_dynamodb_record(records[0], generated_report)

    lite_report = S3ReportSchema(strict=True).dump(generated_report).data

    assert lite_report["generated_date"]
    assert lite_report["report_version"] == REPORTS_VERSION['s3']
    assert not lite_report.get("all_buckets")

    assert lite_report["buckets"]["testbucketNEWBUCKET"]
    assert len(lite_report["buckets"]) == 11

    for bucket in lite_report["buckets"].values():
        keys = bucket.keys()
        for excluded in historical_reports.s3.models.EXCLUDE_FIELDS:
            assert excluded not in keys

        assert bucket["AccountId"] == "123456789012"
        assert bucket["Region"] == "us-east-1"

    # Clean-up:
    historical_reports.s3.models.EXCLUDE_FIELDS = old_fields


@pytest.mark.parametrize("lambda_entry", [False, True])
def test_update_records(existing_s3_report, historical_s3_table, bucket_event, delete_bucket_event, dump_buckets,
                        lambda_entry):
    import historical_reports.s3.entrypoints
    from historical_reports.s3.entrypoints import handler
    from historical_reports.s3.update import update_records

    old_import_bucket = historical_reports.s3.entrypoints.IMPORT_BUCKET
    old_import_prefix = historical_reports.s3.entrypoints.IMPORT_PREFIX

    historical_reports.s3.entrypoints.IMPORT_BUCKET = 'dump0'
    historical_reports.s3.entrypoints.IMPORT_PREFIX = 'historical-report.json'

    # Add a bucket:
    if lambda_entry:
        handler(bucket_event, MockContext())
    else:
        records = deserialize_records(bucket_event["Records"])
        update_records(records, 'dump0', 'historical-report.json')

    new_report = json.loads(
        dump_buckets.get_object(Bucket="dump0", Key="historical-report.json")["Body"].read().decode("utf-8")
    )
    assert len(new_report["buckets"]) == 11

    existing_json = json.loads(existing_s3_report.decode("utf-8"))
    assert len(new_report["buckets"]) != len(existing_json["buckets"])
    assert new_report["buckets"]["testbucketNEWBUCKET"]

    # Delete a bucket:
    if lambda_entry:
        handler(delete_bucket_event, MockContext())
    else:
        records = deserialize_records(delete_bucket_event["Records"])
        update_records(records, 'dump0', 'historical-report.json')

    delete_report = json.loads(
        dump_buckets.get_object(Bucket="dump0", Key="historical-report.json")["Body"].read().decode("utf-8")
    )
    assert len(delete_report["buckets"]) == len(existing_json["buckets"]) == 10
    assert not delete_report["buckets"].get("testbucketNEWBUCKET")

    # Clean-up:
    historical_reports.s3.entrypoints.IMPORT_BUCKET = old_import_bucket
    historical_reports.s3.entrypoints.IMPORT_PREFIX = old_import_prefix


def test_update_records_sans_existing(historical_s3_table, dump_buckets, bucket_event):
    from historical_reports.s3.update import update_records

    # First test that the object is missing, and we aren't going to perform a full report dump:
    update_records(bucket_event["Records"], 'dump0', 'historical-report.json', commit=True, export_missing=False)
    assert not dump_buckets.list_objects_v2(Bucket="dump0")["KeyCount"]

    # Now, with commit set to False:
    update_records(bucket_event["Records"], 'dump0', 'historical-report.json', commit=False, export_missing=True)
    assert not dump_buckets.list_objects_v2(Bucket="dump0")["KeyCount"]

    # Now with commit:
    update_records(bucket_event["Records"], 'dump0', 'historical-report.json', commit=True, export_missing=True)
    assert len(dump_buckets.list_objects_v2(Bucket="dump0")["Contents"]) == 1

