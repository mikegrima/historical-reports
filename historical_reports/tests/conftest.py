"""
.. module: historical_reports.tests.conftest
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import json
import os
import random
import string

import boto3
import pytest
from mock import patch

from historical.s3.models import CurrentS3Model
from historical.security_group.models import CurrentSecurityGroupModel
from moto import mock_dynamodb2, mock_s3

from historical_reports.tests.factories import DynamoDBRecordFactory, DynamoDBDataFactory, \
    serialize, UserIdentityFactory, RecordsFactory, SQSDataFactory, SnsDataFactory

S3_BUCKET = """{
    "arn": "arn:aws:s3:::testbucket{number}",
    "principalId": "joe@example.com",
    "userIdentity": {
        "sessionContext": {
            "userName": "TseXSKEYQrxm",
            "type": "Role",
            "arn": "arn:aws:iam::123456789012:role/historical_poller",
            "principalId": "AROAIKELBS2RNWG7KASDF",
            "accountId": "123456789012"
        },
        "principalId": "AROAIKELBS2RNWG7KASDF:joe@example.com",
        "type": "Service"
    },
    "eventSource": "aws.s3",
    "accountId": "123456789012",
    "eventTime": "2017-11-10T18:33:44Z",
    "eventSource": "aws.s3",
    "BucketName": "testbucket{number}",
    "Region": "us-east-1",
    "Tags": {
        "theBucketName": "testbucket{number}"
    },
    "configuration": {
        "Grants": {
            "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a": [
                "FULL_CONTROL"
            ]
        },
        "LifecycleRules": [
            {
                "Expiration": {
                    "Date": "2015-01-01T00:00:00Z",
                    "Days": 123
                },
                "ID": "string",
                "Prefix": "string",
                "Status": "Enabled",
                "Transitions": [
                    {
                        "Date": "2015-01-01T00:00:00Z",
                        "Days": 123,
                        "StorageClass": "GLACIER"
                    }
                ]
            }
        ],
        "Logging": {},
        "Policy": null,
        "Tags": {
            "theBucketName": "testbucket{number}"
        },
        "Versioning": {},
        "Website": null,
        "Cors": [],
        "Notifications": {},
        "Acceleration": null,
        "Replication": {},
        "CreationDate": "2006-02-03T16:45:09Z",
        "AnalyticsConfigurations": [],
        "MetricsConfigurations": [],
        "InventoryConfigurations": [],
        "Name": "testbucket{number}",
        "_version": 8
    }
}"""

SECURITY_GROUP = """{
    "arn": "arn:aws:ec2:us-east-1:123456789012:security-group/sg-{id}",
    "GroupId": "sg-{id}",
    "GroupName": "group{number}",
    "eventSource": "aws.ec2",
    "VpcId": "vpc-123343",
    "accountId": "{account}",
    "OwnerId": "{account}",
    "Description": "This is a test",
    "Region": "{region}",
    "Tags": [
        {
            "Name": "test",
            "Value": "<empty>"
        }
    ],
    "configuration": {
        "Description": "string",
        "GroupName": "string",
        "IpPermissions": [
            {
                "FromPort": 123,
                "IpProtocol": "string",
                "IpRanges": [
                    {
                        "CidrIp": "string"
                    }
                ],
                "Ipv6Ranges": [
                    {
                        "CidrIpv6": "string"
                    }
                ],
                "PrefixListIds": [
                    {
                        "PrefixListId": "string"
                    }
                ],
                "ToPort": 123,
                "UserIdGroupPairs": [
                    {
                        "GroupId": "string",
                        "GroupName": "string",
                        "PeeringStatus": "string",
                        "UserId": "string",
                        "VpcId": "string",
                        "VpcPeeringConnectionId": "string"
                    }
                ]
            }
        ],
        "OwnerId": "string",
        "GroupId": "string",
        "IpPermissionsEgress": [
            {
                "FromPort": 123,
                "IpProtocol": "string",
                "IpRanges": [
                    {
                        "CidrIp": "string"
                    }
                ],
                "Ipv6Ranges": [
                    {
                        "CidrIpv6": "string"
                    }
                ],
                "PrefixListIds": [
                    {
                        "PrefixListId": "string"
                    }
                ],
                "ToPort": 123,
                "UserIdGroupPairs": [
                    {
                        "GroupId": "string",
                        "GroupName": "string",
                        "PeeringStatus": "string",
                        "UserId": "string",
                        "VpcId": "string",
                        "VpcPeeringConnectionId": "string"
                    }
                ]
            }
        ],
        "Tags": [
            {
                "Key": "string",
                "Value": "string"
            }
        ],
        "VpcId": "string"
    }
}"""


class MockContext:
    def get_remaining_time_in_millis(self):
        return 9000


@pytest.fixture(scope='function')
def dynamodb():
    with mock_dynamodb2():
        yield boto3.client('dynamodb', region_name='us-east-1')


@pytest.fixture(scope='function')
def s3():
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


@pytest.fixture(scope='function')
def retry():
    # Mock the retry:
    def mock_retry_decorator(*args, **kwargs):
        def retry(func):
            return func
        return retry

    p = patch('retrying.retry', mock_retry_decorator)
    yield p.start()

    p.stop()


@pytest.fixture(scope='function')
def swag_accounts(s3, retry):
    from swag_client.backend import SWAGManager
    from swag_client.util import parse_swag_config_options

    s3 = boto3.client('s3', region_name='us-east-1')

    bucket_name = 'SWAG'
    data_file = 'accounts.json'
    region = 'us-east-1'
    owner = 'third-party'

    s3.create_bucket(Bucket=bucket_name)
    os.environ['SWAG_BUCKET'] = bucket_name
    os.environ['SWAG_DATA_FILE'] = data_file
    os.environ['SWAG_REGION'] = region
    os.environ['SWAG_OWNER'] = owner

    swag_opts = {
        'swag.type': 's3',
        'swag.bucket_name': bucket_name,
        'swag.data_file': data_file,
        'swag.region': region,
        'swag.cache_expires': 0
    }

    swag = SWAGManager(**parse_swag_config_options(swag_opts))

    accountOne = {
        'aliases': ['test'],
        'contacts': ['admins@test.net'],
        'description': 'LOL, Test account',
        'email': 'testaccount@test.net',
        'environment': 'test',
        'id': '111111111111',
        'name': 'testaccount',
        'owner': 'third-party',
        'provider': 'aws',
        'sensitive': False,
        'services': [
            {
                'name': 'historical',
                'status': [
                    {
                        'region': 'all',
                        'enabled': True
                    }
                ]
            }
        ]
    }
    swag.create(accountOne)

    accountOne['id'] = '222222222222'
    swag.create(accountOne)


@pytest.fixture(scope='function')
def current_s3_table(dynamodb):
    yield CurrentS3Model.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)


@pytest.fixture(scope='function')
def current_sg_table(dynamodb):
    yield CurrentSecurityGroupModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)


@pytest.fixture(scope="function")
def historical_s3_table(current_s3_table):
    for x in range(0, 10):
        bucket = json.loads(S3_BUCKET.replace("{number}", str(x)))
        CurrentS3Model(**bucket).save()


@pytest.fixture(scope="function")
def historical_sg_table(current_sg_table, swag_accounts):
    for x in range(0, 10):
        region_map = ['us-east-1', 'us-west-2', 'eu-west-1']

        pre_group = SECURITY_GROUP.replace('{number}', str(x)) \
            .replace('{region}', region_map[int((x / 3) % 3)]) \
            .replace('{id}', ''.join(random.choices(string.ascii_lowercase + string.digits, k=16))) \
            .replace('{account}', '111111111111' if int((x / 2) % 2) else '222222222222')

        group = json.loads(pre_group)
        CurrentSecurityGroupModel(**group).save()


@pytest.fixture(scope="function")
def dump_buckets(s3):
    import historical_reports.common.constants
    old_dump_buckets = historical_reports.common.constants.DUMP_TO_BUCKETS
    old_dump_prefix = historical_reports.common.constants.DUMP_TO_PREFIX

    buckets = []
    for x in range(0, 10):
        bucket_name = "dump{}".format(x)
        s3.create_bucket(Bucket=bucket_name)
        buckets.append(bucket_name)

    historical_reports.common.constants.DUMP_TO_BUCKETS = buckets
    historical_reports.common.constants.DUMP_TO_PREFIX = "historical-report.json"

    yield s3

    historical_reports.common.constants.DUMP_TO_BUCKETS = old_dump_buckets
    historical_reports.common.constants.DUMP_TO_PREFIX = old_dump_prefix


@pytest.fixture(scope="function")
def generated_s3_report_file(historical_s3_table):
    from historical_reports.s3.models import S3ReportSchema
    all_buckets = CurrentS3Model.scan()
    return S3ReportSchema(strict=True).dumps({"all_buckets": all_buckets}).data.encode("utf-8")


@pytest.fixture(scope="function")
def generated_s3_report(generated_s3_report_file):
    return json.loads(generated_s3_report_file.decode("utf-8"))


@pytest.fixture(scope="function")
def generated_sg_report_file(historical_sg_table):
    from historical_reports.common.model_table import INDEX
    import historical_reports.common.constants

    historical_reports.common.constants.STACK = STACK = 'securitygroup'

    from historical_reports.common.models import ReportSchema, HistoricalField

    # Need to dynamically generate the fields. This is similar to:
    # https://stackoverflow.com/questions/42231334/define-fields-programmatically-in-marshmallow-schema
    # This will serialize everything from the `all_TECHNOLOGY` field.
    Schema = type('ReportSchema', (ReportSchema,), {
        'all_{}'.format(INDEX[STACK]): HistoricalField(required=True, dump_to=INDEX[STACK]),
        INDEX[STACK]: HistoricalField(required=True, load_from=INDEX[STACK], load_only=True)
    })

    # Only for 222222222222/us-east-1:
    all_securitygroups = CurrentSecurityGroupModel.scan((CurrentSecurityGroupModel.accountId == '222222222222') &
                                                        (CurrentSecurityGroupModel.Region == 'us-east-1'))
    yield Schema(strict=True).dumps({'all_{}'.format(INDEX[STACK]): all_securitygroups}).data.encode("utf-8")

    historical_reports.common.constants.STACK = None


@pytest.fixture(scope="function")
def generated_sg_report(generated_sg_report_file):
    return json.loads(generated_sg_report_file.decode("utf-8"))


@pytest.fixture(scope="function")
def bucket_event():
    new_bucket = json.loads(S3_BUCKET.replace("{number}", "NEWBUCKET"))

    new_item = json.dumps(DynamoDBRecordFactory(
        dynamodb=DynamoDBDataFactory(
            NewImage=new_bucket,
            Keys={
                'arn': new_bucket['arn']
            }
        ),
        eventName='INSERT'), default=serialize)

    records = RecordsFactory(records=[SQSDataFactory(body=json.dumps(SnsDataFactory(Message=new_item),
                                                                     default=serialize))])
    return json.loads(json.dumps(records, default=serialize))


@pytest.fixture(scope="function")
def delete_bucket_event():
    delete_bucket = json.loads(S3_BUCKET.replace("{number}", "NEWBUCKET"))
    delete_bucket["configuration"] = {}

    new_item = json.dumps(DynamoDBRecordFactory(
        dynamodb=DynamoDBDataFactory(
            NewImage=delete_bucket,
            Keys={
                'arn': delete_bucket['arn']
            }
        ),
        eventName='MODIFY'), default=serialize)

    records = RecordsFactory(records=[SQSDataFactory(body=json.dumps(SnsDataFactory(Message=new_item),
                                                                     default=serialize))])

    return json.loads(json.dumps(records, default=serialize))


@pytest.fixture(scope="function")
def ttl_event():
    bucket = json.loads(S3_BUCKET.replace("{number}", "NEWBUCKET"))

    new_item = json.dumps(DynamoDBRecordFactory(
        dynamodb=DynamoDBDataFactory(
            OldImage=bucket,
            Keys={
                'arn': bucket['arn']
            }),
        eventName='REMOVE',
        userIdentity=UserIdentityFactory(
            type='Service',
            principalId='dynamodb.amazonaws.com'
        )), default=serialize)

    records = RecordsFactory(records=[SQSDataFactory(body=json.dumps(SnsDataFactory(Message=new_item),
                                                                     default=serialize))])

    return json.loads(json.dumps(records, default=serialize))


@pytest.fixture(scope="function")
def existing_s3_report(dump_buckets, generated_s3_report_file):
    dump_buckets.put_object(Bucket="dump0", Key="historical-report.json", Body=generated_s3_report_file)

    return generated_s3_report_file


@pytest.fixture(scope="function")
def existing_sg_report(dump_buckets, generated_sg_report_file):
    dump_buckets.put_object(Bucket="dump0", Key="222222222222_us-east-1.json", Body=generated_sg_report_file)

    return generated_sg_report_file
