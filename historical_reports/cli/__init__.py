"""
.. module: historical_reports.cli
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import logging

import click

from historical_reports.common.model_table import INDEX

logging.basicConfig()
log = logging.getLogger('historical-reports-cli')
log.setLevel(logging.DEBUG)


@click.group()
def cli():
    """Historical-Reports commandline for managing historical reports."""
    pass


def get_csv_fields(ctx, param, fields):
    return fields.split(",")


@cli.command()
@click.option("--buckets", type=click.STRING, required=True, help="Comma separated list of S3 bucket to dump the "
                                                                  "report to.", callback=get_csv_fields)
@click.option("--dump-prefix", type=click.STRING, required=True)
@click.option("--exclude-fields", type=click.STRING, required=False, default="Name,_version",
              help="Comma separated top-level fields to not be included in the final report.",
              callback=get_csv_fields)
@click.option("-c", "--commit", default=False, is_flag=True, help="Will only dump to S3 if commit flag is present")
def s3_report(buckets, dump_prefix, exclude_fields, commit):
    from historical_reports.s3.generate import dump_report

    log.info('[@] Generating S3 report...')

    if not commit:
        log.warning("[!] COMMIT FLAG NOT SET -- NOT SAVING ANYTHING TO S3!")

    import historical_reports.s3.models
    historical_reports.s3.models.EXCLUDE_FIELDS = exclude_fields
    dump_report(buckets, dump_prefix, commit=commit)


@cli.command()
@click.option("--stack", type=click.Choice(list(INDEX.keys())), required=True,
              help="The stack to generate the report for.")
@click.option('--account', type=click.STRING, required=True, help='The AWS Account ID to generate a report for.')
@click.option('--region', type=click.STRING, required=True, help='The region to dump the report for.')
@click.option("--buckets", type=click.STRING, required=True, help="Comma separated list of S3 bucket to dump the "
                                                                  "report to.", callback=get_csv_fields)
@click.option("--dump-prefix", type=click.STRING, required=True)
@click.option("--exclude-fields", type=click.STRING, required=False, default="Name,_version",
              help="Comma separated top-level fields to not be included in the final report.",
              callback=get_csv_fields)
@click.option("-c", "--commit", default=False, is_flag=True, help="Will only dump to S3 if commit flag is present")
def generate(stack, account, region, buckets, dump_prefix, exclude_fields, commit):
    # Setup the configuration variables:
    import historical_reports.common.constants
    historical_reports.common.constants.STACK = stack
    historical_reports.common.constants.EXCLUDE_FIELDS = exclude_fields
    historical_reports.common.constants.DUMP_TO_BUCKETS = buckets
    historical_reports.common.constants.DUMP_TO_PREFIX = dump_prefix

    log.info('[@] Generating report for {} in {}/{}...'.format(stack, account, region))

    if not commit:
        log.warning("[!] COMMIT FLAG NOT SET -- NOT SAVING ANYTHING TO S3!")

    from historical_reports.common.generate import dump_report
    dump_report(buckets, dump_prefix, account, region, commit=commit)
