"""
.. module: historical_reports.common.model_table
    :platform: Unix
    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
from historical.security_group.models import CurrentSecurityGroupModel, DurableSecurityGroupModel


CURRENT_TABLE = {
    'securitygroup': CurrentSecurityGroupModel
}

DURABLE_TABLE = {
    'securitygroup': DurableSecurityGroupModel
}

# This is the primary name field for a given technology stack:
NAME_FIELDS = {
    'securitygroup': 'GroupId'
}

# Field to keep tab of technology and their plural index in the JSON dataset:
INDEX = {
    'securitygroup': 'securitygroups'
}
