#!/usr/bin/env python
# -*- coding: utf-8 -*-
import functools
import logging

import boto3
from botocore.exceptions import ClientError

from DSpace.exception import S3ClientError

logger = logging.getLogger(__name__)


def exception_wapper(fun):
    @functools.wraps(fun)
    def _wapper(*args, **kwargs):
        try:
            fun(*args, **kwargs)
        except ClientError as e:
            logger.warning("S3 client error: %s", e)
            raise S3ClientError(str(e))
    return _wapper


class S3Client(object):
    s3 = None

    def __init__(self, endpoint_url, access_key, secret_access_key):
        self.s3 = boto3.resource(
            's3', use_ssl=False, endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key)

    @exception_wapper
    def bucket_create(self, name, placement=None,
                      versioning=None, acls=None):
        kwargs = {}
        if placement:
            kwargs['CreateBucketConfiguration'] = {
                'LocationConstraint': placement
            }
        self.s3.create_bucket(Bucket=name, **kwargs)
        if acls:
            self.bucket_acl_set(name, acls)
        if versioning:
            self.bucket_versioning_set(name, enabled=versioning)

    def bucket_get(self, bucket):
        if isinstance(bucket, str):
            return self.s3.Bucket(bucket)
        else:
            return bucket

    def bucket_get_name(self, bucket):
        if isinstance(bucket, str):
            return bucket
        else:
            return bucket.name

    @exception_wapper
    def bucket_delete(self, bucket):
        bucket = self.bucket_get(bucket)
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()

    @exception_wapper
    def bucket_list(self):
        res = {}
        buckets = self.s3.buckets.all()
        for bucket in buckets:
            res[bucket.name] = {
                "name": bucket.name
            }
        return res

    @exception_wapper
    def bucket_lifecycle_get(self, bucket):
        bucket_name = self.bucket_get_name(bucket)
        bucket_lifecycle = self.s3.BucketLifecycleConfiguration(bucket_name)
        rules = []
        for rule in bucket_lifecycle.rules:
            days = rule['Expiration'].get('Days')
            date = rule['Expiration'].get('Date')
            _rule = {
                "ID": rule['ID'],
                "Filter": rule['Filter']['Prefix'],
                "Expiration": days or date,
                "Status": rule['Status'],
            }
            rules.append(_rule)
        return rules

    @exception_wapper
    def bucket_lifecycle_set(self, bucket, lifecycles):
        """Set Lifecycle

        example: [{
            "ID": "rule_name",
            "Filter": "prefix",
            "Expiration": datatime|int,
            "Status": 'Enabled'|'Disabled',
        }]
        """
        bucket_name = self.bucket_get_name(bucket)
        bucket_lifecycle = self.s3.BucketLifecycleConfiguration(bucket_name)

        # format rules
        rules = []
        for lifecycle in lifecycles:
            rule = {
                'Expiration': {},
                'ID': lifecycle['ID'],
                'Filter': {'Prefix': lifecycle['Filter']},
                'Status': lifecycle['Status']
            }
            expiration = lifecycle['Expiration']
            if isinstance(expiration, int):
                rule['Expiration']['Days'] = expiration
            else:
                rule['Expiration']['Date'] = expiration
            rules.append(rule)

        # set lifecycle
        bucket_lifecycle.put(
            LifecycleConfiguration={
                    'Rules': rules
            }
        )

    @exception_wapper
    def bucket_acl_get(self, bucket):
        bucket_name = self.bucket_get_name(bucket)
        bucket_acl = self.s3.BucketAcl(bucket_name)
        return bucket_acl.grants

    @exception_wapper
    def bucket_acl_set(self, bucket, acls):
        """Set bucket acl

        "Permission": 'FULL_CONTROL'|'WRITE'|'WRITE_ACP'|'READ'|'READ_ACP'
        "Type": "Group"|"CanonicalUser"
        "ID": "<userid>"
        "URI": "AuthenticatedUsers"|"AllUsers"

        example: [{
            "Type": "Group",
            "URI": "AuthenticatedUsers",
            "Permission": 'READ'
        },{
            "Type": "CanonicalUser",
            "ID": "testuser",
            "Permission": 'WRITE'
        }]
        """
        bucket_name = self.bucket_get_name(bucket)
        bucket_acl = self.s3.BucketAcl(bucket_name)

        # format acl
        grants = []
        for acl in acls:
            # check type
            t = acl.get('Type')
            if t not in ["Group", "CanonicalUser"]:
                raise ValueError("Type %s not allowed" % t)
            # check Permission
            p = acl.get('Permission')
            if p not in ['FULL_CONTROL', 'WRITE', 'WRITE_ACP', 'READ',
                         'READ_ACP']:
                raise ValueError("Permission %s not allowed" % p)
            grant = {
                'Grantee': {
                    'Type': t,
                },
                'Permission': p
            }
            if t == "CanonicalUser":
                grant['Grantee']["ID"] = acl.get('ID')
            else:
                # t == "Group"
                g = acl.get('URI')
                if g == "AllUsers":
                    uri = "http://acs.amazonaws.com/groups/global/AllUsers"
                    grant['Grantee']["URI"] = uri
                elif g == "AuthenticatedUsers":
                    uri = ("http://acs.amazonaws.com/groups/global/"
                           "AuthenticatedUsers")
                    grant['Grantee']["URI"] = uri
                else:
                    grant['Grantee']["URI"] = g
            grants.append(grant)

        # current user FULL_CONTROL
        grants.append({
            'Grantee': {
                'Type': "CanonicalUser",
                'ID': bucket_acl.owner['ID']

            },
            'Permission': "FULL_CONTROL"
        })

        # set acl
        bucket_acl.put(
            AccessControlPolicy={
                'Grants': grants,
                'Owner': {
                    'ID': bucket_acl.owner['ID']
                }
            },
        )

    @exception_wapper
    def bucket_versioning_set(self, bucket, enabled=False):
        bucket_name = self.bucket_get_name(bucket)
        bucket_versioning = self.s3.BucketVersioning(bucket_name)
        if enabled:
            bucket_versioning.enable()
        elif bucket_versioning.status == "Enabled":
            bucket_versioning.suspend()

    @exception_wapper
    def bucket_versioning_get(self, bucket):
        bucket_name = self.bucket_get_name(bucket)
        bucket_versioning = self.s3.BucketVersioning(bucket_name)
        return bucket_versioning.status


if __name__ == '__main__':
    import logging
    boto3.set_stream_logger('boto3.resources', logging.DEBUG)
    access_key = "CLN1BZ7UK5DB34BH7W1N"
    secret_access_key = "9MQPTeYbOZJqRcCTbj9uawvkbGPymb3pUkRLlClF"
    endpoint_url = 'http://192.168.210.21:7480'
    s3 = S3Client(endpoint_url, access_key, secret_access_key)
    bucket_name = "aaa9"
    acls = [{
        "Type": "Group",
        "URI": "AuthenticatedUsers",
        "Permission": 'READ'
    }, {
        "Type": "CanonicalUser",
        "ID": "testuser3",
        "Permission": 'WRITE'
    }]
    # create
    s3.bucket_create(bucket_name, acls=acls, versioning=True,
                     placement=":abc")
    # version
    print("version:")
    print(s3.bucket_versioning_get(bucket_name))
    # lifecycle
    lifecycle = [{
        "ID": "abc",
        "Filter": "prefix",
        "Expiration": 1,
        "Status": 'Enabled',
    }]
    s3.bucket_lifecycle_set(bucket_name, lifecycle)
    print("lifecycle:")
    print(s3.bucket_lifecycle_get(bucket_name))
    # delete
    print("delete:")
    s3.bucket_delete(bucket_name)
    print(s3.bucket_list())
