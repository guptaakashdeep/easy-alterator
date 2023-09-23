"""Module for S3 related utilities."""

import boto3
from botocore.exceptions import ClientError
from utils.helper import get_aws_region

REGION = get_aws_region()


def _get_bucket_key(s3_path):
    """
    Gets the S3 bucket and key.
    :param s3_path: str
    :return: tuple
    """
    s3_path = s3_path.replace("s3://", "")
    s3_path_list = s3_path.split("/", 1)
    if len(s3_path_list) < 2:
        return "", ""
    s3_bucket = s3_path_list[0]
    s3_key = s3_path_list[1]
    return s3_bucket, s3_key


def validate_s3_object(s3_path):
    """
    Validates the S3 path.
    :param s3_path: str
    :return: bool
    """
    s3_bucket, s3_key = _get_bucket_key(s3_path)
    s3 = boto3.client("s3", region_name=REGION)
    try:
        # using list_object_v2 to validate instead of head_object because s3_key can be just path to folder like structure
        response = s3.list_objects_v2(
            Bucket=s3_bucket, Prefix=s3_key, Delimiter="/", MaxKeys=1
        )
    except ClientError:
        return False
    if response["KeyCount"] == 0:
        return False
    return True


def list_s3_objects(s3_path):
    """
    Lists all the objects in the S3 path.
    :param s3_path: str
    :return: list
    """
    s3_bucket, s3_key = _get_bucket_key(s3_path)
    s3 = boto3.client("s3", region_name=REGION)
    keylist = []
    kwargs = {"Bucket": s3_bucket}
    if isinstance(s3_key, str):
        kwargs["Prefix"] = s3_key
    while True:
        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        try:
            response = s3.list_objects_v2(**kwargs)
        except ClientError:
            return []
        # if no keys found, return empty
        if response["KeyCount"]:
            contents = response["Contents"]
            for obj in contents:
                key = obj["Key"]
                keylist.append(key)
        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs["ContinuationToken"] = response["NextContinuationToken"]
        except KeyError:
            break
    return keylist


def read_s3_file(s3_path):
    """
    Reads the S3 file.
    :param s3_path: str
    :return: str
    """
    s3_bucket, s3_key = _get_bucket_key(s3_path)
    s3 = boto3.client("s3", region_name=REGION)
    try:
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    except ClientError:
        return ""
    return response["Body"].read().decode("utf-8")
