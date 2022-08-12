import dataclasses
import boto3
import tempfile
import os.path
from datetime import datetime
from typing import List, Sequence, Iterator

s3 = boto3.client('s3')

class FileUpdateAction(dataclasses.dataclass):
    bucket: str
    key: str


def append_to_file(filepath):
    with open(filepath, 'a') as f:
        f.write("\n")
        f.write(datetime.now().isoformat())

def update_txt_file(bucket, key, filepath):
    s3.download_file(Bucket=bucket, Key=key, Filename=filepath)
    append_to_file(filepath)
    s3.upload_file(Bucket=bucket, Key=key, Filename=filepath)


def filter_buckets(buckets: Iterator[str], bucket_name_pattern: str) -> Iterator[str]:
    return (bucket for bucket in buckets if bucket_name_pattern in bucket)


def list_s3_keys(bucket: str) -> Iterator[str]:
    objects = s3.list_objects_v2(Bucket=bucket)['Contents']
    for o in objects:
        yield o['Key']


def filter_s3_keys(s3_keys: List[str], extension: str) -> Iterator[str]:
    return (k for k in s3_keys if os.path.splitext(k)[1].lower() == extension)


def determine_update_actions(bucket, s3_keys: List[str]) -> Iterator[FileUpdateAction]:  # Generator[FileUpdateAction, None, None]
    for key in filter_s3_keys(s3_keys, '.txt'):
        yield FileUpdateAction(bucket, key)


def update(bucket_name_pattern: str):
    buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    filtered_buckets = filter_buckets(buckets, bucket_name_pattern)
    for bucket in filtered_buckets:
        print(bucket)
        keys = list_s3_keys(bucket=bucket)
        for action in determine_update_actions(bucket, keys):
            update_txt_file()
        






if __name__ == "__main__":
    update('reactive-kata')
            