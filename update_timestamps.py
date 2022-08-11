import boto3
import tempfile
import os.path
from datetime import datetime

s3 = boto3.client('s3')

def append_to_file(filepath):
    with open(filepath, 'a') as f:
        f.write("\n")
        f.write(datetime.now().isoformat())


def update(bucket_name_pattern: str):
    buckets = s3.list_buckets()['Buckets']
    filtered_buckets = [bucket['Name'] for bucket in buckets if bucket_name_pattern in bucket['Name']]
    for bucket in filtered_buckets:
        print(bucket)
        objects = s3.list_objects_v2(Bucket=bucket)['Contents']
        with tempfile.TemporaryDirectory() as tmpdir:
            for s3_obj in objects:
                key = s3_obj['Key']
                file_ext = os.path.splitext(key)[1].lower()
                if file_ext == '.txt':
                    print('\t' + key)
                    filepath = os.path.join(tmpdir, os.path.basename(key))
                    s3.download_file(Bucket=bucket, Key=key, Filename=filepath)
                    append_to_file(filepath)
                    s3.upload_file(Bucket=bucket, Key=key, Filename=filepath)


if __name__ == "__main__":
    update('reactive-kata')
            