import boto3
import json
import os
import requests


class S3Utils:
    def __init__(self):
        self.s3 = boto3.resource("s3")

    def get_config_from_json(self, s3_path):
        bucket = s3_path.split("/")[2]
        key = "/".join(s3_path.split("/")[3:])
        content_object = self.s3.Object(bucket, key)
        file_content = content_object.get()["Body"].read().decode("utf-8")
        schema_json = json.loads(file_content)
        return schema_json

    def download_file_from_s3(self, s3_path, file_name):
        bucket = s3_path.split("/")[5]
        key = "/".join(s3_path.split("/")[6:])
        print(file_name)
        self.s3.meta.client.download_file(
            Bucket=bucket, Key=key, Filename="input/" + file_name + ".csv"
        )

    def download_file_from_s3_v2(self, s3_path, local_path):
        bucket = s3_path.split("/")[5]
        key = "/".join(s3_path.split("/")[6:])
        print(local_path)
        self.s3.meta.client.download_file(
            Bucket=bucket, Key=key, Filename=local_path
        )

    def upload_file_to_s3(self, file_path, bucket, prefix):
        self.s3.meta.client.upload_file(file_path, bucket, prefix)
        
    def download_presigned_url_from_s3(presigned_url, local_folder):
    
        filename = os.path.basename(presigned_url)
        split_results = filename.split('?X-Amz-')
        local_file_name = split_results[0]
        local_filepath = os.path.join(local_folder, local_file_name)
        
        if not os.path.exists(local_folder):
            os.makedirs(local_folder)
        
        with requests.get(presigned_url, stream=True) as response:
            if response.status_code == 200:
                with open(local_filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return local_filepath
            else:
                print(f"Failed to download file: HTTP status code {response.status_code}")    
