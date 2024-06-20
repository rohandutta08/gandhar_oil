import os
import boto3
import zipfile
import hashlib
import json
import io
import datetime
import tempfile
from botocore.exceptions import NoCredentialsError


class EltVcsManager:
    def __init__(self, current_path, elt_instance_path):
        self.bucket_name = "clear-elt-test"
        self.s3 = boto3.client('s3')
        self.current_path = current_path
        self.elt_instance_path = elt_instance_path
        self.whitelisted_folders = ['macros', 'models', 'seeds', 'config']
        self.whitelisted_files = ['dbt_project.yml', 'README.md']
        self.files = self.get_valid_files()

    def get_zip_name(self):
        return f"{self.elt_instance_path}.zip"

    def compare_directories(self, dir1, dir2):
        files_dir1 = {file for file in os.listdir(dir1) if os.path.isfile(os.path.join(dir1, file))}
        files_dir2 = {file for file in os.listdir(dir2) if os.path.isfile(os.path.join(dir2, file))}

        added_files = files_dir2 - files_dir1
        removed_files = files_dir1 - files_dir2
        common_files = files_dir1 & files_dir2

        # Report added and removed files
        for file in added_files:
            print(f"Added: {file}")
        for file in removed_files:
            print(f"Removed: {file}")

        # Report modified files
        for file in common_files:
            with open(os.path.join(dir1, file), 'r') as f1, open(os.path.join(dir2, file), 'r') as f2:
                if f1.read() != f2.read():
                    print(f"Modified: {file}")

    @staticmethod
    def extract_zip(zip_path, extract_to):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)

    def download_zip(self, version_id, is_temp=True):
        zip_name = self.get_zip_name()
        if is_temp:
            local_zip_path = os.path.join(tempfile.gettempdir(), zip_name)
        else:
            local_zip_path = os.path.join(self.current_path, zip_name)
        try:
            self.s3.download_file(self.bucket_name, self.get_s3_path(), local_zip_path,
                                  ExtraArgs={"VersionId": version_id})
            return local_zip_path
        except Exception as e:
            print(f"Error downloading file: {e}")
            return None

    def compare_zip_versions(self, version1, version2):
        zip1_path = self.download_zip(version1)
        zip2_path = self.download_zip(version2)

        with tempfile.TemporaryDirectory() as tempdir1, tempfile.TemporaryDirectory() as tempdir2:
            self.extract_zip(zip1_path, tempdir1)
            self.extract_zip(zip2_path, tempdir2)
            self.compare_directories(tempdir1, tempdir2)

        # Cleanup
        os.remove(zip1_path)
        os.remove(zip2_path)

    def get_s3_path(self):
        return f"{self.elt_instance_path}/{self.get_zip_name()}"

    def get_version_file(self):
        return f"{self.elt_instance_path}/version.json"

    def append_version_info(self, new_version_info):
        # Fetch the current version data
        try:
            s3_object = self.s3.get_object(Bucket=self.bucket_name, Key=self.get_version_file())
            version_data = s3_object['Body'].read().decode('utf-8')
            version_lines = version_data.splitlines() if version_data else []
        except self.s3.exceptions.NoSuchKey:
            print(f"Version file {self.get_version_file()} not found. Creating a new one.")
            version_lines = []

        # Convert new version information to a JSON string and append it as a new line
        version_lines.append(json.dumps(new_version_info))

        # Upload the updated version data in NDJSON format
        updated_version_data = '\n'.join(version_lines).encode('utf-8')
        self.s3.put_object(Bucket=self.bucket_name, Key=self.get_version_file(), Body=io.BytesIO(updated_version_data))
        print(f"Updated version file {self.get_version_file()} on S3.")

    def get_version_details(self):
        try:
            # Get the object from S3
            s3_object = self.s3.get_object(Bucket=self.bucket_name, Key=self.get_version_file())
            version_details = []

            # Stream the file content
            for line in s3_object['Body']._raw_stream:
                decoded_line = line.decode('utf-8')
                version_details.append(json.loads(decoded_line))

            return version_details
        except self.s3.exceptions.NoSuchKey:
            print(f"Version file {self.get_version_file()} not found in S3.")
        except json.JSONDecodeError:
            print("Error decoding JSON from the version file.")
        except Exception as e:
            print(f"An error occurred: {e}")

        return []

    def upload_zip(self, hash_value):
        file_path = self.get_zip_name()
        try:
            with open(self.get_zip_name(), 'rb') as file_data:
                response = self.s3.put_object(Bucket=self.bucket_name, Key=self.get_s3_path(), Body=file_data)

            version_id = response.get('VersionId')
            print(f"File {file_path} uploaded as {self.get_s3_path()}. {version_id}")
            return version_id
        except FileNotFoundError:
            print("The file was not found.")
        except NoCredentialsError:
            print("Credentials not available.")
        return None

    def get_valid_files(self):
        file_info = []

        for root, dirs, files in os.walk(self.current_path):
            in_whitelisted_folder = self.whitelisted_folders is not None and any(
                wf in root for wf in self.whitelisted_folders)

            for file in files:
                if in_whitelisted_folder or file in self.whitelisted_files:
                    file_path = os.path.join(root, file)
                    file_hash = self.calculate_file_hash(file_path)
                    relative_file_path = os.path.relpath(file_path, self.current_path)
                    file_info.append((relative_file_path, file_hash))

        file_info.sort()
        return file_info

    def create_zip(self):
        with zipfile.ZipFile(self.get_zip_name(), 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path, _ in self.files:  # We only need the file path, not the hash
                relative_file_path = os.path.relpath(file_path, self.current_path)
                zipf.write(file_path, relative_file_path)

    @staticmethod
    def calculate_file_hash(file_path):
        """Calculate the MD5 hash of a single file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def calculate_folder_hash(self):
        """Calculate a consistent hash for the folder based on file paths and their contents."""
        hash_md5 = hashlib.md5()

        for file_path, file_hash in self.files:
            hash_md5.update(f"{file_path}:{file_hash}".encode())

        return hash_md5.hexdigest()

    def list_versions_in_path(self):
        version_details = self.get_version_details()
        for version in version_details:
            print(version)

    def check_and_push_to_vcs(self):
        version_details = self.get_version_details()
        print(version_details)
        hash_value = self.calculate_folder_hash()
        if version_details and version_details[-1]["file_hash"] == hash_value:
            print("No changes detected. Skipping upload.")
            return
        zip_filename = f'{self.elt_instance_path}.zip'
        self.create_zip()
        version_id = self.upload_zip(hash_value)
        os.remove(zip_filename)
        new_version_info = {
            "s3_version": version_id,
            "file_hash": hash_value,
            "message": "New version uploaded",
            "created_by": "Your Name",
            "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        self.append_version_info(new_version_info)

    def pull(self, version_id):
        if version_id is None:
            version_details = self.get_version_details()
            version_id = version_details[-1]['s3_version']
        self.download_zip(version_id, is_temp=False)

    def generate_presign_url(self, version_id=None):
        if version_id is None:
            version_details = self.get_version_details()
            version_id = version_details[-1]['s3_version']

        presigned_url = self.s3.generate_presigned_url('get_object',
                                                       Params={'Bucket': self.bucket_name,
                                                               'Key': self.get_s3_path(),
                                                               'VersionId': version_id},
                                                       ExpiresIn=9600)  # URL expires in 1 hour
        print(presigned_url)

        return presigned_url

