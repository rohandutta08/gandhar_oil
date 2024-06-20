import os
import requests

def download_file_from_s3(presigned_url, local_folder):
    
        filename = os.path.basename(presigned_url)
        split_results = filename.split('?X-Amz-')
        local_file_name = split_results[0]
        # Construct local filepath in the desired directory
        local_filepath = os.path.join(local_folder, local_file_name)
        
        if not os.path.exists(local_folder):
            os.makedirs(local_folder)
        
        # Download file from the presigned URL
        with requests.get(presigned_url, stream=True) as response:
            # Check if the request was successful
            if response.status_code == 200:
                # Write the file to the local folder
                with open(local_filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return local_filepath
            else:
                print(f"Failed to download file: HTTP status code {response.status_code}") 