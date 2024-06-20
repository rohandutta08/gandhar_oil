def get_s3_files_from_task_config(task_config):
    request = task_config["request"]
    file_info = []
    for table in request["fileInfo"]:
        for file in request["fileInfo"][table]:
            file_info.append(
                {
                    "table_name": table,
                    "s3_url": file["s3FileUrl"],
                    "file_name": file["userFileName"],
                }
            )
    return file_info
