import os
import time
import argparse
from azure.storage.blob import BlobServiceClient

FILE_EXTENSIONS = [".safetensors", ".json"]


def upload_files(directory, blob_service_client, bucket, known_files):
    for root, _, files in os.walk(directory):
        for file in files:
            if not any(file.endswith(ext) for ext in FILE_EXTENSIONS):
                continue

            file_path = os.path.join(root, file)
            file_stat = os.stat(file_path)
            file_key = (file_path, file_stat.st_size, file_stat.st_mtime)

            if file_key in known_files:
                continue

            blob_name = os.path.basename(file_path)
            blob_client = blob_service_client.get_blob_client(
                container=bucket, blob=blob_name
            )

            if blob_client.exists():
                print(f"{blob_name} already exists in Azure Blob. Skipping.")
                continue

            with open(file_path, "rb") as f:
                blob_client.upload_blob(f)

            print(f"Uploaded {blob_name}")
            known_files.add(file_key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=f"Watch a directory and upload new files with specified extensions to Azure Blob Storage."
    )
    parser.add_argument(
        "-c", "--connection-string", required=True, help="Azure connection string."
    )
    parser.add_argument(
        "-b", "--bucket", required=True, help="Azure Blob Storage container name."
    )
    parser.add_argument("-d", "--directory", required=True, help="Directory to watch.")

    args = parser.parse_args()
    blob_service_client = BlobServiceClient.from_connection_string(
        args.connection_string
    )

    known_files = set()

    while True:
        print("Observer is live.")
        upload_files(args.directory, blob_service_client, args.bucket, known_files)
        time.sleep(30)
