import os
import time
import argparse
from azure.storage.blob import BlobServiceClient

FILE_EXTENSIONS = [".safetensors", ".anotherextension"]


def upload_files(directory, blob_service_client, bucket, known_files):
    container_client = blob_service_client.get_container_client(bucket)
    existing_blobs = {blob.name for blob in container_client.list_blobs()}

    for root, _, files in os.walk(directory):
        for file in files:
            if not any(file.endswith(ext) for ext in FILE_EXTENSIONS):
                continue

            file_path = os.path.join(root, file)
            blob_name = os.path.basename(file_path)

            if blob_name in existing_blobs:
                print(f"{blob_name} already exists in Azure Blob. Skipping.")
                known_files.add(blob_name)
                continue

            if blob_name in known_files:
                continue

            blob_client = container_client.get_blob_client(blob_name)
            with open(file_path, "rb") as f:
                blob_client.upload_blob(f)

            print(f"Uploaded {blob_name}")
            known_files.add(blob_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Watch a directory and upload new files to Azure Blob Storage."
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
