from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import time
import argparse
from azure.storage.blob import BlobServiceClient

FILE_EXTENSION = ".safetensors"


class Watcher:
    def __init__(self, directory_to_watch):
        self.DIRECTORY_TO_WATCH = directory_to_watch
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                print("Observer is live.")
                time.sleep(30)
        except KeyboardInterrupt:
            self.observer.stop()
            print("Observer stopped")


class Handler(FileSystemEventHandler):
    def process(self, event):
        blob_service_client = BlobServiceClient.from_connection_string(
            args.connection_string
        )

        if event.is_directory:
            for root, dirs, files in os.walk(event.src_path):
                for file in files:
                    if not file.endswith(FILE_EXTENSION):
                        continue

                    file_path = os.path.join(root, file)
                    blob_name = os.path.basename(file_path)
                    blob_client = blob_service_client.get_blob_client(
                        container=args.bucket, blob=blob_name
                    )

                    if blob_client.exists():
                        print(f"{blob_name} already exists in Azure Blob. Skipping.")
                        continue

                    with open(file_path, "rb") as f:
                        blob_client.upload_blob(f)
                    print(f"Uploaded {blob_name}")
        else:
            if not event.src_path.endswith(FILE_EXTENSION):
                return

            file_path = event.src_path
            blob_name = os.path.basename(file_path)
            blob_client = blob_service_client.get_blob_client(
                container=args.bucket, blob=blob_name
            )

            if blob_client.exists():
                print(f"{blob_name} already exists in Azure Blob. Skipping.")
                return

            with open(file_path, "rb") as f:
                blob_client.upload_blob(f)
            print(f"Uploaded {blob_name}")


def initial_upload(directory, blob_service_client, bucket):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if not file.endswith(FILE_EXTENSION):
                continue

            file_path = os.path.join(root, file)
            blob_name = os.path.basename(file_path)
            blob_client = blob_service_client.get_blob_client(
                container=bucket, blob=blob_name
            )

            if blob_client.exists():
                print(f"{blob_name} already exists in Azure Blob. Skipping.")
                continue

            with open(file_path, "rb") as f:
                blob_client.upload_blob(f)
            print(f"Initial upload of {blob_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=f"Watch a directory and upload new {FILE_EXTENSION} files to Azure Blob Storage."
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
    initial_upload(args.directory, blob_service_client, args.bucket)

    w = Watcher(args.directory)
    w.run()
