from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import time
import argparse
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


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
                print("🚨 Observer is live...")
                time.sleep(30)
        except:
            self.observer.stop()
            print("💀 Observer stopped")


class Handler(FileSystemEventHandler):
    def process(self, event):
        blob_service_client = BlobServiceClient.from_connection_string(
            args.connection_string
        )

        if event.is_directory:
            for root, dirs, files in os.walk(event.src_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    blob_name = os.path.relpath(file_path, start=args.directory)
                    blob_client = blob_service_client.get_blob_client(
                        container=args.bucket, blob=blob_name
                    )

                    with open(file_path, "rb") as f:
                        blob_client.upload_blob(f)
                    print(f"✅ Uploaded: {blob_name}")
        else:
            file_path = event.src_path
            blob_name = os.path.basename(file_path)
            blob_client = blob_service_client.get_blob_client(
                container=args.bucket, blob=blob_name
            )

            with open(file_path, "rb") as f:
                blob_client.upload_blob(f)
            print(f"✅ Uploaded: {blob_name}")

    def on_created(self, event):
        self.process(event)


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

    w = Watcher(args.directory)
    w.run()
