from azure.storage.blob import BlobServiceClient
import json
import os
import argparse


def main():
    # Initialize argument parser
    parser = argparse.ArgumentParser(
        description="Download data from Azure blob containers and combine them."
    )
    parser.add_argument(
        "-p",
        "--project_name",
        required=True,
        help="Name of the project, used as prefix for blob containers.",
    )
    parser.add_argument(
        "-c",
        "--connection_string",
        required=True,
        help="Azure Blob Storage connection string.",
    )

    # Parse arguments
    args = parser.parse_args()
    project_name = args.project_name
    connection_str = args.connection_string

    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_str)

    # Names of the blob containers
    caption_container_name = f"{project_name}-target"
    training_data_container_name = f"{project_name}-training-data"

    # Parent folder
    parent_folder = "training-projects"

    # Folder to store the downloaded data
    download_folder = f"{parent_folder}/{project_name}-training-data"

    # Create the parent and download folders if they don't exist
    os.makedirs(download_folder, exist_ok=True)

    # Get container clients for each container
    caption_container_client = blob_service_client.get_container_client(
        caption_container_name
    )
    training_data_container_client = blob_service_client.get_container_client(
        training_data_container_name
    )

    # Helper function to download blob
    def download_blob(blob_client, download_path):
        blob_data = blob_client.download_blob()
        with open(download_path, "wb") as f:
            f.write(blob_data.readall())

    # Iterate over the caption blobs
    for blob_info in caption_container_client.list_blobs():
        blob_client = caption_container_client.get_blob_client(blob_info)
        json_str = blob_client.download_blob().readall().decode("utf-8")
        json_data = json.loads(json_str)

        # Get the image file name from the JSON blob
        captioning_data = json_data["task"]["data"]["captioning"]
        image_name = captioning_data.split("/")[-1]
        base_name, _ = os.path.splitext(image_name)

        # Search for the image blob in the training data container with possible extensions
        for ext in ["png", "jpg", "jpeg"]:
            image_blob_client = training_data_container_client.get_blob_client(
                f"{base_name}.{ext}"
            )

            if image_blob_client.exists():
                # Download the image
                download_blob(
                    image_blob_client,
                    os.path.join(download_folder, f"{base_name}.{ext}"),
                )
                break

        # Extract the caption text and write it to a .txt file
        caption_text = json_data["result"][0]["value"]["text"][0]
        with open(os.path.join(download_folder, f"{base_name}.txt"), "w") as f:
            f.write(caption_text)

    print("Download and processing completed.")


if __name__ == "__main__":
    main()
