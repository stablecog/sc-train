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

    # Get blob clients for each container
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

    # Get all the blobs (files) in the caption container
    caption_blobs = caption_container_client.list_blobs()

    # Loop through each JSON blob in the caption container
    for blob in caption_blobs:
        blob_client = caption_container_client.get_blob_client(blob)
        json_str = blob_client.download_blob().readall().decode("utf-8")
        json_data = json.loads(json_str)

        # Extract text and corresponding image name
        caption_text = json_data["result"][0]["value"]["text"][0]
        image_name = os.path.basename(json_data["task"]["data"]["captioning"])

        # Determine the actual image name in the training-data bucket (JPEG or PNG)
        for ext in [".jpeg", ".jpg", ".png"]:
            image_blob_client = training_data_container_client.get_blob_client(
                image_name.replace(".jpeg", ext)
            )
            if image_blob_client.exists():
                actual_image_name = image_name.replace(".jpeg", ext)
                break

        # Download the image
        download_path = os.path.join(download_folder, actual_image_name)
        download_blob(image_blob_client, download_path)

        # Save the caption text to a .txt file
        txt_name = f"{os.path.splitext(actual_image_name)[0]}.txt"
        txt_path = os.path.join(download_folder, txt_name)
        with open(txt_path, "w") as f:
            f.write(caption_text)

    print("Download and processing completed.")


if __name__ == "__main__":
    main()
