import os, pathlib, logging,uuid
from azure.storage.blob import BlobServiceClient, __version__

package_directory = pathlib.Path(__file__).resolve().parent.parent.parent
os.chdir(package_directory)

def upload_file():
    """
    Upload file result.csv to Azure storage

    https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
    """
    try:
        logging.info(f"Azure Blob Storage v {__version__}")
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if connect_str:
            blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            container_name = str(uuid.uuid4())
            container_client = blob_service_client.create_container(container_name)
            if os.path.exists("result.csv"):
                blob_client = blob_service_client.get_blob_client(container=container_name, blob="result.csv")
                logging.info(f"Uploading to Azure Storage as blob:\n\t result.csv")
                with open("result.csv", "rb") as data:
                    blob_client.upload_blob(data)
                print(f"File uploaded to Azure in container {container_name}")
            else:
                logging.error("File result.csv is not available for upload")
                print("Unable to upload to Azure storage")
        else:
            logging.error(f"No Azure connection string found")
            print("Configure Azure connection string in environment variables")

    except Exception as e:
        logging.error(e)
        print("Error occured during result.csv upload to Azure storage")