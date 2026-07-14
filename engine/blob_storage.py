import os
import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient


class BlobStorage:

    def __init__(self):
        connection_string = (
            os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            or os.getenv("AzureWebJobsStorage")
        )

        self.client = BlobServiceClient.from_connection_string(connection_string)

        self.inputs = self.client.get_container_client("runs-inputs")
        self.outputs = self.client.get_container_client("runs-outputs")
        self.metadata = self.client.get_container_client("runs-metadata")

    def upload_input(self, run_number, file_name, file_bytes):
        blob = self.inputs.get_blob_client(f"{run_number}/{file_name}")
        blob.upload_blob(file_bytes, overwrite=True)
        return blob.url

    def upload_output(self, run_number, file_name, file_bytes):
        blob = self.outputs.get_blob_client(f"{run_number}/{file_name}")
        blob.upload_blob(file_bytes, overwrite=True)
        return blob.url

    def upload_metadata(self, run_number, data):
        blob = self.metadata.get_blob_client(f"{run_number}/run.json")
        blob.upload_blob(
            json.dumps(data, indent=4, default=str),
            overwrite=True
        )
        return blob.url

    def list_outputs(self, run_number):
        return [
            blob.name
            for blob in self.outputs.list_blobs(name_starts_with=f"{run_number}/")
        ]

    def health(self):
        return {
            "connected": True,
            "utc": datetime.utcnow().isoformat()
        }
