import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings


INPUTS_CONTAINER = "runs-inputs"
OUTPUTS_CONTAINER = "runs-outputs"
METADATA_CONTAINER = "runs-metadata"


class BlobStorage:
    def __init__(self) -> None:
        connection_string = (
            os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            or os.getenv("AzureWebJobsStorage")
        )

        if not connection_string:
            raise RuntimeError(
                "AZURE_STORAGE_CONNECTION_STRING or AzureWebJobsStorage is missing."
            )

        self.service = BlobServiceClient.from_connection_string(
            connection_string
        )

    def initialize_containers(self) -> None:
        for container_name in (
            INPUTS_CONTAINER,
            OUTPUTS_CONTAINER,
            METADATA_CONTAINER,
        ):
            container = self.service.get_container_client(container_name)
            try:
                container.create_container()
            except Exception as ex:
                message = str(ex).lower()
                if (
                    "containeralreadyexists" not in message
                    and "already exists" not in message
                ):
                    raise

    @staticmethod
    def sanitize_file_name(file_name: str) -> str:
        name = str(file_name or "file").replace("\\", "_").replace("/", "_").strip()
        return name or "file"

    def upload_bytes(
        self,
        container_name: str,
        blob_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        blob = self.service.get_blob_client(
            container=container_name,
            blob=blob_name,
        )
        blob.upload_blob(
            data,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
            metadata=metadata,
        )
        properties = blob.get_blob_properties()
        return {
            "container": container_name,
            "blob_name": blob_name,
            "size_bytes": int(properties.size or 0),
            "content_type": (
                properties.content_settings.content_type
                or content_type
            ),
            "etag": str(properties.etag or ""),
            "last_modified": properties.last_modified,
        }

    def upload_input(
        self,
        run_number: str,
        file_name: str,
        file_bytes: bytes,
        content_type: str = "application/octet-stream",
    ) -> Dict[str, Any]:
        safe_name = self.sanitize_file_name(file_name)
        return self.upload_bytes(
            INPUTS_CONTAINER,
            f"{run_number}/{safe_name}",
            file_bytes,
            content_type,
            {"run_number": run_number, "category": "input"},
        )

    def upload_output(
        self,
        run_number: str,
        file_name: str,
        file_bytes: bytes,
        content_type: str = "application/octet-stream",
    ) -> Dict[str, Any]:
        safe_name = self.sanitize_file_name(file_name)
        return self.upload_bytes(
            OUTPUTS_CONTAINER,
            f"{run_number}/{safe_name}",
            file_bytes,
            content_type,
            {"run_number": run_number, "category": "output"},
        )

    def upload_metadata(
        self,
        run_number: str,
        file_bytes: bytes,
        file_name: str = "run.json",
    ) -> Dict[str, Any]:
        safe_name = self.sanitize_file_name(file_name)
        return self.upload_bytes(
            METADATA_CONTAINER,
            f"{run_number}/{safe_name}",
            file_bytes,
            "application/json; charset=utf-8",
            {"run_number": run_number, "category": "metadata"},
        )

    def download_blob(
        self,
        container_name: str,
        blob_name: str,
    ) -> Dict[str, Any]:
        blob = self.service.get_blob_client(
            container=container_name,
            blob=blob_name,
        )
        try:
            properties = blob.get_blob_properties()
            data = blob.download_blob().readall()
        except ResourceNotFoundError as ex:
            raise FileNotFoundError(
                f"Blob not found: {container_name}/{blob_name}"
            ) from ex

        return {
            "data": data,
            "content_type": (
                properties.content_settings.content_type
                or "application/octet-stream"
            ),
            "size_bytes": int(properties.size or len(data)),
            "etag": str(properties.etag or ""),
            "last_modified": properties.last_modified,
        }

    def list_run_files(
        self,
        run_number: str,
        container_name: str,
    ) -> List[Dict[str, Any]]:
        container = self.service.get_container_client(container_name)
        prefix = f"{run_number}/"
        rows: List[Dict[str, Any]] = []
        for blob in container.list_blobs(name_starts_with=prefix):
            settings = getattr(blob, "content_settings", None)
            rows.append({
                "container": container_name,
                "blob_name": blob.name,
                "file_name": blob.name[len(prefix):],
                "size_bytes": int(blob.size or 0),
                "content_type": (
                    settings.content_type
                    if settings and settings.content_type
                    else "application/octet-stream"
                ),
                "last_modified": blob.last_modified,
            })
        return rows

    def list_all_run_files(self, run_number: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for category, container_name in (
            ("input", INPUTS_CONTAINER),
            ("output", OUTPUTS_CONTAINER),
            ("metadata", METADATA_CONTAINER),
        ):
            for row in self.list_run_files(run_number, container_name):
                row["category"] = category
                rows.append(row)
        return sorted(
            rows,
            key=lambda row: (
                row.get("category", ""),
                row.get("file_name", ""),
            ),
        )

    def list_run_numbers(self, limit: int = 500) -> List[str]:
        run_numbers = set()
        for container_name in (
            METADATA_CONTAINER,
            OUTPUTS_CONTAINER,
            INPUTS_CONTAINER,
        ):
            container = self.service.get_container_client(container_name)
            for blob in container.list_blobs():
                name = str(blob.name or "")
                if "/" not in name:
                    continue
                run_number = name.split("/", 1)[0].strip()
                if run_number:
                    run_numbers.add(run_number)
        return sorted(run_numbers, reverse=True)[: max(1, int(limit))]

    def health(self) -> Dict[str, Any]:
        self.initialize_containers()
        account_name = self.service.account_name
        return {
            "status": "Connected",
            "account": account_name,
            "containers": [
                INPUTS_CONTAINER,
                OUTPUTS_CONTAINER,
                METADATA_CONTAINER,
            ],
            "utc": datetime.now(timezone.utc).isoformat(),
        }
