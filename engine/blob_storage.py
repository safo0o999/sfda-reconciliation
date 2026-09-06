import os
import json
import base64
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings


INPUTS_CONTAINER = "runs-inputs"
OUTPUTS_CONTAINER = "runs-outputs"
METADATA_CONTAINER = "runs-metadata"
RESET_LOCK_BLOB = "system/reset-active.json"

logger = logging.getLogger("SFDA-Reconciliation.BlobStorage")


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

    @staticmethod
    def warehouse_prefix() -> str:
        from engine.warehouse_context import current_warehouse_id
        return f"w{int(current_warehouse_id())}"

    @classmethod
    def scoped_blob_name(cls, blob_name: str) -> str:
        clean = str(blob_name or "").lstrip("/")
        prefix = cls.warehouse_prefix() + "/"
        return clean if clean.startswith(prefix) else prefix + clean

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
            self.scoped_blob_name(f"{run_number}/{safe_name}"),
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
            self.scoped_blob_name(f"{run_number}/{safe_name}"),
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
            self.scoped_blob_name(f"{run_number}/{safe_name}"),
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
        prefix = self.scoped_blob_name(f"{run_number}/")
        rows: List[Dict[str, Any]] = []
        for blob in container.list_blobs(name_starts_with=prefix):
            settings = getattr(blob, "content_settings", None)
            rows.append({
                "container": container_name,
                "blob_name": blob.name,
                "file_name": blob.name[len(prefix):],
                "size_bytes": int(blob.size or 0),
                "content_type": settings.content_type if settings and settings.content_type else "application/octet-stream",
                "last_modified": blob.last_modified,
            })
        from engine.warehouse_context import current_warehouse_id
        if int(current_warehouse_id()) == 1:
            legacy_prefix = f"{run_number}/"
            for blob in container.list_blobs(name_starts_with=legacy_prefix):
                settings = getattr(blob, "content_settings", None)
                rows.append({
                    "container": container_name,
                    "blob_name": blob.name,
                    "file_name": blob.name[len(legacy_prefix):],
                    "size_bytes": int(blob.size or 0),
                    "content_type": settings.content_type if settings and settings.content_type else "application/octet-stream",
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

    def list_run_numbers(
        self,
        limit: int = 500,
        *,
        include_fallback_containers: bool = False,
    ) -> List[str]:
        """List run numbers efficiently.

        Normal History discovery uses only runs-metadata. Older deployments can
        explicitly request the slower fallback scan across output/input containers.
        This removes two full Blob listings from every ordinary History refresh.
        """
        run_numbers = set()
        containers = [METADATA_CONTAINER]
        if include_fallback_containers:
            containers.extend([OUTPUTS_CONTAINER, INPUTS_CONTAINER])

        from engine.warehouse_context import current_warehouse_id
        warehouse_id = int(current_warehouse_id())
        prefix = self.warehouse_prefix() + "/"

        for container_name in containers:
            container = self.service.get_container_client(container_name)
            for blob in container.list_blobs(name_starts_with=prefix):
                name = str(blob.name or "")
                relative = name[len(prefix):]
                if "/" not in relative:
                    continue
                run_number = relative.split("/", 1)[0].strip()
                if run_number and run_number != "background-jobs":
                    run_numbers.add(run_number)

            if warehouse_id == 1:
                for blob in container.list_blobs():
                    name = str(blob.name or "")
                    if name.startswith("w") and "/" in name:
                        continue
                    if "/" not in name:
                        continue
                    run_number = name.split("/", 1)[0].strip()
                    if run_number and run_number != "background-jobs":
                        run_numbers.add(run_number)

        return sorted(run_numbers, reverse=True)[: max(1, int(limit))]

    @staticmethod
    def _chunks(values: List[str], size: int = 256):
        for index in range(0, len(values), size):
            yield values[index:index + size]

    @staticmethod
    def _delete_blob_names(container, names: List[str]) -> int:
        """Delete Blob names in Azure batch requests with a safe fallback."""
        deleted = 0
        for chunk in BlobStorage._chunks(names, 256):
            if not chunk:
                continue
            try:
                container.delete_blobs(*chunk, delete_snapshots="include")
                deleted += len(chunk)
            except Exception:
                # Some storage accounts / SDK combinations can reject one item in
                # a batch. Fall back only for that batch instead of failing reset.
                for blob_name in chunk:
                    try:
                        container.delete_blob(blob_name, delete_snapshots="include")
                        deleted += 1
                    except ResourceNotFoundError:
                        pass
        return deleted

    def cleanup_expired_inputs(self, retention_hours: int = 24) -> Dict[str, Any]:
        """Delete only uploaded input Blob bytes older than the retention window.

        SQL run/file records, runs-metadata, and generated outputs are deliberately
        preserved so History remains auditable after the original WMS/SFDA upload
        expires.
        """
        safe_hours = max(1, int(retention_hours))
        cutoff = datetime.now(timezone.utc) - timedelta(hours=safe_hours)
        container = self.service.get_container_client(INPUTS_CONTAINER)
        names = [
            str(blob.name)
            for blob in container.list_blobs()
            if blob.last_modified is not None and blob.last_modified < cutoff
        ]
        deleted = self._delete_blob_names(container, names)
        return {
            "status": "Completed",
            "retention_hours": safe_hours,
            "cutoff_utc": cutoff.isoformat(),
            "deleted_input_blobs": deleted,
        }

    def delete_input_blob_names(self, blob_names: List[str]) -> int:
        """Delete explicitly identified input blobs after successful processing.

        Callers pass only Blob names returned by ``upload_input`` or
        ``upload_job_input``.  Output and metadata containers are never touched.
        The warehouse prefix check prevents one warehouse from deleting another
        warehouse's uploaded files.
        """
        from engine.warehouse_context import current_warehouse_id

        warehouse_id = int(current_warehouse_id())
        scoped_prefix = self.warehouse_prefix() + "/"
        safe_names: List[str] = []
        for value in blob_names or []:
            blob_name = str(value or "").strip()
            if not blob_name:
                continue
            if blob_name.startswith(scoped_prefix):
                safe_names.append(blob_name)
                continue
            # Warehouse 1 predates warehouse-scoped Blob paths. Preserve support
            # for its legacy run inputs while keeping all other warehouses strict.
            if warehouse_id == 1 and not blob_name.lower().startswith("w"):
                safe_names.append(blob_name)

        if not safe_names:
            return 0
        container = self.service.get_container_client(INPUTS_CONTAINER)
        return self._delete_blob_names(container, sorted(set(safe_names)))


    def write_background_job_status(
        self,
        job_id: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Persist warehouse-scoped status/result for an async reconciliation job."""
        import json

        safe_job_id = self.sanitize_file_name(job_id)
        data = json.dumps(
            payload,
            ensure_ascii=False,
            default=str,
            allow_nan=False,
        ).encode("utf-8")
        return self.upload_bytes(
            METADATA_CONTAINER,
            self.scoped_blob_name(f"background-jobs/{safe_job_id}.json"),
            data,
            "application/json; charset=utf-8",
            {"job_id": safe_job_id, "category": "background-job-status"},
        )

    def read_background_job_status(self, job_id: str) -> Dict[str, Any]:
        """Read one warehouse-scoped async reconciliation job status."""
        import json

        safe_job_id = self.sanitize_file_name(job_id)
        downloaded = self.download_blob(
            METADATA_CONTAINER,
            self.scoped_blob_name(f"background-jobs/{safe_job_id}.json"),
        )
        parsed = json.loads(downloaded["data"].decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("Background job status is invalid.")

        # Reset jobs must heartbeat while running. A Function App restart can
        # stop the queue invocation while the persisted status blob still says
        # Running forever. Detect that condition from the blob Last-Modified
        # timestamp, mark the job Interrupted, and release its reset lock.
        status = str(parsed.get("status") or "").strip().lower()
        job_type = str(parsed.get("job_type") or "").strip().lower()
        if job_type == "warehouse_reset" and status in {"queued", "running"}:
            stale_minutes = max(
                2,
                int(os.getenv("WAREHOUSE_RESET_STALE_MINUTES", "10") or 10),
            )
            last_modified = downloaded.get("last_modified")
            if last_modified is not None:
                if last_modified.tzinfo is None:
                    last_modified = last_modified.replace(tzinfo=timezone.utc)
                age = datetime.now(timezone.utc) - last_modified
                if age >= timedelta(minutes=stale_minutes):
                    parsed = {
                        **parsed,
                        "status": "Failed",
                        "progress": 100,
                        "current_stage": "Reset interrupted or worker restarted",
                        "completed_at": datetime.now(timezone.utc).isoformat(),
                        "error": (
                            "Reset stopped updating for "
                            f"{int(age.total_seconds() // 60)} minute(s). "
                            "The previous worker is treated as interrupted. "
                            "It is safe to start Reset again; committed reset batches are idempotent."
                        ),
                    }
                    self.write_background_job_status(job_id, parsed)
                    try:
                        self.clear_warehouse_reset_lock(job_id, force=True)
                    except Exception:
                        logger.exception("Unable to clear stale warehouse reset lock for %s", job_id)
        return parsed

    def list_active_background_jobs(
        self,
        exclude_job_id: str = "",
        max_age_hours: int = 48,
    ) -> List[Dict[str, Any]]:
        """List recent queued/running background reconciliation jobs for this warehouse."""
        prefix = self.scoped_blob_name("background-jobs/")
        container = self.service.get_container_client(METADATA_CONTAINER)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(max_age_hours)))
        active: List[Dict[str, Any]] = []
        excluded = str(exclude_job_id or "").strip()

        for blob in container.list_blobs(name_starts_with=prefix):
            if blob.last_modified is not None and blob.last_modified < cutoff:
                continue
            name = str(blob.name or "")
            if not name.endswith(".json"):
                continue
            job_id = name.rsplit("/", 1)[-1][:-5]
            if excluded and job_id == self.sanitize_file_name(excluded):
                continue
            try:
                downloaded = self.download_blob(METADATA_CONTAINER, name)
                payload = json.loads(downloaded["data"].decode("utf-8"))
            except Exception:
                logger.exception("Unable to inspect background job status %s", name)
                continue
            status = str((payload or {}).get("status") or "").strip().lower()
            if status in {"queued", "running"}:
                active.append(payload)
        return active

    def write_warehouse_reset_lock(
        self,
        job_id: str,
        warehouse_name: str,
        expires_hours: int = 6,
    ) -> Dict[str, Any]:
        """Create/update the warehouse-scoped reset lock marker."""
        created_at = datetime.now(timezone.utc)
        payload = {
            "job_id": str(job_id),
            "warehouse_name": str(warehouse_name or ""),
            "status": "Active",
            "created_at": created_at.isoformat(),
            "expires_at": (created_at + timedelta(hours=max(1, int(expires_hours)))).isoformat(),
        }
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        return self.upload_bytes(
            METADATA_CONTAINER,
            self.scoped_blob_name(RESET_LOCK_BLOB),
            data,
            "application/json; charset=utf-8",
            {"job_id": str(job_id), "category": "warehouse-reset-lock"},
        )

    def try_create_warehouse_reset_lock(
        self,
        job_id: str,
        warehouse_name: str,
        expires_hours: int = 6,
    ) -> bool:
        """Atomically acquire the warehouse reset lock without overwriting an owner."""
        created_at = datetime.now(timezone.utc)
        payload = {
            "job_id": str(job_id),
            "warehouse_name": str(warehouse_name or ""),
            "status": "Active",
            "created_at": created_at.isoformat(),
            "expires_at": (
                created_at + timedelta(hours=max(1, int(expires_hours)))
            ).isoformat(),
        }
        blob = self.service.get_blob_client(
            container=METADATA_CONTAINER,
            blob=self.scoped_blob_name(RESET_LOCK_BLOB),
        )
        try:
            blob.upload_blob(
                json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                overwrite=False,
                content_settings=ContentSettings(
                    content_type="application/json; charset=utf-8"
                ),
                metadata={"job_id": str(job_id), "category": "warehouse-reset-lock"},
            )
            return True
        except ResourceExistsError:
            return False

    def read_warehouse_reset_lock(self) -> Optional[Dict[str, Any]]:
        """Return the active reset lock, automatically ignoring stale locks."""
        try:
            downloaded = self.download_blob(
                METADATA_CONTAINER,
                self.scoped_blob_name(RESET_LOCK_BLOB),
            )
        except (FileNotFoundError, ResourceNotFoundError):
            return None

        try:
            payload = json.loads(downloaded["data"].decode("utf-8"))
        except Exception:
            logger.exception("Invalid warehouse reset lock payload; treating it as active for safety.")
            return {"status": "Active", "job_id": "unknown"}

        # If the owning Reset job was interrupted/restarted, release the lock
        # immediately instead of keeping the warehouse blocked for hours.
        # IMPORTANT: do not depend on the persisted status payload containing
        # job_type=warehouse_reset. Older reset status blobs may not contain
        # that field, which previously allowed a stale lock to survive forever.
        lock_job_id = str(payload.get("job_id") or "").strip()
        if lock_job_id:
            try:
                safe_job_id = self.sanitize_file_name(lock_job_id)
                status_blob_name = self.scoped_blob_name(
                    f"background-jobs/{safe_job_id}.json"
                )
                status_download = self.download_blob(METADATA_CONTAINER, status_blob_name)
                status_payload = json.loads(status_download["data"].decode("utf-8"))
                status_value = str((status_payload or {}).get("status") or "").strip().lower()

                if status_value not in {"queued", "running"}:
                    self.clear_warehouse_reset_lock(lock_job_id, force=True)
                    return None

                stale_minutes = max(
                    2,
                    int(os.getenv("WAREHOUSE_RESET_STALE_MINUTES", "10") or 10),
                )
                last_modified = status_download.get("last_modified")
                if last_modified is not None:
                    if last_modified.tzinfo is None:
                        last_modified = last_modified.replace(tzinfo=timezone.utc)
                    age = datetime.now(timezone.utc) - last_modified
                    if age >= timedelta(minutes=stale_minutes):
                        interrupted = {
                            **(status_payload if isinstance(status_payload, dict) else {}),
                            "job_id": lock_job_id,
                            "job_type": "warehouse_reset",
                            "status": "Failed",
                            "progress": 100,
                            "current_stage": "Reset interrupted or worker restarted",
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "error": (
                                "Reset stopped updating for "
                                f"{int(age.total_seconds() // 60)} minute(s). "
                                "The previous worker is treated as interrupted. "
                                "Committed reset batches are safe to resume."
                            ),
                        }
                        self.write_background_job_status(lock_job_id, interrupted)
                        self.clear_warehouse_reset_lock(lock_job_id, force=True)
                        return None
            except FileNotFoundError:
                # A lock without its owning status record cannot represent a
                # live reset worker. Release it so the warehouse can recover.
                self.clear_warehouse_reset_lock(lock_job_id, force=True)
                return None
            except Exception:
                logger.exception("Unable to inspect reset job while evaluating reset lock.")

        expires_at = str(payload.get("expires_at") or "").strip()
        if expires_at:
            try:
                expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                if expiry.tzinfo is None:
                    expiry = expiry.replace(tzinfo=timezone.utc)
                if expiry <= datetime.now(timezone.utc):
                    self.clear_warehouse_reset_lock(str(payload.get("job_id") or ""), force=True)
                    return None
            except Exception:
                logger.exception("Unable to evaluate warehouse reset lock expiry.")
        return payload if isinstance(payload, dict) else None

    def clear_warehouse_reset_lock(self, job_id: str = "", force: bool = False) -> None:
        """Remove the reset lock only for the owning job unless force=True."""
        blob_name = self.scoped_blob_name(RESET_LOCK_BLOB)
        if not force and job_id:
            current = self.read_warehouse_reset_lock()
            if current and str(current.get("job_id") or "") not in {"", str(job_id)}:
                return
        try:
            self.service.get_blob_client(
                container=METADATA_CONTAINER,
                blob=blob_name,
            ).delete_blob(delete_snapshots="include")
        except ResourceNotFoundError:
            pass

    def upload_job_input(
        self,
        job_id: str,
        category: str,
        file_name: str,
        file_bytes: bytes,
        content_type: str = "application/octet-stream",
    ) -> Dict[str, Any]:
        """Upload one historical-build input using a collision-safe path."""

        safe_category = self.sanitize_file_name(category).lower()
        safe_name = self.sanitize_file_name(file_name)
        blob_name = self.scoped_blob_name(f"{job_id}/{safe_category}/{safe_name}")

        result = self.upload_bytes(
            INPUTS_CONTAINER,
            blob_name,
            file_bytes,
            content_type,
            {
                "job_id": str(job_id),
                "category": safe_category,
                "file_name": safe_name,
            },
        )
        result["file_name"] = safe_name
        result["category"] = safe_category
        return result

    @staticmethod
    def _upload_block_id(chunk_index: int) -> str:
        """Return a fixed-width Azure Block Blob id for one browser chunk."""
        index = int(chunk_index)
        if index < 0:
            raise ValueError("chunk_index must be zero or greater.")
        return base64.b64encode(f"{index:08d}".encode("ascii")).decode("ascii")

    def stage_job_input_block(
        self,
        job_id: str,
        category: str,
        file_name: str,
        chunk_index: int,
        data: bytes,
    ) -> Dict[str, Any]:
        """Stage one retry-safe browser upload chunk without buffering the file."""
        safe_category = self.sanitize_file_name(category).lower()
        safe_name = self.sanitize_file_name(file_name)
        blob_name = self.scoped_blob_name(f"{job_id}/{safe_category}/{safe_name}")
        block_id = self._upload_block_id(chunk_index)
        blob = self.service.get_blob_client(
            container=INPUTS_CONTAINER,
            blob=blob_name,
        )
        blob.stage_block(block_id=block_id, data=data)
        return {
            "container": INPUTS_CONTAINER,
            "blob_name": blob_name,
            "file_name": safe_name,
            "category": safe_category,
            "block_id": block_id,
            "size_bytes": len(data),
        }

    def commit_job_input_blocks(
        self,
        job_id: str,
        category: str,
        file_name: str,
        block_count: int,
        content_type: str = "application/octet-stream",
    ) -> Dict[str, Any]:
        """Commit all staged chunks and return the normal job-input manifest row."""
        safe_category = self.sanitize_file_name(category).lower()
        safe_name = self.sanitize_file_name(file_name)
        count = int(block_count)
        if count < 1:
            raise ValueError("block_count must be at least one.")
        blob_name = self.scoped_blob_name(f"{job_id}/{safe_category}/{safe_name}")
        blob = self.service.get_blob_client(
            container=INPUTS_CONTAINER,
            blob=blob_name,
        )
        blob.commit_block_list(
            [self._upload_block_id(index) for index in range(count)],
            content_settings=ContentSettings(content_type=content_type),
            metadata={
                "job_id": str(job_id),
                "category": safe_category,
                "file_name": safe_name,
            },
        )
        properties = blob.get_blob_properties()
        return {
            "container": INPUTS_CONTAINER,
            "blob_name": blob_name,
            "file_name": safe_name,
            "category": safe_category,
            "size_bytes": int(properties.size or 0),
            "content_type": (
                properties.content_settings.content_type
                or content_type
            ),
            "etag": str(properties.etag or ""),
            "last_modified": properties.last_modified,
        }

    def upload_job_output(
        self,
        job_id: str,
        file_name: str,
        file_bytes: bytes,
        content_type: str = "application/octet-stream",
    ) -> Dict[str, Any]:
        """Upload one historical-build output."""

        safe_name = self.sanitize_file_name(file_name)
        result = self.upload_bytes(
            OUTPUTS_CONTAINER,
            self.scoped_blob_name(f"{job_id}/{safe_name}"),
            file_bytes,
            content_type,
            {
                "job_id": str(job_id),
                "category": "historical-output",
                "file_name": safe_name,
            },
        )
        result["file_name"] = safe_name
        return result

    def delete_current_warehouse_data(
        self,
        preserve_blob_names: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
        max_passes: int = 3,
    ) -> Dict[str, Any]:
        """Delete and verify Blob data owned by the current warehouse.

        Reference/configuration files are not stored in these run containers, so
        GLN mappings, Pack Size and business rules are never touched here.
        Warehouse 1 additionally owns legacy unscoped run archives created
        before Multi-Warehouse, so an explicitly confirmed Warehouse 1 reset
        removes those legacy run blobs while preserving every other wN/ prefix.
        """
        from engine.warehouse_context import current_warehouse_id

        warehouse_id = int(current_warehouse_id())
        if warehouse_id < 1:
            raise RuntimeError("A valid WarehouseID is required for Blob reset.")

        prefix = f"w{warehouse_id}/"
        preserved = {str(name) for name in (preserve_blob_names or []) if str(name).strip()}
        deleted: Dict[str, int] = {}
        failed: Dict[str, List[str]] = {}
        remaining: Dict[str, List[str]] = {}

        def is_target(name: str) -> bool:
            if not name or name in preserved:
                return False
            if name.startswith(prefix):
                return True
            # Warehouse 1 owns pre-Multi-Warehouse run archives. A valid wN/
            # prefix always belongs to that explicit warehouse and is preserved.
            return warehouse_id == 1 and re.match(r"^w\d+/", name) is None

        def target_names(container) -> List[str]:
            if warehouse_id != 1:
                return [
                    str(blob.name)
                    for blob in container.list_blobs(name_starts_with=prefix)
                    if is_target(str(blob.name or ""))
                ]
            # Admin/Warehouse 1 must also remove legacy unscoped archives, so a
            # full listing is unavoidable until those archives are gone. Use one
            # linear scan and a set instead of the previous two-list/O(n^2) path.
            return sorted({
                str(blob.name)
                for blob in container.list_blobs()
                if is_target(str(blob.name or ""))
            })

        def emit(container_name: str, deleted_count: int, pending_count: int) -> None:
            if progress_callback is None:
                return
            try:
                progress_callback(container_name, int(deleted_count), int(pending_count))
            except Exception:
                logger.exception("Warehouse Blob reset progress callback failed.")

        for container_name in (
            INPUTS_CONTAINER,
            OUTPUTS_CONTAINER,
            METADATA_CONTAINER,
        ):
            container = self.service.get_container_client(container_name)
            container_deleted = 0
            container_failed: List[str] = []
            passes = max(1, min(int(max_passes or 1), 5))
            for pass_number in range(1, passes + 1):
                names = target_names(container)
                emit(container_name, container_deleted, len(names))
                if not names:
                    break
                pass_deleted = 0
                for chunk in self._chunks(names, 256):
                    if not chunk:
                        continue
                    try:
                        container.delete_blobs(*chunk, delete_snapshots="include")
                        container_deleted += len(chunk)
                        pass_deleted += len(chunk)
                    except Exception:
                        for blob_name in chunk:
                            try:
                                container.delete_blob(blob_name, delete_snapshots="include")
                                container_deleted += 1
                                pass_deleted += 1
                            except ResourceNotFoundError:
                                pass
                            except Exception:
                                logger.exception(
                                    "Warehouse reset could not delete blob %s/%s",
                                    container_name,
                                    blob_name,
                                )
                                container_failed.append(blob_name)
                    emit(container_name, container_deleted, max(0, len(names) - pass_deleted))

                logger.info(
                    "Warehouse Blob reset pass completed. warehouse_id=%s container=%s pass=%s deleted=%s",
                    warehouse_id,
                    container_name,
                    pass_number,
                    container_deleted,
                )

            deleted[container_name] = container_deleted
            if container_failed:
                failed[container_name] = sorted(set(container_failed))
            final_remaining = target_names(container)
            if final_remaining:
                remaining[container_name] = final_remaining
            emit(container_name, container_deleted, len(final_remaining))

        return {
            "status": "Incomplete" if remaining else "Completed",
            "warehouse_id": warehouse_id,
            "deleted_blobs": deleted,
            "deleted_blobs_total": int(sum(deleted.values())),
            "failed_blobs": failed,
            "failed_blobs_total": int(sum(len(items) for items in failed.values())),
            "remaining_blobs": remaining,
            "remaining_blobs_total": int(sum(len(items) for items in remaining.values())),
            "clean_state_verified": not bool(remaining),
            "legacy_unscoped_madinah_blobs_deleted": warehouse_id == 1,
        }

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
