import ast
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace


class ResourceExistsError(Exception):
    pass


class ResourceNotFoundError(Exception):
    pass


class _ContentSettings:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


azure = ModuleType("azure")
azure_core = ModuleType("azure.core")
azure_core_exceptions = ModuleType("azure.core.exceptions")
azure_core_exceptions.ResourceExistsError = ResourceExistsError
azure_core_exceptions.ResourceNotFoundError = ResourceNotFoundError
azure_storage = ModuleType("azure.storage")
azure_storage_blob = ModuleType("azure.storage.blob")
azure_storage_blob.BlobServiceClient = SimpleNamespace(from_connection_string=lambda *_a, **_k: None)
azure_storage_blob.ContentSettings = _ContentSettings
sys.modules.setdefault("azure", azure)
sys.modules.setdefault("azure.core", azure_core)
sys.modules.setdefault("azure.core.exceptions", azure_core_exceptions)
sys.modules.setdefault("azure.storage", azure_storage)
sys.modules.setdefault("azure.storage.blob", azure_storage_blob)

from engine.blob_storage import (
    INPUTS_CONTAINER,
    METADATA_CONTAINER,
    OUTPUTS_CONTAINER,
    BlobStorage,
)
from engine.warehouse_context import warehouse_scope


class _FakeContainer:
    def __init__(self, names=()):
        self.names = set(names)

    def list_blobs(self, name_starts_with=None):
        prefix = str(name_starts_with or "")
        return [
            SimpleNamespace(name=name)
            for name in sorted(self.names)
            if not prefix or name.startswith(prefix)
        ]

    def delete_blobs(self, *names, **_kwargs):
        for name in names:
            self.names.discard(str(name))

    def delete_blob(self, name, **_kwargs):
        self.names.discard(str(name))


class _UndeletableContainer(_FakeContainer):
    def delete_blobs(self, *_names, **_kwargs):
        raise RuntimeError("delete rejected")

    def delete_blob(self, _name, **_kwargs):
        raise RuntimeError("delete rejected")


class _FakeLockBlob:
    def __init__(self, exists=False):
        self.exists = exists

    def upload_blob(self, *_args, overwrite=False, **_kwargs):
        if self.exists and not overwrite:
            raise ResourceExistsError("already exists")
        self.exists = True


class _FakeService:
    def __init__(self, names=(), lock_exists=False):
        self.containers = {
            INPUTS_CONTAINER: _FakeContainer(names),
            OUTPUTS_CONTAINER: _FakeContainer(names),
            METADATA_CONTAINER: _FakeContainer(names),
        }
        self.lock_blob = _FakeLockBlob(lock_exists)

    def get_container_client(self, name):
        return self.containers[name]

    def get_blob_client(self, container, blob):
        if container == METADATA_CONTAINER and blob.endswith("system/reset-active.json"):
            return self.lock_blob
        raise AssertionError(f"Unexpected Blob client: {container}/{blob}")


def _storage(names=(), lock_exists=False):
    storage = object.__new__(BlobStorage)
    storage.service = _FakeService(names, lock_exists)
    return storage


class WarehouseResetBlobTests(unittest.TestCase):
    def test_non_admin_warehouse_deletes_only_its_prefix(self):
        storage = _storage(("w1/old/run.json", "w2/current/run.json", "legacy/run.json"))
        with warehouse_scope(2, "Warehouse 2"):
            result = storage.delete_current_warehouse_data(max_passes=2)

        self.assertTrue(result["clean_state_verified"])
        self.assertEqual(result["remaining_blobs_total"], 0)
        for container in storage.service.containers.values():
            self.assertEqual(container.names, {"w1/old/run.json", "legacy/run.json"})

    def test_admin_warehouse_deletes_scoped_and_legacy_but_preserves_other_warehouses(self):
        names = (
            "w1/old/run.json",
            "w1/system/reset-active.json",
            "w1/background-jobs/RESET-1.json",
            "w2/current/run.json",
            "legacy/run.json",
        )
        storage = _storage(names)
        preserved = [
            "w1/system/reset-active.json",
            "w1/background-jobs/RESET-1.json",
        ]
        with warehouse_scope(1, "Madinah Warehouse"):
            result = storage.delete_current_warehouse_data(
                preserve_blob_names=preserved,
                max_passes=2,
            )

        self.assertTrue(result["clean_state_verified"])
        self.assertEqual(result["remaining_blobs_total"], 0)
        expected = {
            "w1/system/reset-active.json",
            "w1/background-jobs/RESET-1.json",
            "w2/current/run.json",
        }
        for container in storage.service.containers.values():
            self.assertEqual(container.names, expected)

    def test_reset_lock_creation_is_atomic(self):
        storage = _storage()
        with warehouse_scope(1, "Madinah Warehouse"):
            self.assertTrue(storage.try_create_warehouse_reset_lock("RESET-1", "Madinah"))
            self.assertFalse(storage.try_create_warehouse_reset_lock("RESET-2", "Madinah"))

    def test_remaining_blob_prevents_clean_state(self):
        storage = _storage()
        storage.service.containers[INPUTS_CONTAINER] = _UndeletableContainer(
            ("w2/current/run.json",)
        )
        with warehouse_scope(2, "Warehouse 2"):
            result = storage.delete_current_warehouse_data(max_passes=2)

        self.assertEqual(result["status"], "Incomplete")
        self.assertFalse(result["clean_state_verified"])
        self.assertEqual(result["remaining_blobs_total"], 1)


class WarehouseResetAuthorizationTests(unittest.TestCase):
    def test_reset_routes_require_authenticated_warehouse_user(self):
        source = (Path(__file__).resolve().parents[1] / "function_app.py").read_text(
            encoding="utf-8"
        )
        tree = ast.parse(source)
        required = {
            "warehouse_data_reset_route",
            "warehouse_data_reset_recover_route",
        }
        functions = {
            node.name: node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name in required
        }
        self.assertEqual(set(functions), required)
        for name, node in functions.items():
            guard_calls = [
                call
                for call in ast.walk(node)
                if isinstance(call, ast.Call)
                and isinstance(call.func, ast.Name)
                and call.func.id == "_auth_guard"
            ]
            self.assertTrue(guard_calls, f"{name} must require authentication")
            self.assertTrue(
                all(not call.keywords for call in guard_calls),
                f"{name} must remain available to authenticated warehouse users",
            )


if __name__ == "__main__":
    unittest.main()
