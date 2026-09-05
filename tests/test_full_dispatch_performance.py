import unittest
from pathlib import Path


class FullDispatchPerformanceTests(unittest.TestCase):
    def test_chunk_block_ids_are_fixed_width_and_ordered(self):
        source = Path("engine/blob_storage.py").read_text(encoding="utf-8")

        self.assertIn('f"{index:08d}".encode("ascii")', source)
        self.assertIn("blob.stage_block(block_id=block_id, data=data)", source)
        self.assertIn("blob.commit_block_list(", source)

    def test_full_dispatch_uses_chunked_upload_and_lightweight_outputs(self):
        source = Path("function_app.py").read_text(encoding="utf-8")
        web = Path("web/index.html").read_text(encoding="utf-8")

        self.assertIn("full-reconciliation/dispatch-upload/start", source)
        self.assertIn("stage_job_input_block", source)
        self.assertIn("commit_job_input_blocks", source)
        self.assertIn("return_output_manifest=True", source)
        self.assertIn('"outputs": lightweight_outputs', source)
        self.assertIn("queueChunkedFullDispatch", web)
        self.assertIn("file.downloadUrl", web)

    def test_dispatch_ledgers_keep_openjson_and_safe_fallback(self):
        source = Path("engine/database.py").read_text(encoding="utf-8")

        self.assertIn("OPENJSON Full Dispatch ledger save failed", source)
        self.assertIn("OPENJSON Cancel Dispatch ledger save failed", source)
        self.assertGreaterEqual(
            source.count("MERGE dbo.FullDispatchTransactions WITH (HOLDLOCK)"),
            4,
        )

    def test_post_cutover_dispatch_is_reserved_at_batch_and_customer_level(self):
        database = Path("engine/database.py").read_text(encoding="utf-8")
        engine = Path("engine/full_reconciliation.py").read_text(encoding="utf-8")
        migration = Path("sql/004_full_dispatch_cutover.sql").read_text(encoding="utf-8")

        self.assertIn("SubmittedQuantityPack AS [Reserved Full Dispatch Quantity Pack]", database)
        self.assertIn('target["_Open Reserved Dispatch Pack"]', engine)
        self.assertIn("FullDispatchCutoverBaseline", migration)
        self.assertIn("IX_FullDispatchTransactions_CutoverBatch", migration)


if __name__ == "__main__":
    unittest.main()
