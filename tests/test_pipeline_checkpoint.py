import io
import os
import tempfile
import unittest

from PIL import Image

import localDB


class TestPipelineCheckpoint(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.old_db = localDB.DB_NAME
        self.old_ingest = localDB.INGEST_DB_NAME
        localDB.DB_NAME = os.path.join(self.tmpdir.name, "dados_test.db")
        localDB.INGEST_DB_NAME = os.path.join(self.tmpdir.name, "ingest_test.db")
        localDB.init_db()
        localDB.init_ingest_db()

        self.old_ocr = localDB.extrair_texto_imagem
        self.old_extract = localDB.extract_transactions
        self.old_checks = localDB._run_llm_checks

        self.calls = {"ocr": 0, "extract": 0}

        def fake_ocr(_upload):
            self.calls["ocr"] += 1
            return "01/01/2026 MERCADO 10,00", 0.01, None

        def fake_extract(text=None, df=None):
            self.calls["extract"] += 1
            payload = [{"data": "2026-01-01", "descricao": "Mercado", "valor": 10.0, "categoria": "Outros", "tipo": "saida"}]
            metrics = localDB._compute_extraction_metrics(payload)
            return localDB.ExtractionResult(method="regex", payload=payload, metrics=metrics, reason=None)

        localDB.extrair_texto_imagem = fake_ocr
        localDB.extract_transactions = fake_extract
        localDB._run_llm_checks = lambda payload: {"passed": True, "confidence": 0.99, "issues": [], "summary": {"count": len(payload)}}

    def tearDown(self):
        localDB.extrair_texto_imagem = self.old_ocr
        localDB.extract_transactions = self.old_extract
        localDB._run_llm_checks = self.old_checks
        localDB.DB_NAME = self.old_db
        localDB.INGEST_DB_NAME = self.old_ingest
        self.tmpdir.cleanup()

    def _make_png_bytes(self):
        img = Image.new("RGB", (200, 120), color=(255, 255, 255))
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()

    def test_pipeline_is_checkpointed_and_idempotent(self):
        doc = localDB.store_raw_document("nota.png", "image/png", self._make_png_bytes(), storage_root=self.tmpdir.name)

        ok1, _ = localDB.run_pipeline_for_document(doc["id"])
        self.assertTrue(ok1)

        docs = {d["id"]: d for d in localDB.list_ingest_documents()}
        current = docs[doc["id"]]
        self.assertEqual(current["status"], localDB.STATUS_HITL_REVIEW)
        self.assertTrue(current["text_uri"] and os.path.exists(current["text_uri"]))
        self.assertTrue(current["extraction_uri"] and os.path.exists(current["extraction_uri"]))

        ocr_calls = self.calls["ocr"]
        extract_calls = self.calls["extract"]

        ok2, _ = localDB.run_pipeline_for_document(doc["id"])
        self.assertTrue(ok2)
        self.assertEqual(self.calls["ocr"], ocr_calls)
        self.assertEqual(self.calls["extract"], extract_calls)

    def test_resume_from_text_extracted_skips_ocr(self):
        doc = localDB.store_raw_document("nota2.png", "image/png", self._make_png_bytes(), storage_root=self.tmpdir.name)
        ok1, _ = localDB.run_pipeline_for_document(doc["id"])
        self.assertTrue(ok1)

        localDB.reset_document_to_stage(doc["id"], localDB.STATUS_TEXT_EXTRACTED)

        before = self.calls["ocr"]
        ok2, _ = localDB.run_pipeline_for_document(doc["id"])
        self.assertTrue(ok2)
        self.assertEqual(self.calls["ocr"], before)

    def test_error_records_failed_stage(self):
        def bad_extract(text=None, df=None):
            raise RuntimeError("falha proposital")

        localDB.extract_transactions = bad_extract
        doc = localDB.store_raw_document("nota3.png", "image/png", self._make_png_bytes(), storage_root=self.tmpdir.name)

        ok, _ = localDB.run_pipeline_for_document(doc["id"])
        self.assertFalse(ok)

        current = {d["id"]: d for d in localDB.list_ingest_documents()}[doc["id"]]
        self.assertEqual(current["status"], localDB.STATUS_ERROR_PROCESSING)
        self.assertEqual(current["failed_stage"], "STRUCTURED_EXTRACTION")
        self.assertIn("falha proposital", current["error_message"])


if __name__ == "__main__":
    unittest.main()
