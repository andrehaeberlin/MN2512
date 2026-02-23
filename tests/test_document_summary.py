import json
import os
import sys
import tempfile
import types
import unittest

if "dotenv" not in sys.modules:
    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = dotenv_stub

import localDB


class TestDocumentSummaryFinalize(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.old_db = localDB.DB_NAME
        self.old_ingest = localDB.INGEST_DB_NAME

        localDB.DB_NAME = os.path.join(self.tmpdir.name, "dados_financeiros_test.db")
        localDB.INGEST_DB_NAME = os.path.join(self.tmpdir.name, "ingestao_test.db")

        localDB.init_db()
        localDB.init_ingest_db()

        self.document_id = "doc-123"
        review_dir = os.path.join(self.tmpdir.name, "review")
        os.makedirs(review_dir, exist_ok=True)
        self.review_path = os.path.join(review_dir, "approved.json")

        payload = [
            {"data": "2026-02-19", "descricao": "Produto A", "valor": 223.15, "tipo": "saida", "categoria": "Outros"},
            {"data": "2026-02-19", "descricao": "Produto B", "valor": 151.81, "tipo": "saida", "categoria": "Outros"},
            {"data": "2026-02-19", "descricao": "Total", "valor": 374.96, "tipo": "saida", "categoria": "Outros"},
        ]
        with open(self.review_path, "w", encoding="utf-8") as handler:
            json.dump(payload, handler, ensure_ascii=False)

        with localDB.sqlite3.connect(localDB.INGEST_DB_NAME) as conn:
            conn.execute(
                "INSERT INTO documents (id, sha256, original_name, mime, size_bytes, storage_uri_raw, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (self.document_id, "sha-test", "nota.jpeg", "image/jpeg", 10, "raw://test", localDB.STATUS_FINALIZE_PENDING),
            )
            conn.execute(
                "INSERT INTO reviews (document_id, reviewer, decision, edited_payload_uri, notes) VALUES (?, ?, ?, ?, ?)",
                (self.document_id, "tester", "APPROVED", self.review_path, "ok"),
            )

    def tearDown(self):
        localDB.DB_NAME = self.old_db
        localDB.INGEST_DB_NAME = self.old_ingest
        self.tmpdir.cleanup()

    def test_finaliza_e_popula_resumo_e_itens(self):
        ok, msg = localDB.finalize_document(self.document_id)
        self.assertTrue(ok, msg)

        df_resumos = localDB.get_document_summaries()
        self.assertEqual(len(df_resumos), 1)
        resumo = df_resumos.iloc[0]
        self.assertEqual(resumo["document_id"], self.document_id)
        self.assertEqual(resumo["qtd_itens"], 2)
        self.assertAlmostEqual(float(resumo["total_itens"]), 374.96, places=2)
        self.assertAlmostEqual(float(resumo["total_declarado"]), 374.96, places=2)
        self.assertEqual(int(resumo["total_confere"]), 1)

        df_itens = localDB.get_document_items(self.document_id)
        self.assertEqual(len(df_itens), 2)
        descricoes = set(df_itens["descricao"].tolist())
        self.assertEqual(descricoes, {"Produto A", "Produto B"})


if __name__ == "__main__":
    unittest.main()
