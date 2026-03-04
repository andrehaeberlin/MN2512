import os
import tempfile
import unittest

import localDB


class TestExtractionGating(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.old_ingest = localDB.INGEST_DB_NAME
        localDB.INGEST_DB_NAME = os.path.join(self.tmpdir.name, "ingestao_test.db")
        localDB.init_ingest_db()

        self.old_regex = localDB.extrair_dados_financeiros
        self.old_llm = localDB.extrair_dados_financeiros_llm

    def tearDown(self):
        localDB.INGEST_DB_NAME = self.old_ingest
        localDB.extrair_dados_financeiros = self.old_regex
        localDB.extrair_dados_financeiros_llm = self.old_llm
        self.tmpdir.cleanup()

    def test_regex_good_quality_does_not_call_llm(self):
        calls = {"llm": 0}

        def regex_ok(_text):
            return [
                {"data": "2026-01-01", "descricao": f"Item {i}", "valor": float(i), "categoria": "Outros", "tipo": "saida"}
                for i in range(1, 8)
            ]

        def llm_never(_text):
            calls["llm"] += 1
            return [], "should not be called"

        localDB.extrair_dados_financeiros = regex_ok
        localDB.extrair_dados_financeiros_llm = llm_never

        result = localDB.extract_transactions(text="texto")
        self.assertEqual(result.method, "regex")
        self.assertGreaterEqual(result.metrics.confidence, localDB.MIN_CONF)
        self.assertEqual(calls["llm"], 0)

    def test_regex_low_quality_calls_llm(self):
        def regex_bad(_text):
            return [{"data": "", "descricao": "Sem data", "valor": 10.0, "categoria": "Outros", "tipo": "saida"}]

        def llm_ok(_text):
            return [
                {"data": "2026-01-01", "descricao": "A", "valor": 10.0, "categoria": "Outros", "tipo": "saida"},
                {"data": "2026-01-02", "descricao": "B", "valor": 12.0, "categoria": "Outros", "tipo": "saida"},
                {"data": "2026-01-03", "descricao": "C", "valor": 14.0, "categoria": "Outros", "tipo": "saida"},
                {"data": "2026-01-04", "descricao": "D", "valor": 16.0, "categoria": "Outros", "tipo": "saida"},
                {"data": "2026-01-05", "descricao": "E", "valor": 18.0, "categoria": "Outros", "tipo": "saida"},
            ], None

        localDB.extrair_dados_financeiros = regex_bad
        localDB.extrair_dados_financeiros_llm = llm_ok

        result = localDB.extract_transactions(text="texto")
        self.assertEqual(result.method, "llm")
        self.assertIsNotNone(result.reason)
        self.assertGreaterEqual(result.metrics.valid_items, 5)


    def test_llm_cache_roundtrip_by_text_hash(self):
        payload = [{"data": "2026-01-01", "descricao": "Item", "valor": 9.9, "categoria": "Outros", "tipo": "saida"}]
        payload_uri = os.path.join(self.tmpdir.name, "llm_payload.json")
        with open(payload_uri, "w", encoding="utf-8") as handler:
            import json
            json.dump(payload, handler)

        text_hash = localDB.compute_text_hash("texto qualquer")
        localDB._save_llm_cache(text_hash, payload_uri, llm_model="gpt-test")
        cached = localDB._get_llm_cached_payload(text_hash)

        self.assertIsNotNone(cached)
        cached_payload, cached_uri, cached_model = cached
        self.assertEqual(cached_payload, payload)
        self.assertEqual(cached_uri, payload_uri)
        self.assertEqual(cached_model, "gpt-test")


if __name__ == "__main__":
    unittest.main()
