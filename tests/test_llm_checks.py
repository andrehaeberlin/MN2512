import unittest

from localDB import _run_llm_checks


class TestLLMChecks(unittest.TestCase):
    def _rules(self, result):
        return {issue["rule"] for issue in result["issues"]}

    def test_payload_valido_sem_issues(self):
        payload = [
            {
                "data": "2026-01-12",
                "valor": 85.9,
                "descricao": "Almoço restaurante",
                "tipo": "saida",
            }
        ]

        result = _run_llm_checks(payload)

        self.assertTrue(result["passed"])
        self.assertEqual(result["issues"], [])
        self.assertEqual(result["summary"]["entrada"]["count"], 0)
        self.assertEqual(result["summary"]["saida"]["count"], 1)

    def test_detecta_campos_ausentes_e_invalidos(self):
        payload = [
            {
                "data": "",
                "valor": 0,
                "descricao": "",
                "tipo": "",
            }
        ]

        result = _run_llm_checks(payload)
        rules = self._rules(result)

        self.assertFalse(result["passed"])
        self.assertIn("missing_date", rules)
        self.assertIn("missing_type", rules)
        self.assertIn("missing_description", rules)
        self.assertIn("zero_value", rules)

    def test_detecta_ruido_e_tipo_invalido(self):
        payload = [
            {
                "data": "2026-02-30",
                "valor": 10.0,
                "descricao": "RECIBO AUTENTICACAO 123 TERMINAL TM-001 PROTOCOLO XYZ",
                "tipo": "despesa",
            }
        ]

        result = _run_llm_checks(payload)
        rules = self._rules(result)

        self.assertFalse(result["passed"])
        self.assertIn("invalid_date", rules)
        self.assertIn("invalid_type", rules)
        self.assertIn("description_noise", rules)


if __name__ == "__main__":
    unittest.main()
