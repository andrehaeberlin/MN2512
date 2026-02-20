import unittest
from unittest.mock import patch

from llm_extractor import extrair_dados_financeiros_llm


class TestExtracaoLLM(unittest.TestCase):
    def test_adiciona_document_type_quando_ausente(self):
        resposta_mock = {
            "choices": [
                {
                    "message": {
                        "content": '[{"data": "2026-01-12", "valor": 1200.0, "descricao": "Pagamento", "tipo": "saida"}]'
                    }
                }
            ]
        }

        with patch("llm_extractor._post_chat_completion", return_value=resposta_mock), patch.dict(
            "os.environ", {"OPENAI_API_KEY": "test-key"}, clear=False
        ):
            saida, erro = extrair_dados_financeiros_llm("texto qualquer")

        self.assertIsNone(erro)
        self.assertEqual(saida[0]["document_type"], "Extrato")

    def test_normaliza_document_type_invalido(self):
        resposta_mock = {
            "choices": [
                {
                    "message": {
                        "content": '[{"data": "2026-01-12", "valor": 1200.0, "descricao": "Recebimento", "tipo": "entrada", "document_type": "recibo"}]'
                    }
                }
            ]
        }

        with patch("llm_extractor._post_chat_completion", return_value=resposta_mock), patch.dict(
            "os.environ", {"OPENAI_API_KEY": "test-key"}, clear=False
        ):
            saida, erro = extrair_dados_financeiros_llm("texto qualquer")

        self.assertIsNone(erro)
        self.assertEqual(saida[0]["document_type"], "Extrato")

    def test_preserva_document_type_valido(self):
        resposta_mock = {
            "choices": [
                {
                    "message": {
                        "content": '[{"data": "2026-01-12", "valor": 1200.0, "descricao": "Recebimento", "tipo": "entrada", "document_type": "Saída"}]'
                    }
                }
            ]
        }

        with patch("llm_extractor._post_chat_completion", return_value=resposta_mock), patch.dict(
            "os.environ", {"OPENAI_API_KEY": "test-key"}, clear=False
        ):
            saida, erro = extrair_dados_financeiros_llm("texto qualquer")

        self.assertIsNone(erro)
        self.assertEqual(saida[0]["document_type"], "Saída")


if __name__ == "__main__":
    unittest.main()
