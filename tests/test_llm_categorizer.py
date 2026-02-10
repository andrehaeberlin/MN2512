import unittest
from unittest.mock import patch

from llm_extractor import categorizar_transacoes_llm


class TestCategorizacaoLLM(unittest.TestCase):
    def test_categoriza_por_indice(self):
        transacoes = [
            {"data": "2026-01-12", "valor": 85.90, "descricao": "Almoco Restaurante"},
            {"data": "2026-01-13", "valor": 29.00, "descricao": "Uber Centro"},
        ]
        resposta_mock = {
            "choices": [
                {
                    "message": {
                        "content": '[{"index": 0, "categoria": "Alimentação"}, {"index": 1, "categoria": "Transporte"}]'
                    }
                }
            ]
        }

        with patch("llm_extractor._post_chat_completion", return_value=resposta_mock), patch.dict(
            "os.environ", {"OPENAI_API_KEY": "test-key"}, clear=False
        ):
            saida, erro = categorizar_transacoes_llm(transacoes)

        self.assertIsNone(erro)
        self.assertEqual(saida[0]["categoria"], "Alimentação")
        self.assertEqual(saida[1]["categoria"], "Transporte")

    def test_fallback_para_outros_em_categoria_invalida(self):
        transacoes = [{"data": "2026-01-12", "valor": 10.0, "descricao": "Compra aleatoria"}]
        resposta_mock = {
            "choices": [
                {
                    "message": {
                        "content": '[{"index": 0, "categoria": "Viagem"}]'
                    }
                }
            ]
        }

        with patch("llm_extractor._post_chat_completion", return_value=resposta_mock), patch.dict(
            "os.environ", {"OPENAI_API_KEY": "test-key"}, clear=False
        ):
            saida, erro = categorizar_transacoes_llm(transacoes)

        self.assertIsNone(erro)
        self.assertEqual(saida[0]["categoria"], "Outros")

    def test_sem_chave_api_nao_bloqueia_fluxo(self):
        transacoes = [{"data": "2026-01-12", "valor": 10.0, "descricao": "Conta"}]

        with patch.dict("os.environ", {}, clear=True):
            saida, erro = categorizar_transacoes_llm(transacoes)

        self.assertEqual(saida, transacoes)
        self.assertIsNotNone(erro)


if __name__ == "__main__":
    unittest.main()
