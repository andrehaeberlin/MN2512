import unittest
import sys
import types
from unittest.mock import patch

if "dotenv" not in sys.modules:
    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = dotenv_stub

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

    def test_fallback_heuristico_para_subitens_de_nota(self):
        texto_nota = """
        DOCUMENTO AUX. DA NOTA FISCAL DE CONSUMIDOR ELETRÔNICA
        19/02/2026 07:50:34
        1000022036 CONDRES AH C/30 CAPS 157,740MG        R$ 234,90
        1 UN De R$ 234,90 por R$ 223,15
        Valor Liquido                                   R$ 223,15
        1000007820 PERMEAR C/30 CPR REV GAST 300MG      R$ 253,02
        1 UN De R$ 253,02 por R$ 151,81
        Valor Liquido                                   R$ 151,81
        Total                                           R$ 374,96
        Forma Pagamento CARTÃO DE CRÉDITO               R$ 374,96
        """
        resposta_mock = {
            "choices": [
                {
                    "message": {
                        "content": '[{"data": "2026-02-19", "valor": 374.96, "descricao": "Pagamento", "tipo": "saida", "document_type": "Saída"}]'
                    }
                }
            ]
        }

        with patch("llm_extractor._post_chat_completion", return_value=resposta_mock), patch.dict(
            "os.environ", {"OPENAI_API_KEY": "test-key"}, clear=False
        ):
            saida, erro = extrair_dados_financeiros_llm(texto_nota)

        self.assertIsNone(erro)
        self.assertEqual(len(saida), 2)
        self.assertEqual(saida[0]["descricao"], "CONDRES AH C/30 CAPS 157,740MG")
        self.assertEqual(saida[0]["valor"], 223.15)
        self.assertEqual(saida[1]["descricao"], "PERMEAR C/30 CPR REV GAST 300MG")
        self.assertEqual(saida[1]["valor"], 151.81)


if __name__ == "__main__":
    unittest.main()
