import json
import os
import urllib.request
import urllib.error


def extrair_dados_financeiros_llm(texto_bruto):
    """
    Faz fallback de extração usando um LLM quando o regex falhar.
    Retorna (lista_de_dados, erro).
    """
    if not texto_bruto:
        return [], "Texto vazio para extração via LLM."

    api_key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return [], "Chave de API não configurada para extração via LLM."

    api_base = os.getenv("LLM_API_BASE", "https://api.openai.com/v1/chat/completions")
    model = os.getenv("LLM_MODEL", "gpt-4o-mini")

    prompt = (
        "Extraia transações financeiras do texto abaixo. "
        "Retorne SOMENTE um JSON válido no formato de lista de objetos, "
        "cada objeto com as chaves: data (YYYY-MM-DD), valor (float) e descricao (string). "
        "Se não houver dados, retorne uma lista vazia [].\n\n"
        f"Texto:\n{texto_bruto}"
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Você é um extrator de dados financeiros."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
    }

    req = urllib.request.Request(
        api_base,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return [], f"Erro na API LLM: {exc.code} - {exc.reason}"
    except urllib.error.URLError as exc:
        return [], f"Falha de conexão com LLM: {exc.reason}"
    except Exception as exc:
        return [], f"Erro inesperado no LLM: {str(exc)}"

    content = (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
        .strip()
    )
    if not content:
        return [], "Resposta vazia do LLM."

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return [], "Resposta do LLM não está em JSON válido."

    if not isinstance(parsed, list):
        return [], "Resposta do LLM não retornou uma lista."

    return parsed, None
