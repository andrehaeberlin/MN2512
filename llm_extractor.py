import json
import os
import urllib.request
import urllib.error


def _parse_json_content(content):
    """
    Faz parsing tolerante de JSON, incluindo respostas com markdown code fence.
    """
    texto = content.strip()
    if texto.startswith("```"):
        linhas = texto.splitlines()
        if linhas:
            linhas = linhas[1:]
        if linhas and linhas[-1].strip().startswith("```"):
            linhas = linhas[:-1]
        texto = "\n".join(linhas).strip()
    return json.loads(texto)


def _post_chat_completion(api_base, api_key, payload, timeout=30):
    req = urllib.request.Request(
        api_base,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


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

    try:
        data = _post_chat_completion(api_base, api_key, payload, timeout=30)
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
        parsed = _parse_json_content(content)
    except json.JSONDecodeError:
        return [], "Resposta do LLM não está em JSON válido."

    if not isinstance(parsed, list):
        return [], "Resposta do LLM não retornou uma lista."

    return parsed, None


def categorizar_transacoes_llm(transacoes):
    """
    Categoriza transações usando LLM e retorna (transacoes_categorizadas, erro).
    Não altera data/valor/descricao, apenas adiciona/normaliza campo `categoria`.
    """
    if not transacoes:
        return [], None

    api_key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return transacoes, "Chave de API não configurada para categorização via LLM."

    api_base = os.getenv("LLM_API_BASE", "https://api.openai.com/v1/chat/completions")
    model = os.getenv("LLM_MODEL", "gpt-4o-mini")
    categorias_validas = ["Alimentação", "Transporte", "Serviços", "Outros"]

    prompt = (
        "Classifique cada transação em UMA categoria dentre: "
        f"{', '.join(categorias_validas)}. "
        "Retorne SOMENTE um JSON válido com uma lista de objetos no formato: "
        "[{\"index\": 0, \"categoria\": \"Outros\"}]. "
        "Não altere índices e não retorne texto adicional.\n\n"
        f"Transações (JSON):\n{json.dumps(transacoes, ensure_ascii=False)}"
    )

    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Você classifica transações financeiras em categorias de orçamento "
                    "pessoal com alta precisão."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
    }

    try:
        data = _post_chat_completion(api_base, api_key, payload, timeout=30)
    except urllib.error.HTTPError as exc:
        return transacoes, f"Erro na API LLM (categorização): {exc.code} - {exc.reason}"
    except urllib.error.URLError as exc:
        return transacoes, f"Falha de conexão com LLM (categorização): {exc.reason}"
    except Exception as exc:
        return transacoes, f"Erro inesperado no LLM (categorização): {str(exc)}"

    content = (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
        .strip()
    )
    if not content:
        return transacoes, "Resposta vazia do LLM na categorização."

    try:
        classificacoes = _parse_json_content(content)
    except json.JSONDecodeError:
        return transacoes, "Resposta de categorização do LLM não está em JSON válido."

    if not isinstance(classificacoes, list):
        return transacoes, "Resposta de categorização do LLM não retornou uma lista."

    classificacao_por_indice = {}
    for item in classificacoes:
        if not isinstance(item, dict):
            continue
        idx = item.get("index")
        categoria = item.get("categoria")
        if isinstance(idx, int) and categoria in categorias_validas:
            classificacao_por_indice[idx] = categoria

    transacoes_saida = []
    for idx, transacao in enumerate(transacoes):
        t = dict(transacao)
        categoria = classificacao_por_indice.get(idx, t.get("categoria", "Outros"))
        if categoria not in categorias_validas:
            categoria = "Outros"
        t["categoria"] = categoria
        transacoes_saida.append(t)

    return transacoes_saida, None
