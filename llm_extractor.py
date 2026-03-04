import json
import os
import random
import re
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from dotenv import load_dotenv

load_dotenv()

MAX_RETRIES = int(os.getenv("MAX_LLM_RETRIES", "3"))
MAX_LLM_CONCURRENCY = int(os.getenv("MAX_LLM_CONCURRENCY", "3"))
MAX_LLM_RPM = int(os.getenv("MAX_LLM_RPM", "60"))
RETRYABLE_HTTP = {429, 500, 502, 503, 504}
DOCUMENT_TYPES = {"Entrada", "Saída", "Extrato", "Fatura"}
DEFAULT_DOCUMENT_TYPE = "Extrato"
llm_sem = threading.Semaphore(max(1, MAX_LLM_CONCURRENCY))
_rpm_lock = threading.Lock()
_rpm_window = deque()


RECEIPT_MARKERS = [
    "documento aux. da nota fiscal",
    "nota fiscal de consumidor eletr",
    "qtd total de itens",
    "valor liquido",
]


def _parse_money_value(raw_value):
    value = (raw_value or "").strip().lower()
    if not value:
        return None

    value = value.replace("r$", "").replace("rs", "").strip()
    value = re.sub(r"\s+", "", value)
    value = re.sub(r"[^0-9,\.-]", "", value)
    if not value:
        return None

    negative = value.startswith("-")
    if negative:
        value = value[1:]

    if "," in value and "." in value:
        if value.rfind(",") > value.rfind("."):
            value = value.replace(".", "").replace(",", ".")
        else:
            value = value.replace(",", "")
    elif "," in value:
        value = value.replace(".", "").replace(",", ".")
    elif value.count(".") > 1:
        value = value.replace(".", "")

    try:
        parsed = float(value)
        return -parsed if negative else parsed
    except ValueError:
        return None


def _extract_receipt_subitems(texto_bruto):
    texto = (texto_bruto or "").strip()
    if not texto:
        return []

    texto_lower = texto.lower()
    if not any(marker in texto_lower for marker in RECEIPT_MARKERS):
        return []

    date_match = re.search(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b", texto)
    data_base = None
    if date_match:
        raw = date_match.group(1).replace("-", "/")
        parts = raw.split("/")
        if len(parts) == 3:
            day, month, year = parts
            year = f"20{year}" if len(year) == 2 else year
            data_base = f"{year.zfill(4)}-{month.zfill(2)}-{day.zfill(2)}"

    lines = [re.sub(r"\s+", " ", line).strip() for line in texto.splitlines() if line.strip()]
    header_pattern = re.compile(
        r"^(\d{6,14})\s+(.+?)(?:\s+(?:r\$\s*)?([\d\.,]{3,}))?$",
        flags=re.IGNORECASE,
    )
    money_pattern = re.compile(r"(?:r\$\s*)?-?\d[\d\.,]*[\.,]\d{2}", flags=re.IGNORECASE)

    blocked_tokens = ["total", "desconto", "valor pagar", "valor pago", "cartao", "nfc-e", "serie"]

    items = []
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        header = header_pattern.match(line)
        if not header:
            idx += 1
            continue

        descricao = (header.group(2) or "").strip(" -:")
        desc_lower = descricao.lower()
        if not descricao or any(token in desc_lower for token in blocked_tokens):
            idx += 1
            continue

        valor = _parse_money_value(header.group(3) or "")

        lookahead = idx + 1
        while lookahead < len(lines) and lookahead <= idx + 4:
            probe = lines[lookahead]
            if header_pattern.match(probe):
                break
            if "valor liquido" in probe.lower() or " por " in probe.lower():
                amounts = money_pattern.findall(probe)
                if amounts:
                    parsed_probe = _parse_money_value(amounts[-1])
                    if parsed_probe is not None:
                        valor = parsed_probe
            lookahead += 1

        if valor is not None and valor > 0:
            items.append(
                {
                    "data": data_base,
                    "valor": round(float(valor), 2),
                    "descricao": descricao,
                    "tipo": "saida",
                    "document_type": "Saída",
                }
            )

        idx += 1

    if len(items) < 2:
        return []
    return items


def _parse_json_content(content):
    """Faz parsing tolerante de JSON, incluindo respostas com markdown code fence."""
    texto = content.strip()
    if texto.startswith("```"):
        linhas = texto.splitlines()
        if linhas:
            linhas = linhas[1:]
        if linhas and linhas[-1].strip().startswith("```"):
            linhas = linhas[:-1]
        texto = "\n".join(linhas).strip()
    return json.loads(texto)


def _shrink_text(texto, head=15000, tail=5000):
    t = (texto or "").strip()
    if len(t) <= head + tail:
        return t
    return t[:head] + "\n...\n" + t[-tail:]


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




def _acquire_rpm_slot(rpm: int = MAX_LLM_RPM):
    rpm = max(1, int(rpm))
    while True:
        now = time.time()
        with _rpm_lock:
            while _rpm_window and (now - _rpm_window[0]) >= 60:
                _rpm_window.popleft()

            if len(_rpm_window) < rpm:
                _rpm_window.append(now)
                return

            wait_for = max(0.01, 60 - (now - _rpm_window[0]))
        time.sleep(min(wait_for, 0.5))


def _call_llm_controlled(api_base, api_key, payload, timeout=30):
    _acquire_rpm_slot()
    with llm_sem:
        return _post_chat_completion(api_base, api_key, payload, timeout=timeout)


def _call_llm_with_retry(api_base, api_key, payload, timeout=30):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return _call_llm_controlled(api_base, api_key, payload, timeout=timeout)
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code in RETRYABLE_HTTP and attempt < MAX_RETRIES:
                time.sleep((2 ** (attempt - 1)) + random.random() * 0.2)
                continue
            raise
        except urllib.error.URLError as exc:
            last_error = exc
            if attempt < MAX_RETRIES:
                time.sleep((2 ** (attempt - 1)) + random.random() * 0.2)
                continue
            raise
    if last_error:
        raise last_error


def extrair_dados_financeiros_llm(texto_bruto):
    """
    Faz fallback de extração usando um LLM quando o regex falhar.
    Retorna (lista_de_dados, erro).
    Cada item inclui o campo `document_type` com uma das classes:
    Entrada, Saída, Extrato ou Fatura.
    """
    if not texto_bruto:
        return [], "Texto vazio para extração via LLM."

    api_key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return [], "Chave de API não configurada para extração via LLM."

    api_base = os.getenv("LLM_API_BASE", "https://api.openai.com/v1/chat/completions")
    model = os.getenv("LLM_MODEL", "gpt-4o-mini")
    texto_reduzido = _shrink_text(texto_bruto)

    prompt = (
        "Extraia transações financeiras do texto OCR abaixo e classifique o documento inteiro. "
        "Retorne SOMENTE um JSON válido no formato de lista de objetos. "
        "Cada objeto deve conter: data (YYYY-MM-DD), valor (float), descricao (string curta e limpa), tipo ('entrada' ou 'saida') e document_type ('Entrada', 'Saída', 'Extrato' ou 'Fatura'). "
        "Regras: "
        "1) Normalize datas como DD/MM/AAAA para YYYY-MM-DD; "
        "2) Não inclua texto de autenticação/terminal/protocolo na descricao; "
        "3) Se detectar pagamento/compra, use tipo='saida'; se detectar recebimento/credito, use tipo='entrada'; "
        "4) O campo document_type deve refletir o tipo global do documento e se repetir em todos os itens; "
        "5) Para cupons/notas fiscais, retorne os itens/subitens comprados (cada produto em uma linha) e não apenas o total/pagamento; "
        "6) Ignore linhas de resumo como Total, Valor Pago, Forma de Pagamento e Desconto como transações independentes; "
        "7) Se não conseguir identificar nada com segurança, retorne [] sem texto adicional.\n\n"
        f"Texto:\n{texto_reduzido}"
    )

    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Você é um extrator financeiro especialista em classificação documental. "
                    "Classifique o documento como Entrada, Saída, Extrato ou Fatura e inclua "
                    "o campo document_type em todos os itens de saída."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
    }

    try:
        data = _call_llm_with_retry(api_base, api_key, payload, timeout=30)
    except urllib.error.HTTPError as exc:
        return [], f"Erro na API LLM: {exc.code} - {exc.reason}"
    except urllib.error.URLError as exc:
        return [], f"Falha de conexão com LLM: {exc.reason}"
    except Exception as exc:
        return [], f"Erro inesperado no LLM: {str(exc)}"

    content = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
    if not content:
        return [], "Resposta vazia do LLM."

    try:
        parsed = _parse_json_content(content)
    except json.JSONDecodeError:
        return [], "Resposta do LLM não está em JSON válido."

    if not isinstance(parsed, list):
        return [], "Resposta do LLM não retornou uma lista."

    normalized = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        item_saida = dict(item)
        document_type = str(item_saida.get("document_type") or DEFAULT_DOCUMENT_TYPE).strip().title()
        if document_type not in DOCUMENT_TYPES:
            document_type = DEFAULT_DOCUMENT_TYPE
        item_saida["document_type"] = document_type
        normalized.append(item_saida)

    if len(normalized) <= 1:
        heuristica = _extract_receipt_subitems(texto_bruto)
        if len(heuristica) >= 2:
            return heuristica, None

    return normalized, None


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

    transacoes_json = _shrink_text(json.dumps(transacoes, ensure_ascii=False), head=12000, tail=3000)
    prompt = (
        "Classifique cada transação em UMA categoria dentre: "
        f"{', '.join(categorias_validas)}. "
        "Retorne SOMENTE um JSON válido com uma lista de objetos no formato: "
        "[{\"index\": 0, \"categoria\": \"Outros\"}]. "
        "Se não conseguir classificar, retorne [] e não retorne texto adicional.\n\n"
        f"Transações (JSON):\n{transacoes_json}"
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
        data = _call_llm_with_retry(api_base, api_key, payload, timeout=30)
    except urllib.error.HTTPError as exc:
        return transacoes, f"Erro na API LLM (categorização): {exc.code} - {exc.reason}"
    except urllib.error.URLError as exc:
        return transacoes, f"Falha de conexão com LLM (categorização): {exc.reason}"
    except Exception as exc:
        return transacoes, f"Erro inesperado no LLM (categorização): {str(exc)}"

    content = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
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
