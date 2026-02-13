import re
from datetime import datetime
from typing import Dict, List, Optional


DATE_PATTERNS = [
    "%d/%m/%Y", "%d-%m-%Y",
    "%d/%m/%y", "%d-%m-%y",
    "%Y-%m-%d", "%Y/%m/%d",
]

KEYWORDS_TOTAL = [
    "total", "valor total", "total a pagar", "valor a pagar", "total r$", "total rs",
    "vlr total", "total geral"
]


def _parse_date(raw: str) -> Optional[str]:
    raw = raw.strip()
    raw_alt = raw.replace(".", "/").replace("-", "/")

    for fmt in DATE_PATTERNS:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

        try:
            dt = datetime.strptime(raw_alt, fmt.replace("-", "/"))
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def _parse_money(raw_value: str) -> Optional[float]:
    if not raw_value:
        return None

    value = raw_value.strip().lower()
    value = value.replace("r$", "").replace("rs", "").strip()

    negative = False
    if value.startswith("-"):
        negative = True
        value = value[1:].strip()
    if value.startswith("(") and value.endswith(")"):
        negative = True
        value = value[1:-1].strip()

    value = re.sub(r"\s+", "", value)
    value = re.sub(r"[^0-9,\.]", "", value)
    if not value:
        return None

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


def _find_best_total(text: str) -> Optional[float]:
    text_lower = text.lower()
    pattern_money = r"(?:r\$\s*)?\(?-?\d[\d\.,]*[\,\.]\d{2}\)?"

    for keyword in KEYWORDS_TOTAL:
        idx = text_lower.find(keyword)
        if idx < 0:
            continue

        window = text[idx: idx + 120]
        matches = re.findall(pattern_money, window, flags=re.IGNORECASE)
        if matches:
            total = _parse_money(matches[-1])
            if total is not None and abs(total) > 0:
                return total

    parsed = [_parse_money(item) for item in re.findall(pattern_money, text, flags=re.IGNORECASE)]
    parsed = [value for value in parsed if value is not None and 0.01 <= abs(value) <= 1e7]
    if not parsed:
        return None

    return max(parsed, key=lambda candidate: abs(candidate))


def extrair_dados_financeiros(texto_bruto: str) -> List[Dict]:
    """
    MN2512-Fix: Versão híbrida que suporta tanto listas de transações (extratos)
    quanto documentos únicos (recibos isolados).
    """
    if not texto_bruto or not texto_bruto.strip():
        return []

    texto = texto_bruto.strip()
    resultados = []
    
    # --- ESTRATÉGIA 1: Segmentação por Datas (Para Extratos/Listas) ---
    # Regex para identificar inícios de linha com data (DD/MM/AAAA, AAAA-MM-DD, etc)
    regex_data_inicio = r'^\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})\b'
    
    matches_data = list(re.finditer(regex_data_inicio, texto, flags=re.MULTILINE))
    
    # Se encontrou datas, tentamos fatiar o texto
    if matches_data:
        for i, match in enumerate(matches_data):
            # Define o início e fim do bloco de texto da transação atual
            inicio_atual = match.start()
            # Vai até a próxima data ou até o fim do texto
            fim_atual = matches_data[i+1].start() if i + 1 < len(matches_data) else len(texto)
            
            bloco = texto[inicio_atual:fim_atual].strip()
            data_raw = match.group(1).strip()
            
            # Remove a data do bloco para processar descrição e valor
            texto_sem_data = re.sub(regex_data_inicio, "", bloco, count=1, flags=re.MULTILINE).strip()
            
            # Busca valores monetários no final da linha (ex: 1.500,50 ou 1500.50)
            # Regex: (Digitos) + (Separador opcional) + (2 decimais)
            regex_valor = r'(?:r\$\s*)?\(?-?\d[\d\.,]*[\,\.]\d{2}\)?'
            matches_valor = re.findall(regex_valor, texto_sem_data, flags=re.IGNORECASE)
            
            valor = 0.0
            descricao = texto_sem_data
            
            if matches_valor:
                # Assume que o valor da transação é o último número encontrado na linha
                valor_str = matches_valor[-1]
                
                pos = descricao.lower().rfind(valor_str.lower())
                if pos >= 0:
                    descricao = (descricao[:pos] + descricao[pos + len(valor_str):]).strip()

                parsed = _parse_money(valor_str)
                valor = float(parsed if parsed is not None else 0.0)

            # Normalização de Data
            data_fmt = _parse_date(data_raw) or data_raw

            resultados.append({
                'data': data_fmt,
                'valor': valor,
                'descricao': descricao.strip(" -:\n\t") or "Não identificado"
            })

    # --- ESTRATÉGIA 2: Fallback (Para Recibos Únicos/Ruído) ---
    # Se a estratégia de lista falhou (nenhum resultado ou resultado vazio), 
    # usamos a lógica antiga de "pescar" um único dado.
    if not resultados:
        dados = {'data': None, 'valor': 0.0, 'descricao': "Não identificado"}
        
        # Lógica antiga simplificada para data
        match_data = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})', texto)
        if match_data:
            dados['data'] = _parse_date(match_data.group(1)) or match_data.group(1)
            
        # Lógica para valor, priorizando labels de total
        best_total = _find_best_total(texto)
        if best_total is not None:
            dados['valor'] = float(best_total)
             
        linhas = [line.strip() for line in texto.splitlines() if line.strip()]
        dados['descricao'] = (linhas[0] if linhas else texto[:80]).strip() or "Não identificado"
        resultados.append(dados)

    return resultados
