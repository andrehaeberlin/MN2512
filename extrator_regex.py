import re
from datetime import datetime

def extrair_dados_financeiros(texto_bruto):
    """
    MN2512-9: Versão aprimorada com maior tolerância a falhas de OCR.
    """
    dados = {
        'data': None,
        'valor': None,
        'descricao': "Não identificado"
    }

    if not texto_bruto:
        return dados

    # --- 1. DATA: Suporte a separadores variados e datas sem zero à esquerda ---
    # Aceita DD/MM/AAAA, DD-MM-AA, D/M/AAAA, etc.
    padrao_data = r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
    match_data = re.search(padrao_data, texto_bruto)
    if match_data:
        data_str = match_data.group(1).replace('-', '/')
        formatos = ["%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d"]
        for fmt in formatos:
            try:
                dados['data'] = datetime.strptime(data_str, fmt)
                break
            except ValueError:
                continue

    # --- 2. VALOR: Captura valores com ou sem símbolo, tratando ruidos comuns ---
    # Procura por palavras chave próximas ao valor ou apenas o formato numérico
    # Regex explica: (Palavra opcional) (Espaços/Pontos) (Número com vírgula ou ponto decimal)
    padrao_valor = r'(?:TOTAL|VALOR|R\$|\$|PAGAR)?[\s:]*(\d{1,3}(?:\.\d{3})*,\d{2}|\d+,\d{2}|\d+\.\d{2})'
    matches_valor = re.findall(padrao_valor, texto_bruto, re.IGNORECASE)
    
    if matches_valor:
        # Geralmente o maior valor em um extrato/recibo é o total
        valores_convertidos = []
        for v in matches_valor:
            v_limpo = v.replace('.', '').replace(',', '.')
            try:
                valores_convertidos.append(float(v_limpo))
            except: continue
        
        if valores_convertidos:
            dados['valor'] = max(valores_convertidos)

    # --- 3. DESCRIÇÃO: Busca por blocos de texto que pareçam nomes ---
    linhas = [l.strip() for l in texto_bruto.split('\n') if len(l.strip()) > 3]
    keywords_desc = ["NOME", "RAZÃO", "ESTABELECIMENTO", "CLIENTE", "RECEBEMOS DE"]
    
    for i, linha in enumerate(linhas):
        # Se encontrar keyword, pega o resto da linha ou a linha seguinte
        if any(kw in linha.upper() for kw in keywords_desc):
            desc = re.sub(r'|'.join(keywords_desc), '', linha, flags=re.IGNORECASE)
            dados['descricao'] = desc.strip(': ').strip()
            break
        # Fallback: Se não achou nada e estamos na primeira linha, costuma ser o cabeçalho/nome
        if i == 0 and dados['descricao'] == "Não identificado":
            dados['descricao'] = linha[:50]

    return dados