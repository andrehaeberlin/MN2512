import re
from datetime import datetime

def extrair_dados_financeiros(texto_bruto):
    """
    MN2512-Fix: Versão híbrida que suporta tanto listas de transações (extratos)
    quanto documentos únicos (recibos isolados).
    """
    if not texto_bruto:
        return []

    resultados = []
    
    # --- ESTRATÉGIA 1: Segmentação por Datas (Para Extratos/Listas) ---
    # Regex para identificar inícios de linha com data (DD/MM/AAAA, AAAA-MM-DD, etc)
    regex_data_inicio = r'(?:^|\s)(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})(?=\s)'
    
    matches_data = list(re.finditer(regex_data_inicio, texto_bruto))
    
    # Se encontrou datas, tentamos fatiar o texto
    if matches_data:
        for i, match in enumerate(matches_data):
            # Define o início e fim do bloco de texto da transação atual
            inicio_atual = match.start()
            # Vai até a próxima data ou até o fim do texto
            fim_atual = matches_data[i+1].start() if i + 1 < len(matches_data) else len(texto_bruto)
            
            bloco = texto_bruto[inicio_atual:fim_atual].strip()
            data_raw = match.group(1).strip()
            
            # Remove a data do bloco para processar descrição e valor
            texto_sem_data = bloco[len(data_raw):].strip()
            
            # Busca valores monetários no final da linha (ex: 1.500,50 ou 1500.50)
            # Regex: (Digitos) + (Separador opcional) + (2 decimais)
            regex_valor = r'(\d[\d.,]*[.,]\d{2})\b'
            matches_valor = list(re.finditer(regex_valor, texto_sem_data))
            
            valor = 0.0
            descricao = texto_sem_data
            
            if matches_valor:
                # Assume que o valor da transação é o último número encontrado na linha
                valor_str = matches_valor[-1].group(1)
                
                # Remove o valor da descrição para limpar o texto
                descricao = texto_sem_data.replace(valor_str, "").strip()
                
                # Normalização de Valor (Brasil vs US)
                v_limpo = valor_str.replace('R$', '').strip()
                try:
                    if ',' in v_limpo and '.' in v_limpo: # Ex: 1.500,50
                        if v_limpo.rfind(',') > v_limpo.rfind('.'): # Padrão BR
                            v_limpo = v_limpo.replace('.', '').replace(',', '.')
                        else: # Padrão US
                            v_limpo = v_limpo.replace(',', '')
                    elif ',' in v_limpo: # Ex: 1500,50
                        v_limpo = v_limpo.replace(',', '.')
                    valor = float(v_limpo)
                except:
                    valor = 0.0

            # Normalização de Data
            data_fmt = data_raw
            for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"]:
                try:
                    dt = datetime.strptime(data_raw, fmt)
                    data_fmt = dt.strftime("%Y-%m-%d")
                    break
                except ValueError: continue

            resultados.append({
                'data': data_fmt,
                'valor': valor,
                'descricao': descricao.strip(" -:") or "Não identificado"
            })

    # --- ESTRATÉGIA 2: Fallback (Para Recibos Únicos/Ruído) ---
    # Se a estratégia de lista falhou (nenhum resultado ou resultado vazio), 
    # usamos a lógica antiga de "pescar" um único dado.
    if not resultados:
        dados = {'data': None, 'valor': 0.0, 'descricao': "Não identificado"}
        
        # Lógica antiga simplificada para data
        match_data = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', texto_bruto)
        if match_data:
            try:
                dt = datetime.strptime(match_data.group(1).replace('-', '/'), "%d/%m/%Y")
                dados['data'] = dt.strftime("%Y-%m-%d")
            except: pass
            
        # Lógica antiga para valor (maior valor encontrado)
        vals = re.findall(r'(\d+,\d{2})', texto_bruto)
        if vals:
             dados['valor'] = max([float(v.replace(',', '.')) for v in vals])
             
        dados['descricao'] = texto_bruto[:50].strip() # Pega início como descrição
        resultados.append(dados)

    return resultados