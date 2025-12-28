# ============================================================
# FILE: planilhas.py
# ============================================================
import pandas as pd

def processar_planilha(uploaded_file):
    """
    Lê, mapeia e normaliza arquivos Excel/CSV seguindo os critérios da Task MN2512-5.
    """
    try:
        # 1. Leitura de Buffer (Critério: Suporte a CSV e XLSX)
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        # 2. Mapeamento Flexível (Critério: Dicionário de Sinônimos)
        mapeamento = {
            "data": ["data", "date", "vencimento", "dia", "período"],
            "valor": ["valor", "preço", "total", "amount", "pago", "valor pago"],
            "descricao": ["descrição", "item", "serviço", "histórico", "nome", "estabelecimento"]
        }

        # Padroniza nomes das colunas atuais para minúsculo (Critério: Case Insensitive)
        colunas_originais = {c.lower(): c for c in df.columns}
        df.columns = [c.lower() for c in df.columns]

        colunas_finais = {}
        for padrao, sinonimos in mapeamento.items():
            # Tenta encontrar um sinônimo nas colunas do arquivo
            encontrada = next((s for s in sinonimos if s in df.columns), None)
            
            if encontrada:
                colunas_finais[encontrada] = padrao
            else:
                # 5. Fallback de Erro (Critério: Erro Amigável)
                return None, f"Não encontramos a coluna de **{padrao}** no arquivo '{uploaded_file.name}'."

        # Renomeia e filtra apenas o que importa
        df = df.rename(columns=colunas_finais)
        
        # 3. Limpeza de Dados (Critério: Remover vazios e converter Valor)
        df = df.dropna(subset=list(mapeamento.keys()), how='all') # Remove se tudo estiver vazio
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce') # Garante que é float
        
        # 4. Conversão de Data (Critério: Datetime Pandas)
        df['data'] = pd.to_datetime(df['data'], errors='coerce')
        
        # Adiciona a coluna de fonte
        df['fonte'] = uploaded_file.name

        # Retorna apenas as 4 colunas padrão
        return df[["data", "descricao", "valor", "fonte"]], None

    except Exception as e:
        return None, f"Erro crítico ao ler '{uploaded_file.name}': {str(e)}"