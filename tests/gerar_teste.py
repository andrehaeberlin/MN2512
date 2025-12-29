import pandas as pd

# Criando dados de exemplo com cabeçalhos variados
# Note que usamos 'Vencimento' em vez de 'Data' e 'Preço' em vez de 'Valor'
dados = {
    "Vencimento": ["2023-10-01", "2023-10-05", "2023-10-10", None, "2023-10-15"],
    "Histórico": ["Mercado Central", "Posto de Gasolina", "Internet Fibra", "Linha Vazia", "Aluguel"],
    "Preço": [150.50, 220.00, 99.90, None, 1200.00]
}

df_teste = pd.DataFrame(dados)

# Salvando em Excel (XLSX)
nome_arquivo = "teste_extrator.xlsx"
df_teste.to_excel(nome_arquivo, index=False)

print(f"✅ Arquivo '{nome_arquivo}' gerado com sucesso!")