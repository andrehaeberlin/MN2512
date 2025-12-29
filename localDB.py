# ============================================================
# FILE: localDB.py
# ============================================================
import sqlite3
import pandas as pd
import os
from datetime import datetime

DB_NAME = "dados_financeiros.db"

def init_db():
    """Inicializa o banco com a restrição de unicidade para evitar duplicatas."""
    folder = os.path.dirname(DB_NAME)
    if folder and not os.path.exists(folder):
        os.makedirs(folder)
    
    query = """
    CREATE TABLE IF NOT EXISTS transacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT NOT NULL,
        descricao TEXT NOT NULL,
        valor REAL NOT NULL,
        fonte TEXT NOT NULL,
        categoria TEXT,
        criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(data, descricao, valor, fonte) 
    );
    """
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(query)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_data ON transacoes(data);")

def insert_transactions(df: pd.DataFrame):
    """Insere apenas novos registros tratando valores nulos e duplicatas."""
    if df.empty:
        return 0
    
    # Criamos uma cópia para não afetar o DataFrame original da UI
    df_final = df.copy()
    
    # Regra de Negócio: Se a data for nula, usamos a data de hoje para evitar o erro NOT NULL
    if 'data' in df_final.columns:
        df_final['data'] = df_final['data'].fillna(datetime.now().strftime('%Y-%m-%d'))
    
    colunas_obrigatorias = ['data', 'descricao', 'valor', 'fonte', 'categoria']
    
    # Garante que todas as colunas existem antes de enviar para o staging
    for col in colunas_obrigatorias:
        if col not in df_final.columns:
            # Usamos string vazia ou 0 em vez de None para respeitar o NOT NULL
            df_final[col] = "" if col != 'valor' else 0.0
            
    df_final = df_final[colunas_obrigatorias]
    
    try:
        with sqlite3.connect(DB_NAME) as conn:
            # Staging para comparação de duplicatas
            df_final.to_sql('staging_transacoes', conn, if_exists='replace', index=False)
            
            query_upsert = """
            INSERT OR IGNORE INTO transacoes (data, descricao, valor, fonte, categoria)
            SELECT data, descricao, valor, fonte, categoria FROM staging_transacoes;
            """
            cursor = conn.execute(query_upsert)
            novas_linhas = cursor.rowcount
            
            conn.execute("DROP TABLE staging_transacoes;")
            return novas_linhas
    except Exception as e:
        raise Exception(f"Erro técnico na camada de dados: {e}")

def get_all_transactions():
    """Busca o histórico completo."""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            return pd.read_sql_query("SELECT * FROM transacoes ORDER BY data DESC", conn)
    except Exception:
        return pd.DataFrame()