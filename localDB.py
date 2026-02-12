import hashlib
import os
import sqlite3
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

DB_NAME = "dados_financeiros.db"


def init_db():
    """Inicializa/migra o banco principal da aplicação."""
    folder = os.path.dirname(DB_NAME)
    if folder and not os.path.exists(folder):
        os.makedirs(folder)

    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            """
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
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_data ON transacoes(data);")

        # Migração leve para transações já existentes
        colunas = [row[1] for row in conn.execute("PRAGMA table_info(transacoes)").fetchall()]
        if "document_id" not in colunas:
            conn.execute("ALTER TABLE transacoes ADD COLUMN document_id INTEGER;")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL,
                mime TEXT NOT NULL,
                sha256 TEXT NOT NULL UNIQUE,
                storage_path TEXT NOT NULL,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_type TEXT NOT NULL,
                from_id INTEGER NOT NULL,
                to_type TEXT NOT NULL,
                to_id INTEGER NOT NULL,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(from_type, from_id, to_type, to_id)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_links_from ON links(from_type, from_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_links_to ON links(to_type, to_id);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS statements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER NOT NULL,
                banco TEXT,
                cartao TEXT,
                competencia TEXT,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(document_id) REFERENCES documents(id)
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS statement_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                statement_id INTEGER NOT NULL,
                data TEXT,
                descricao TEXT NOT NULL,
                valor REAL NOT NULL,
                parcela_total INTEGER,
                parcela_atual INTEGER,
                merchant TEXT,
                hash_linha TEXT NOT NULL UNIQUE,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(statement_id) REFERENCES statements(id)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stmt_lines_stmt ON statement_lines(statement_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stmt_lines_data ON statement_lines(data);")


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def insert_document(file_name: str, mime: str, file_bytes: bytes, storage_dir: str = "storage") -> int:
    """Salva documento no disco e registra no DB com deduplicação por hash."""
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)

    sha = _sha256_bytes(file_bytes)
    storage_path = os.path.join(storage_dir, sha)

    if not os.path.exists(storage_path):
        with open(storage_path, "wb") as handler:
            handler.write(file_bytes)

    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO documents (nome, mime, sha256, storage_path) VALUES (?, ?, ?, ?)",
            (file_name, mime or "application/octet-stream", sha, storage_path),
        )
        row = conn.execute("SELECT id FROM documents WHERE sha256 = ?", (sha,)).fetchone()

    return int(row[0])


def get_document_bytes(document_id: int) -> Tuple[bytes, str, str]:
    """Retorna (bytes, mime, nome) para visualização/download."""
    with sqlite3.connect(DB_NAME) as conn:
        row = conn.execute(
            "SELECT storage_path, mime, nome FROM documents WHERE id = ?", (document_id,)
        ).fetchone()

    if not row:
        raise FileNotFoundError("Documento não encontrado.")

    storage_path, mime, nome = row
    with open(storage_path, "rb") as handler:
        return handler.read(), mime, nome


def link_entities(from_type: str, from_id: int, to_type: str, to_id: int) -> None:
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO links (from_type, from_id, to_type, to_id) VALUES (?, ?, ?, ?)",
            (from_type, int(from_id), to_type, int(to_id)),
        )


def find_transaction_id(data: str, descricao: str, valor: float, fonte: str) -> Optional[int]:
    with sqlite3.connect(DB_NAME) as conn:
        row = conn.execute(
            "SELECT id FROM transacoes WHERE data = ? AND descricao = ? AND valor = ? AND fonte = ?",
            (data, descricao, float(valor), fonte),
        ).fetchone()
    return int(row[0]) if row else None


def insert_transactions(df: pd.DataFrame):
    """Insere apenas novos registros tratando valores nulos e duplicatas."""
    if df.empty:
        return 0

    df_final = df.copy()

    if "data" in df_final.columns:
        df_final["data"] = df_final["data"].fillna(datetime.now().strftime("%Y-%m-%d"))

    colunas_obrigatorias = ["data", "descricao", "valor", "fonte", "categoria"]

    for col in colunas_obrigatorias:
        if col not in df_final.columns:
            df_final[col] = "" if col != "valor" else 0.0

    df_final = df_final[colunas_obrigatorias]

    try:
        with sqlite3.connect(DB_NAME) as conn:
            df_final.to_sql("staging_transacoes", conn, if_exists="replace", index=False)

            query_upsert = """
            INSERT OR IGNORE INTO transacoes (data, descricao, valor, fonte, categoria)
            SELECT data, descricao, valor, fonte, categoria FROM staging_transacoes;
            """
            cursor = conn.execute(query_upsert)
            novas_linhas = cursor.rowcount

            conn.execute("DROP TABLE staging_transacoes;")
            return novas_linhas
    except Exception as exc:
        raise Exception(f"Erro técnico na camada de dados: {exc}")


def get_all_transactions():
    """Busca o histórico completo."""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            return pd.read_sql_query("SELECT * FROM transacoes ORDER BY data DESC", conn)
    except Exception:
        return pd.DataFrame()
