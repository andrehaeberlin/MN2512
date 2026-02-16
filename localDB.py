import hashlib
import io
import json
import logging
import os
import sqlite3
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from extrator_regex import extrair_dados_financeiros
from llm_extractor import extrair_dados_financeiros_llm
from ocr import extrair_texto_imagem
from parsers.ofx_parser import StatementLine, build_hash_linha
from pdfs import converter_pdf_para_imagens, extrair_texto_pdf
from planilhas import processar_planilha

DB_NAME = "dados_financeiros.db"
INGEST_DB_NAME = "ingestao.db"

STATUS_STORED = "STORED"
STATUS_PROCESSING = "PROCESSING"
STATUS_LLM_REVIEW = "LLM_REVIEW"
STATUS_HITL_REVIEW = "HITL_REVIEW"
STATUS_FINALIZE_PENDING = "FINALIZE_PENDING"
STATUS_FINALIZED = "FINALIZED"
STATUS_ERROR_STORAGE = "ERROR_STORAGE"
STATUS_ERROR_PROCESSING = "ERROR_PROCESSING"

logger = logging.getLogger(__name__)

if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")


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
                tipo TEXT NOT NULL DEFAULT 'saida',
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(data, descricao, valor, fonte, tipo)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_data ON transacoes(data);")

        # Migração leve para transações já existentes
        colunas = [row[1] for row in conn.execute("PRAGMA table_info(transacoes)").fetchall()]
        if "document_id" not in colunas:
            conn.execute("ALTER TABLE transacoes ADD COLUMN document_id INTEGER;")
        if "tipo" not in colunas:
            conn.execute("ALTER TABLE transacoes ADD COLUMN tipo TEXT NOT NULL DEFAULT 'saida';")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_tipo_data ON transacoes(tipo, data);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_categoria_data ON transacoes(categoria, data);")

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


def find_transaction_id(data: str, descricao: str, valor: float, fonte: str, tipo: str = "saida") -> Optional[int]:
    with sqlite3.connect(DB_NAME) as conn:
        row = conn.execute(
            "SELECT id FROM transacoes WHERE data = ? AND descricao = ? AND valor = ? AND fonte = ? AND tipo = ?",
            (data, descricao, float(valor), fonte, tipo),
        ).fetchone()
    return int(row[0]) if row else None


def insert_transactions(df: pd.DataFrame):
    """Insere apenas novos registros tratando valores nulos e duplicatas."""
    if df.empty:
        return 0

    df_final = df.copy()

    if "data" in df_final.columns:
        df_final["data"] = df_final["data"].fillna(datetime.now().strftime("%Y-%m-%d"))

    colunas_obrigatorias = ["data", "descricao", "valor", "fonte", "categoria", "tipo"]

    for col in colunas_obrigatorias:
        if col not in df_final.columns:
            if col == "valor":
                df_final[col] = 0.0
            elif col == "tipo":
                df_final[col] = "saida"
            else:
                df_final[col] = ""

    df_final = df_final[colunas_obrigatorias]
    df_final["valor"] = pd.to_numeric(df_final["valor"], errors="coerce").fillna(0.0).abs()
    df_final["tipo"] = df_final["tipo"].astype(str).str.lower().str.strip()
    df_final.loc[~df_final["tipo"].isin(["entrada", "saida"]), "tipo"] = "saida"

    try:
        with sqlite3.connect(DB_NAME) as conn:
            df_final.to_sql("staging_transacoes", conn, if_exists="replace", index=False)

            query_upsert = """
            INSERT OR IGNORE INTO transacoes (data, descricao, valor, fonte, categoria, tipo)
            SELECT data, descricao, valor, fonte, categoria, tipo FROM staging_transacoes;
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


def insert_statement(document_id: int, banco: Optional[str], cartao: Optional[str], competencia: str) -> int:
    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.execute(
            "INSERT INTO statements (document_id, banco, cartao, competencia) VALUES (?, ?, ?, ?)",
            (int(document_id), banco, cartao, competencia),
        )
        return int(cur.lastrowid)


def insert_statement_lines(statement_id: int, competencia: str, lines: Iterable[StatementLine]) -> int:
    payload = []
    for ln in lines:
        payload.append(
            (
                int(statement_id),
                ln.data,
                ln.descricao,
                float(ln.valor),
                ln.parcela_total,
                ln.parcela_atual,
                ln.merchant,
                build_hash_linha(competencia, ln),
            )
        )

    if not payload:
        return 0

    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.executemany(
            """
            INSERT OR IGNORE INTO statement_lines
            (statement_id, data, descricao, valor, parcela_total, parcela_atual, merchant, hash_linha)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        return cur.rowcount


def init_ingest_db():
    """Inicializa/migra o banco de ingestão da pipeline."""
    with sqlite3.connect(INGEST_DB_NAME) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                sha256 TEXT NOT NULL UNIQUE,
                original_name TEXT NOT NULL,
                mime TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                storage_uri_raw TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ingest_documents_status ON documents(status);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                storage_uri TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                meta_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(document_id, kind, storage_uri),
                FOREIGN KEY(document_id) REFERENCES documents(id)
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS extractions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                extractor TEXT NOT NULL,
                payload_uri TEXT NOT NULL,
                confidence REAL NOT NULL,
                llm_checks_uri TEXT,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(document_id) REFERENCES documents(id)
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                reviewer TEXT,
                decision TEXT NOT NULL,
                edited_payload_uri TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(document_id) REFERENCES documents(id)
            );
            """
        )


def _safe_name(filename: str) -> str:
    return os.path.basename(filename).replace(" ", "_") or "document.bin"


def _write_bytes(path: str, content: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as handler:
        handler.write(content)


def _write_json(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handler:
        json.dump(payload, handler, ensure_ascii=False, indent=2)


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def store_raw_document(file_name: str, mime: str, file_bytes: bytes, storage_root: str = "data") -> Dict[str, Any]:
    """Primeiro salva raw e depois registra no DB de ingestão com status STORED."""
    init_ingest_db()
    sha = _sha256_bytes(file_bytes)
    ext = os.path.splitext(_safe_name(file_name))[1] or ".bin"
    raw_path = os.path.join(storage_root, "raw", sha, f"original{ext}")
    if not os.path.exists(raw_path):
        _write_bytes(raw_path, file_bytes)

    with sqlite3.connect(INGEST_DB_NAME) as conn:
        existing = conn.execute(
            "SELECT id, sha256, original_name, mime, size_bytes, storage_uri_raw, status, created_at, updated_at FROM documents WHERE sha256 = ?",
            (sha,),
        ).fetchone()

        if existing:
            return {
                "id": existing[0],
                "sha256": existing[1],
                "original_name": existing[2],
                "mime": existing[3],
                "size_bytes": existing[4],
                "storage_uri_raw": existing[5],
                "status": existing[6],
                "created_at": existing[7],
                "updated_at": existing[8],
                "is_duplicate": True,
            }

        ingest_id = str(uuid.uuid4())
        now = _now_iso()
        payload = (ingest_id, sha, file_name, mime or "application/octet-stream", len(file_bytes), raw_path, STATUS_STORED, None, now, now)
        conn.execute(
            """
            INSERT INTO documents
            (id, sha256, original_name, mime, size_bytes, storage_uri_raw, status, error_message, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )

    return {
        "id": ingest_id,
        "sha256": sha,
        "original_name": file_name,
        "mime": mime or "application/octet-stream",
        "size_bytes": len(file_bytes),
        "storage_uri_raw": raw_path,
        "status": STATUS_STORED,
        "created_at": now,
        "updated_at": now,
        "is_duplicate": False,
    }


def list_ingest_documents(statuses: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    init_ingest_db()
    query = "SELECT id, sha256, original_name, mime, size_bytes, storage_uri_raw, status, error_message, created_at, updated_at FROM documents"
    params: Tuple[Any, ...] = ()
    if statuses:
        placeholders = ",".join(["?"] * len(statuses))
        query += f" WHERE status IN ({placeholders})"
        params = tuple(statuses)
    query += " ORDER BY created_at DESC"

    with sqlite3.connect(INGEST_DB_NAME) as conn:
        rows = conn.execute(query, params).fetchall()

    return [
        {
            "id": row[0],
            "sha256": row[1],
            "original_name": row[2],
            "mime": row[3],
            "size_bytes": row[4],
            "storage_uri_raw": row[5],
            "status": row[6],
            "error_message": row[7],
            "created_at": row[8],
            "updated_at": row[9],
        }
        for row in rows
    ]


def _update_ingest_status(document_id: str, status: str, error_message: Optional[str] = None) -> None:
    with sqlite3.connect(INGEST_DB_NAME) as conn:
        conn.execute(
            "UPDATE documents SET status = ?, error_message = ?, updated_at = ? WHERE id = ?",
            (status, error_message, _now_iso(), document_id),
        )


def _save_artifact(document_id: str, doc_sha: str, kind: str, relative_path: str, content: bytes, meta: Optional[Dict[str, Any]] = None) -> str:
    storage_uri = os.path.join("data", "artifacts", doc_sha, relative_path)
    _write_bytes(storage_uri, content)
    art_sha = _sha256_bytes(content)
    with sqlite3.connect(INGEST_DB_NAME) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO artifacts (document_id, kind, storage_uri, sha256, meta_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (document_id, kind, storage_uri, art_sha, json.dumps(meta or {}, ensure_ascii=False)),
        )
    return storage_uri


def _save_text_artifact(document_id: str, doc_sha: str, kind: str, relative_path: str, text: str, meta: Optional[Dict[str, Any]] = None) -> str:
    return _save_artifact(document_id, doc_sha, kind, relative_path, text.encode("utf-8"), meta=meta)


def _is_low_quality_payload(payload: List[Dict[str, Any]]) -> bool:
    """Heurística para detectar extrações pobres e tentar refinamento por LLM."""
    if not payload:
        return True

    low_quality = 0
    for item in payload:
        data = str(item.get("data") or "").strip()
        descricao = str(item.get("descricao") or "").strip()

        # data ausente/inválida
        if not data:
            low_quality += 1
        else:
            try:
                datetime.strptime(data, "%Y-%m-%d")
            except ValueError:
                low_quality += 1

        # descrição muito longa costuma indicar OCR bruto sem limpeza
        if len(descricao) > 120:
            low_quality += 1

    return low_quality > 0


def _run_llm_checks(payload: List[Dict[str, Any]]) -> Dict[str, Any]:
    logger.info("[LLM_REVIEW] Iniciando validações automáticas para %s transação(ões).", len(payload))
    issues = []
    total = 0.0
    entrada_count = 0
    saida_count = 0
    entrada_total = 0.0
    saida_total = 0.0

    for idx, item in enumerate(payload):
        valor = float(item.get("valor", 0.0) or 0.0)
        total += valor

        data = str(item.get("data") or "").strip()
        descricao = str(item.get("descricao") or "").strip()

        tipo_raw = str(item.get("tipo") or "").strip().lower()
        tipo = tipo_raw if tipo_raw in ["entrada", "saida"] else "saida"
        if tipo == "entrada":
            entrada_count += 1
            entrada_total += valor
        else:
            saida_count += 1
            saida_total += valor

        if not tipo_raw:
            issues.append({"index": idx, "rule": "missing_type", "detail": "Tipo ausente"})
        elif tipo_raw not in ["entrada", "saida"]:
            issues.append({"index": idx, "rule": "invalid_type", "detail": "Tipo inválido"})

        if not data:
            issues.append({"index": idx, "rule": "missing_date", "detail": "Data ausente"})
        else:
            try:
                parsed = datetime.strptime(data, "%Y-%m-%d").date()
                if parsed > datetime.utcnow().date():
                    issues.append({"index": idx, "rule": "future_date", "detail": "Data futura detectada"})
            except ValueError:
                issues.append({"index": idx, "rule": "invalid_date", "detail": "Data inválida"})

        if not descricao:
            issues.append({"index": idx, "rule": "missing_description", "detail": "Descrição ausente"})
        elif len(descricao) > 180:
            issues.append({"index": idx, "rule": "suspicious_description", "detail": "Descrição muito longa"})

        desc_upper = descricao.upper()
        if any(token in desc_upper for token in ["AUTENTICACAO", "TERMINAL", "PROTOCOLO"]):
            issues.append({"index": idx, "rule": "description_noise", "detail": "Descrição contém ruído de comprovante"})

        if abs(valor) > 1_000_000:
            issues.append({"index": idx, "rule": "absurd_value", "detail": "Valor muito alto"})
        if valor == 0:
            issues.append({"index": idx, "rule": "zero_value", "detail": "Valor zerado"})

    logger.info(
        "[LLM_REVIEW] Verificação por tipo concluída. entradas=%s (total=%.2f) | saídas=%s (total=%.2f)",
        entrada_count,
        entrada_total,
        saida_count,
        saida_total,
    )

    result = {
        "passed": len(issues) == 0,
        "confidence": 0.95 if not issues else 0.60,
        "issues": issues,
        "summary": {
            "count": len(payload),
            "sum_valor": total,
            "entrada": {"count": entrada_count, "sum_valor": entrada_total},
            "saida": {"count": saida_count, "sum_valor": saida_total},
        },
    }
    logger.info(
        "[LLM_REVIEW] Validações concluídas. passed=%s confidence=%.2f issues=%s",
        result["passed"],
        result["confidence"],
        len(issues),
    )
    return result


def _record_extraction(document_id: str, extractor: str, payload_uri: str, confidence: float, llm_checks_uri: str) -> None:
    with sqlite3.connect(INGEST_DB_NAME) as conn:
        conn.execute(
            """
            INSERT INTO extractions (document_id, extractor, payload_uri, confidence, llm_checks_uri, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (document_id, extractor, payload_uri, float(confidence), llm_checks_uri, "PENDING"),
        )


def run_pipeline_for_document(document_id: str) -> Tuple[bool, str]:
    init_ingest_db()
    with sqlite3.connect(INGEST_DB_NAME) as conn:
        doc = conn.execute(
            "SELECT id, sha256, original_name, mime, size_bytes, storage_uri_raw, status FROM documents WHERE id = ?",
            (document_id,),
        ).fetchone()

    if not doc:
        return False, "Documento não encontrado na ingestão."

    _, sha, original_name, mime, _, raw_uri, status = doc
    if status != STATUS_STORED:
        return False, f"Documento {document_id} não está em STORED."

    if not os.path.exists(raw_uri):
        _update_ingest_status(document_id, STATUS_ERROR_STORAGE, "Arquivo raw não encontrado")
        return False, "Arquivo raw não encontrado."

    with open(raw_uri, "rb") as handler:
        raw_bytes = handler.read()
    if _sha256_bytes(raw_bytes) != sha:
        _update_ingest_status(document_id, STATUS_ERROR_STORAGE, "SHA256 divergente do raw")
        return False, "SHA divergente do raw."

    _update_ingest_status(document_id, STATUS_PROCESSING)
    logger.info("[PIPELINE] Documento %s (%s) movido para PROCESSING.", document_id, original_name)

    try:
        text_content = ""
        extractor = "regex"
        ext = os.path.splitext(original_name.lower())[1]

        class _UploadWrap(io.BytesIO):
            def __init__(self, data: bytes, name: str, mime_type: str):
                super().__init__(data)
                self.name = name
                self.type = mime_type

        if ext in [".csv", ".xlsx"]:
            upload = _UploadWrap(raw_bytes, original_name, mime)
            df_plan, err = processar_planilha(upload)
            if err:
                raise RuntimeError(err)
            candidate = df_plan[["data", "valor", "descricao", "categoria", "tipo"]].copy()
            candidate["data"] = pd.to_datetime(candidate["data"], errors="coerce").dt.strftime("%Y-%m-%d")
            payload = candidate.fillna("").to_dict(orient="records")
        elif ext == ".pdf":
            upload = _UploadWrap(raw_bytes, original_name, mime)
            text_pdf, is_scanned, err_pdf = extrair_texto_pdf(upload)
            if err_pdf:
                raise RuntimeError(err_pdf)
            text_content = text_pdf
            if is_scanned:
                upload.seek(0)
                imgs, err_img = converter_pdf_para_imagens(upload)
                if err_img:
                    raise RuntimeError(err_img)
                chunks = []
                for idx, img_buffer in enumerate(imgs, start=1):
                    img_bytes = img_buffer.getvalue()
                    _save_artifact(document_id, sha, "pdf_page_image", f"pdf_pages/page-{idx:03d}.png", img_bytes, meta={"page": idx})
                    img_buffer.seek(0)
                    txt, _, _ = extrair_texto_imagem(img_buffer)
                    if txt:
                        chunks.append(txt)
                text_content = "\n".join(chunks)

            _save_text_artifact(document_id, sha, "ocr_text", "ocr/text.txt", text_content, meta={"source": "pdf"})
            payload = extrair_dados_financeiros(text_content)
            needs_llm_refine = _is_low_quality_payload(payload)
            if not payload or needs_llm_refine:
                motivo = "sem resultado" if not payload else "qualidade baixa"
                logger.info(
                    "[PIPELINE] Regex %s para %s. Iniciando extração/refino via LLM.",
                    motivo,
                    document_id,
                )
                llm_payload, llm_err = extrair_dados_financeiros_llm(text_content)
                if llm_err and not llm_payload and not payload:
                    logger.warning("[PIPELINE] Falha na extração LLM para %s: %s", document_id, llm_err)
                    payload = []
                elif llm_err and payload:
                    logger.warning("[PIPELINE] LLM não refinou %s: %s", document_id, llm_err)
                elif llm_payload:
                    payload = llm_payload
                    extractor = "llm"
                    logger.info("[PIPELINE] Extração LLM concluída para %s com %s item(ns).", document_id, len(payload))
        else:
            upload = _UploadWrap(raw_bytes, original_name, mime)
            txt, _, ocr_err = extrair_texto_imagem(upload)
            if ocr_err:
                raise RuntimeError(ocr_err)
            text_content = txt
            _save_text_artifact(document_id, sha, "ocr_text", "ocr/text.txt", text_content, meta={"source": "image"})
            payload = extrair_dados_financeiros(text_content)
            needs_llm_refine = _is_low_quality_payload(payload)
            if not payload or needs_llm_refine:
                motivo = "sem resultado" if not payload else "qualidade baixa"
                logger.info(
                    "[PIPELINE] Regex %s para %s. Iniciando extração/refino via LLM.",
                    motivo,
                    document_id,
                )
                llm_payload, llm_err = extrair_dados_financeiros_llm(text_content)
                if llm_err and not llm_payload and not payload:
                    logger.warning("[PIPELINE] Falha na extração LLM para %s: %s", document_id, llm_err)
                    payload = []
                elif llm_err and payload:
                    logger.warning("[PIPELINE] LLM não refinou %s: %s", document_id, llm_err)
                elif llm_payload:
                    payload = llm_payload
                    extractor = "llm"
                    logger.info("[PIPELINE] Extração LLM concluída para %s com %s item(ns).", document_id, len(payload))

        payload_uri = os.path.join("data", "artifacts", sha, "extraction", "candidate.json")
        checks_uri = os.path.join("data", "artifacts", sha, "extraction", "llm_checks.json")
        checks = _run_llm_checks(payload)
        _write_json(payload_uri, payload)
        _write_json(checks_uri, checks)

        _save_text_artifact(document_id, sha, "extracted_json", "extraction/candidate.json", json.dumps(payload, ensure_ascii=False), meta={"extractor": extractor})
        _save_text_artifact(document_id, sha, "llm_checks", "extraction/llm_checks.json", json.dumps(checks, ensure_ascii=False), meta={"extractor": "checks"})
        _record_extraction(document_id, extractor, payload_uri, checks["confidence"], checks_uri)
        logger.info(
            "[PIPELINE] Documento %s processado. extractor=%s payload_items=%s confidence=%.2f",
            document_id,
            extractor,
            len(payload),
            checks["confidence"],
        )

        _update_ingest_status(document_id, STATUS_LLM_REVIEW)
        logger.info("[LLM_REVIEW] Documento %s movido para LLM_REVIEW.", document_id)
        _update_ingest_status(document_id, STATUS_HITL_REVIEW)
        logger.info("[LLM_REVIEW] Documento %s movido para HITL_REVIEW.", document_id)
        return True, "Pipeline concluída e enviado para HITL_REVIEW."
    except Exception as exc:
        _update_ingest_status(document_id, STATUS_ERROR_PROCESSING, str(exc))
        logger.exception("[PIPELINE] Falha no processamento do documento %s.", document_id)
        return False, str(exc)


def process_stored_documents(limit: int = 20) -> Dict[str, int]:
    docs = list_ingest_documents([STATUS_STORED])[: max(1, int(limit))]
    processed = 0
    failed = 0
    for doc in docs:
        ok, _ = run_pipeline_for_document(doc["id"])
        if ok:
            processed += 1
        else:
            failed += 1
    return {"processed": processed, "failed": failed, "found": len(docs)}


def get_latest_extraction_payload(document_id: str) -> Optional[Tuple[str, List[Dict[str, Any]], Dict[str, Any]]]:
    with sqlite3.connect(INGEST_DB_NAME) as conn:
        row = conn.execute(
            "SELECT payload_uri, llm_checks_uri FROM extractions WHERE document_id = ? ORDER BY created_at DESC, id DESC LIMIT 1",
            (document_id,),
        ).fetchone()
    if not row:
        return None
    payload_uri, checks_uri = row
    if not os.path.exists(payload_uri):
        return None
    with open(payload_uri, "r", encoding="utf-8") as handler:
        payload = json.load(handler)
    checks = {}
    if checks_uri and os.path.exists(checks_uri):
        with open(checks_uri, "r", encoding="utf-8") as handler:
            checks = json.load(handler)
    return payload_uri, payload, checks


def submit_hitl_review(document_id: str, reviewer: str, decision: str, edited_payload: List[Dict[str, Any]], notes: str = "") -> str:
    with sqlite3.connect(INGEST_DB_NAME) as conn:
        row = conn.execute("SELECT sha256 FROM documents WHERE id = ?", (document_id,)).fetchone()
    if not row:
        raise ValueError("Documento não encontrado.")
    sha = row[0]
    review_uri = os.path.join("data", "artifacts", sha, "review", "approved.json")
    _write_json(review_uri, edited_payload)

    with sqlite3.connect(INGEST_DB_NAME) as conn:
        conn.execute(
            "INSERT INTO reviews (document_id, reviewer, decision, edited_payload_uri, notes) VALUES (?, ?, ?, ?, ?)",
            (document_id, reviewer, decision, review_uri, notes),
        )

    if decision in ["APPROVED", "CHANGES"]:
        _update_ingest_status(document_id, STATUS_FINALIZE_PENDING)
    else:
        _update_ingest_status(document_id, STATUS_HITL_REVIEW)

    return review_uri


def finalize_document(document_id: str) -> Tuple[bool, str]:
    init_db()
    init_ingest_db()
    with sqlite3.connect(INGEST_DB_NAME) as conn:
        doc = conn.execute(
            "SELECT id, original_name, status FROM documents WHERE id = ?",
            (document_id,),
        ).fetchone()
        review = conn.execute(
            "SELECT edited_payload_uri FROM reviews WHERE document_id = ? AND decision IN ('APPROVED', 'CHANGES') ORDER BY created_at DESC, id DESC LIMIT 1",
            (document_id,),
        ).fetchone()

    if not doc:
        return False, "Documento não encontrado."
    if doc[2] not in [STATUS_FINALIZE_PENDING, STATUS_FINALIZED]:
        return False, f"Status inválido para finalização: {doc[2]}"
    if doc[2] == STATUS_FINALIZED:
        return True, "Documento já finalizado (idempotente)."
    if not review:
        return False, "Documento sem review aprovado."

    review_uri = review[0]
    if not os.path.exists(review_uri):
        return False, "Payload revisado não encontrado."

    with open(review_uri, "r", encoding="utf-8") as handler:
        payload = json.load(handler)

    rows = []
    for item in payload:
        desc = str(item.get("descricao") or "").strip()
        if not desc:
            continue
        data = str(item.get("data") or datetime.now().strftime("%Y-%m-%d"))
        valor = abs(float(item.get("valor") or 0.0))
        tipo = str(item.get("tipo") or "saida").strip().lower()
        if tipo not in ["entrada", "saida"]:
            tipo = "saida"
        rows.append(
            {
                "data": data,
                "descricao": desc,
                "valor": valor,
                "fonte": doc[1],
                "categoria": str(item.get("categoria") or "Outros"),
                "tipo": tipo,
                "document_id": document_id,
            }
        )

    if not rows:
        return False, "Sem transações válidas para finalizar."

    df = pd.DataFrame(rows)
    insert_transactions(df)

    with sqlite3.connect(DB_NAME) as conn:
        for _, row in df.iterrows():
            tx_id = find_transaction_id(row["data"], row["descricao"], float(row["valor"]), row["fonte"], row["tipo"])
            if tx_id:
                conn.execute("UPDATE transacoes SET document_id = ? WHERE id = ?", (document_id, tx_id))

    _update_ingest_status(document_id, STATUS_FINALIZED)
    return True, "Finalização concluída."


def finalize_pending_documents(limit: int = 20) -> Dict[str, int]:
    docs = list_ingest_documents([STATUS_FINALIZE_PENDING])[: max(1, int(limit))]
    finalized = 0
    failed = 0
    for doc in docs:
        ok, _ = finalize_document(doc["id"])
        if ok:
            finalized += 1
        else:
            failed += 1
    return {"finalized": finalized, "failed": failed, "found": len(docs)}
