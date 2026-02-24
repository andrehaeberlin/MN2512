import base64
import datetime
import io
import json
from typing import Any, Dict, List

import pandas as pd
import streamlit as st
from localDB import (
    STATUS_FINALIZE_PENDING,
    STATUS_HITL_REVIEW,
    STATUS_PROCESSING,
    STATUS_STORED,
    finalize_pending_documents,
    get_all_transactions,
    get_document_items,
    get_document_summaries,
    get_latest_extraction_payload,
    init_db,
    init_ingest_db,
    list_ingest_documents,
    process_stored_documents,
    insert_transactions,
    store_raw_document,
    submit_hitl_review,
)

init_db()
init_ingest_db()

st.set_page_config(page_title="Extrator Pro MVP", page_icon="💰", layout="wide")


SUMMARY_TOKENS = ["total", "valor pago", "valor a pagar", "forma pagamento", "pagamento", "desconto"]
REVIEW_RENDER_CALLS = 0


def _is_summary_line(descricao: str) -> bool:
    desc = (descricao or "").strip().lower()
    return bool(desc) and any(token in desc for token in SUMMARY_TOKENS)


def _normalize_payload_for_editor(payload: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for item in payload:
        descricao = str(item.get("descricao") or "").strip()
        rows.append(
            {
                "data": str(item.get("data") or "").strip(),
                "descricao": descricao,
                "valor": float(item.get("valor") or 0.0),
                "tipo": str(item.get("tipo") or "saida").strip().lower() or "saida",
                "categoria": str(item.get("categoria") or "Outros").strip() or "Outros",
                "is_resumo": _is_summary_line(descricao),
            }
        )
    return pd.DataFrame(rows, columns=["data", "descricao", "valor", "tipo", "categoria", "is_resumo"])


def _payload_from_editor(df_editor: pd.DataFrame) -> List[Dict[str, Any]]:
    payload = []
    if df_editor.empty:
        return payload

    tmp = df_editor.copy()
    tmp["descricao"] = tmp["descricao"].fillna("").astype(str).str.strip()
    tmp["tipo"] = tmp["tipo"].fillna("saida").astype(str).str.strip().str.lower()
    tmp.loc[~tmp["tipo"].isin(["entrada", "saida"]), "tipo"] = "saida"
    tmp["categoria"] = tmp["categoria"].fillna("Outros").astype(str).str.strip()
    tmp["data"] = tmp["data"].fillna("").astype(str).str.strip()
    tmp["valor"] = pd.to_numeric(tmp["valor"], errors="coerce").fillna(0.0)

    for _, row in tmp.iterrows():
        if not row["descricao"]:
            continue
        payload.append(
            {
                "data": row["data"],
                "descricao": row["descricao"],
                "valor": float(row["valor"]),
                "tipo": row["tipo"],
                "categoria": row["categoria"],
            }
        )
    return payload


def _looks_generic_product(descricao: str) -> bool:
    desc = (descricao or "").strip().lower()
    if not desc:
        return False
    return bool(pd.Series([desc]).str.match(r"^produto\s*\d+$", case=False).iloc[0])


def _apply_receipt_cleanup(df_editor: pd.DataFrame) -> pd.DataFrame:
    """Remove linhas genéricas de produto quando houver itens detalhados no mesmo payload."""
    if df_editor.empty:
        return df_editor

    df = df_editor.copy()
    generic_mask = df["descricao"].astype(str).apply(_looks_generic_product)
    detailed_mask = (~generic_mask) & (~df["is_resumo"].astype(bool)) & df["descricao"].astype(str).str.len().ge(10)

    if detailed_mask.any() and generic_mask.any():
        df = df[~generic_mask].reset_index(drop=True)
    return df


def render_preview(arq):
    ext = arq.name.split(".")[-1].lower()
    if ext in ["png", "jpg", "jpeg"]:
        st.image(arq, width="stretch")
        return
    if ext == "pdf":
        b64 = base64.b64encode(arq.getvalue()).decode("utf-8")
        st.components.v1.html(
            f"""
            <iframe src="data:application/pdf;base64,{b64}" width="100%" height="580" style="border:none;"></iframe>
            """,
            height=580,
        )
        return
    if ext in ["csv", "xlsx"]:
        try:
            if ext == "csv":
                df = pd.read_csv(io.BytesIO(arq.getvalue()))
            else:
                df = pd.read_excel(io.BytesIO(arq.getvalue()))
            st.dataframe(df.head(30), width="stretch")
        except Exception as exc:
            st.warning(f"Preview indisponível: {exc}")
        return
    st.download_button("Baixar arquivo", data=arq.getvalue(), file_name=arq.name)


def render_import_store():
    st.title("IMPORT / STORE (raw)")
    arquivos = st.file_uploader(
        "Importar e Armazenar (sem processar)",
        type=["xlsx", "csv", "pdf", "png", "jpg", "jpeg", "ofx"],
        accept_multiple_files=True,
    )

    if not arquivos:
        return

    st.subheader("Pré-visualização")
    for arq in arquivos:
        with st.expander(arq.name):
            render_preview(arq)

    if st.button("💾 Importar e Armazenar", type="primary"):
        salvos = 0
        duplicados = 0
        for arq in arquivos:
            doc = store_raw_document(arq.name, arq.type or "application/octet-stream", arq.getvalue())
            if doc["is_duplicate"]:
                duplicados += 1
            else:
                salvos += 1
        st.success(f"Salvo com sucesso. Novos: {salvos} | Duplicados (sha256): {duplicados}")


def render_pipeline():
    st.title("Pipeline")
    docs = list_ingest_documents([STATUS_STORED, STATUS_PROCESSING, STATUS_HITL_REVIEW])
    if docs:
        st.dataframe(pd.DataFrame(docs), width="stretch")
    else:
        st.info("Sem documentos nesses estados.")

    if st.button("⚙️ Processar pendências (STORED)"):
        result = process_stored_documents(limit=50)
        st.success(
            f"Processamento concluído. Encontrados={result['found']} Processados={result['processed']} Falhas={result['failed']}"
        )
        st.rerun()


def render_review():
    global REVIEW_RENDER_CALLS
    REVIEW_RENDER_CALLS += 1
    if REVIEW_RENDER_CALLS > 1:
        st.warning("Tela de revisão já renderizada nesta execução. Ignorando render duplicado.")
        return

    st.title("HITL_REVIEW")
    docs = list_ingest_documents([STATUS_HITL_REVIEW])
    if not docs:
        st.info("Nenhum documento aguardando revisão.")
        return

    labels = {d["id"]: f"{d['original_name']} ({d['id'][:8]})" for d in docs}
    selected = st.selectbox(
        "Documento",
        options=list(labels.keys()),
        format_func=lambda x: labels[x],
        key="review_selected_document",
    )

    payload_info = get_latest_extraction_payload(selected)
    if not payload_info:
        st.warning("Sem payload de extração para este documento.")
        return

    payload_uri, payload, checks = payload_info
    st.caption(f"Payload: {payload_uri}")

    with st.expander("Ver checks automáticos (LLM_REVIEW)", expanded=False):
        st.json(checks)

    st.subheader("Edição assistida")
    st.caption("Camada 1: totais/resumo da nota • Camada 2: itens detalhados")
    df_edit_base = _normalize_payload_for_editor(payload)
    doc_key = f"review_{selected}"

    if df_edit_base.empty:
        st.warning("Payload vazio para edição.")
    else:
        total_itens = float(df_edit_base.loc[~df_edit_base["is_resumo"], "valor"].sum())
        declared_candidates = df_edit_base.loc[df_edit_base["is_resumo"], "valor"].tolist()
        total_declarado = float(declared_candidates[-1]) if declared_candidates else None
        status_conf = "⚠️ sem total declarado"
        if total_declarado is not None:
            status_conf = "✅ confere" if abs(total_declarado - total_itens) <= 0.01 else "❌ divergente"

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Itens (qtd)", int((~df_edit_base["is_resumo"]).sum()))
        c2.metric("Total dos itens", f"R$ {total_itens:.2f}")
        c3.metric("Total declarado", f"R$ {total_declarado:.2f}" if total_declarado is not None else "N/A")
        c4.metric("Status", status_conf)

    st.markdown("**Camada 1 · Resumo / Totais**")
    total_declarado_manual = st.number_input(
        "Total declarado da nota (editável)",
        min_value=0.0,
        step=0.01,
        format="%.2f",
        value=float(total_declarado) if "total_declarado" in locals() and total_declarado is not None else 0.0,
        key=f"{doc_key}_total_declarado",
        help="Use este campo para informar/ajustar o valor pago na nota (ex.: 374,96).",
    )

    categorias_validas = ["Alimentação", "Transporte", "Serviços", "Outros"]
    st.markdown("**Camada 2 · Itens detalhados**")
    df_edit = st.data_editor(
        df_edit_base,
        num_rows="dynamic",
        width="stretch",
        key=f"{doc_key}_data_editor",
        column_config={
            "data": st.column_config.TextColumn("Data (YYYY-MM-DD)", help="Mantenha no formato YYYY-MM-DD"),
            "descricao": st.column_config.TextColumn("Descrição", width="large", required=True),
            "valor": st.column_config.NumberColumn("Valor", format="%.2f", required=True),
            "tipo": st.column_config.SelectboxColumn("Tipo", options=["entrada", "saida"], required=True),
            "categoria": st.column_config.SelectboxColumn("Categoria", options=categorias_validas, required=True),
            "is_resumo": st.column_config.CheckboxColumn("Linha de resumo?", help="Marque para Total/Pagamento/Desconto"),
        },
    )

    left, right = st.columns(2)
    with left:
        if st.button("Remover linhas marcadas como resumo", key=f"{doc_key}_btn_remove_summary"):
            if not df_edit.empty:
                df_edit = df_edit[~df_edit["is_resumo"]].reset_index(drop=True)
                st.success("Linhas de resumo removidas da revisão.")
    with right:
        st.caption("Dica: mantenha linhas de resumo apenas para conferência; elas não viram itens finais.")

    if st.button("Aplicar limpeza automática (remover 'Produto X' genérico)", key=f"{doc_key}_btn_cleanup_generic"):
        df_edit = _apply_receipt_cleanup(df_edit)
        st.success("Limpeza aplicada. Itens genéricos removidos quando havia descrição detalhada.")

    edited_payload_data = _payload_from_editor(df_edit)

    if total_declarado_manual > 0:
        edited_payload_data = [
            item
            for item in edited_payload_data
            if not _is_summary_line(str(item.get("descricao") or ""))
        ]
        edited_payload_data.append(
            {
                "data": edited_payload_data[0]["data"] if edited_payload_data else "",
                "descricao": "Total declarado",
                "valor": float(total_declarado_manual),
                "tipo": "saida",
                "categoria": "Outros",
            }
        )

    with st.expander("Ver checks automáticos (LLM_REVIEW)", expanded=False):
        st.json(checks)

    st.subheader("Edição assistida")
    st.caption("Camada 1: totais/resumo da nota • Camada 2: itens detalhados")
    df_edit_base = _normalize_payload_for_editor(payload)
    doc_key = str(selected)

    if df_edit_base.empty:
        st.warning("Payload vazio para edição.")
    else:
        total_itens = float(df_edit_base.loc[~df_edit_base["is_resumo"], "valor"].sum())
        declared_candidates = df_edit_base.loc[df_edit_base["is_resumo"], "valor"].tolist()
        total_declarado = float(declared_candidates[-1]) if declared_candidates else None
        status_conf = "⚠️ sem total declarado"
        if total_declarado is not None:
            status_conf = "✅ confere" if abs(total_declarado - total_itens) <= 0.01 else "❌ divergente"

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Itens (qtd)", int((~df_edit_base["is_resumo"]).sum()))
        c2.metric("Total dos itens", f"R$ {total_itens:.2f}")
        c3.metric("Total declarado", f"R$ {total_declarado:.2f}" if total_declarado is not None else "N/A")
        c4.metric("Status", status_conf)

    st.markdown("**Camada 1 · Resumo / Totais**")
    total_declarado_manual = st.number_input(
        "Total declarado da nota (editável)",
        min_value=0.0,
        step=0.01,
        format="%.2f",
        value=float(total_declarado) if "total_declarado" in locals() and total_declarado is not None else 0.0,
        key=f"review_total_declarado_{doc_key}",
        help="Use este campo para informar/ajustar o valor pago na nota (ex.: 374,96).",
    )

    categorias_validas = ["Alimentação", "Transporte", "Serviços", "Outros"]
    st.markdown("**Camada 2 · Itens detalhados**")
    df_edit = st.data_editor(
        df_edit_base,
        num_rows="dynamic",
        width="stretch",
        key=f"hitl_review_data_editor_{doc_key}",
        column_config={
            "data": st.column_config.TextColumn("Data (YYYY-MM-DD)", help="Mantenha no formato YYYY-MM-DD"),
            "descricao": st.column_config.TextColumn("Descrição", width="large", required=True),
            "valor": st.column_config.NumberColumn("Valor", format="%.2f", required=True),
            "tipo": st.column_config.SelectboxColumn("Tipo", options=["entrada", "saida"], required=True),
            "categoria": st.column_config.SelectboxColumn("Categoria", options=categorias_validas, required=True),
            "is_resumo": st.column_config.CheckboxColumn("Linha de resumo?", help="Marque para Total/Pagamento/Desconto"),
        },
    )

    left, right = st.columns(2)
    with left:
        if st.button("Remover linhas marcadas como resumo", key=f"btn_remove_summary_{doc_key}"):
            if not df_edit.empty:
                df_edit = df_edit[~df_edit["is_resumo"]].reset_index(drop=True)
                st.success("Linhas de resumo removidas da revisão.")
    with right:
        st.caption("Dica: mantenha linhas de resumo apenas para conferência; elas não viram itens finais.")

    if st.button("Aplicar limpeza automática (remover 'Produto X' genérico)", key=f"btn_cleanup_generic_{doc_key}"):
        df_edit = _apply_receipt_cleanup(df_edit)
        st.success("Limpeza aplicada. Itens genéricos removidos quando havia descrição detalhada.")

    edited_payload_data = _payload_from_editor(df_edit)

    if total_declarado_manual > 0:
        edited_payload_data = [
            item
            for item in edited_payload_data
            if not _is_summary_line(str(item.get("descricao") or ""))
        ]
        edited_payload_data.append(
            {
                "data": edited_payload_data[0]["data"] if edited_payload_data else "",
                "descricao": "Total declarado",
                "valor": float(total_declarado_manual),
                "tipo": "saida",
                "categoria": "Outros",
            }
        )

    with st.expander("Ver checks automáticos (LLM_REVIEW)", expanded=False):
        st.json(checks)

    st.subheader("Edição assistida")
    df_edit_base = _normalize_payload_for_editor(payload)

    if df_edit_base.empty:
        st.warning("Payload vazio para edição.")
    else:
        total_itens = float(df_edit_base.loc[~df_edit_base["is_resumo"], "valor"].sum())
        declared_candidates = df_edit_base.loc[df_edit_base["is_resumo"], "valor"].tolist()
        total_declarado = float(declared_candidates[-1]) if declared_candidates else None
        status_conf = "⚠️ sem total declarado"
        if total_declarado is not None:
            status_conf = "✅ confere" if abs(total_declarado - total_itens) <= 0.01 else "❌ divergente"

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Itens (qtd)", int((~df_edit_base["is_resumo"]).sum()))
        c2.metric("Total dos itens", f"R$ {total_itens:.2f}")
        c3.metric("Total declarado", f"R$ {total_declarado:.2f}" if total_declarado is not None else "N/A")
        c4.metric("Status", status_conf)

    total_declarado_manual = st.number_input(
        "Total declarado da nota (editável)",
        min_value=0.0,
        step=0.01,
        format="%.2f",
        value=float(total_declarado) if "total_declarado" in locals() and total_declarado is not None else 0.0,
        help="Use este campo para informar/ajustar o valor pago na nota (ex.: 374,96).",
    )

    categorias_validas = ["Alimentação", "Transporte", "Serviços", "Outros"]
    df_edit = st.data_editor(
        df_edit_base,
        num_rows="dynamic",
        width="stretch",
        key=f"hitl_review_data_editor_{selected}",
        column_config={
            "data": st.column_config.TextColumn("Data (YYYY-MM-DD)", help="Mantenha no formato YYYY-MM-DD"),
            "descricao": st.column_config.TextColumn("Descrição", width="large", required=True),
            "valor": st.column_config.NumberColumn("Valor", format="%.2f", required=True),
            "tipo": st.column_config.SelectboxColumn("Tipo", options=["entrada", "saida"], required=True),
            "categoria": st.column_config.SelectboxColumn("Categoria", options=categorias_validas, required=True),
            "is_resumo": st.column_config.CheckboxColumn("Linha de resumo?", help="Marque para Total/Pagamento/Desconto"),
        },
    )

    left, right = st.columns(2)
    with left:
        if st.button("Remover linhas marcadas como resumo"):
            if not df_edit.empty:
                df_edit = df_edit[~df_edit["is_resumo"]].reset_index(drop=True)
                st.success("Linhas de resumo removidas da revisão.")
    with right:
        st.caption("Dica: mantenha linhas de resumo apenas para conferência; elas não viram itens finais.")

    if st.button("Aplicar limpeza automática (remover 'Produto X' genérico)"):
        df_edit = _apply_receipt_cleanup(df_edit)
        st.success("Limpeza aplicada. Itens genéricos removidos quando havia descrição detalhada.")

    edited_payload_data = _payload_from_editor(df_edit)

    if total_declarado_manual > 0:
        edited_payload_data = [
            item
            for item in edited_payload_data
            if not _is_summary_line(str(item.get("descricao") or ""))
        ]
        edited_payload_data.append(
            {
                "data": edited_payload_data[0]["data"] if edited_payload_data else "",
                "descricao": "Total declarado",
                "valor": float(total_declarado_manual),
                "tipo": "saida",
                "categoria": "Outros",
            }
        )

    with st.expander("Ver checks automáticos (LLM_REVIEW)", expanded=False):
        st.json(checks)

    st.subheader("Edição assistida")
    df_edit_base = _normalize_payload_for_editor(payload)

    if df_edit_base.empty:
        st.warning("Payload vazio para edição.")
    else:
        total_itens = float(df_edit_base.loc[~df_edit_base["is_resumo"], "valor"].sum())
        declared_candidates = df_edit_base.loc[df_edit_base["is_resumo"], "valor"].tolist()
        total_declarado = float(declared_candidates[-1]) if declared_candidates else None
        status_conf = "⚠️ sem total declarado"
        if total_declarado is not None:
            status_conf = "✅ confere" if abs(total_declarado - total_itens) <= 0.01 else "❌ divergente"

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Itens (qtd)", int((~df_edit_base["is_resumo"]).sum()))
        c2.metric("Total dos itens", f"R$ {total_itens:.2f}")
        c3.metric("Total declarado", f"R$ {total_declarado:.2f}" if total_declarado is not None else "N/A")
        c4.metric("Status", status_conf)

    total_declarado_manual = st.number_input(
        "Total declarado da nota (editável)",
        min_value=0.0,
        step=0.01,
        format="%.2f",
        value=float(total_declarado) if "total_declarado" in locals() and total_declarado is not None else 0.0,
        help="Use este campo para informar/ajustar o valor pago na nota (ex.: 374,96).",
    )

    categorias_validas = ["Alimentação", "Transporte", "Serviços", "Outros"]
    df_edit = st.data_editor(
        df_edit_base,
        num_rows="dynamic",
        width="stretch",
        column_config={
            "data": st.column_config.TextColumn("Data (YYYY-MM-DD)", help="Mantenha no formato YYYY-MM-DD"),
            "descricao": st.column_config.TextColumn("Descrição", width="large", required=True),
            "valor": st.column_config.NumberColumn("Valor", format="%.2f", required=True),
            "tipo": st.column_config.SelectboxColumn("Tipo", options=["entrada", "saida"], required=True),
            "categoria": st.column_config.SelectboxColumn("Categoria", options=categorias_validas, required=True),
            "is_resumo": st.column_config.CheckboxColumn("Linha de resumo?", help="Marque para Total/Pagamento/Desconto"),
        },
    )

    left, right = st.columns(2)
    with left:
        if st.button("Remover linhas marcadas como resumo"):
            if not df_edit.empty:
                df_edit = df_edit[~df_edit["is_resumo"]].reset_index(drop=True)
                st.success("Linhas de resumo removidas da revisão.")
    with right:
        st.caption("Dica: mantenha linhas de resumo apenas para conferência; elas não viram itens finais.")

    if st.button("Aplicar limpeza automática (remover 'Produto X' genérico)"):
        df_edit = _apply_receipt_cleanup(df_edit)
        st.success("Limpeza aplicada. Itens genéricos removidos quando havia descrição detalhada.")

    edited_payload_data = _payload_from_editor(df_edit)

    if total_declarado_manual > 0:
        edited_payload_data = [
            item
            for item in edited_payload_data
            if not _is_summary_line(str(item.get("descricao") or ""))
        ]
        edited_payload_data.append(
            {
                "data": edited_payload_data[0]["data"] if edited_payload_data else "",
                "descricao": "Total declarado",
                "valor": float(total_declarado_manual),
                "tipo": "saida",
                "categoria": "Outros",
            }
        )

    with st.expander("Ver checks automáticos (LLM_REVIEW)", expanded=False):
        st.json(checks)

    st.subheader("Edição assistida")
    df_edit_base = _normalize_payload_for_editor(payload)

    if df_edit_base.empty:
        st.warning("Payload vazio para edição.")
    else:
        total_itens = float(df_edit_base.loc[~df_edit_base["is_resumo"], "valor"].sum())
        declared_candidates = df_edit_base.loc[df_edit_base["is_resumo"], "valor"].tolist()
        total_declarado = float(declared_candidates[-1]) if declared_candidates else None
        status_conf = "⚠️ sem total declarado"
        if total_declarado is not None:
            status_conf = "✅ confere" if abs(total_declarado - total_itens) <= 0.01 else "❌ divergente"

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Itens (qtd)", int((~df_edit_base["is_resumo"]).sum()))
        c2.metric("Total dos itens", f"R$ {total_itens:.2f}")
        c3.metric("Total declarado", f"R$ {total_declarado:.2f}" if total_declarado is not None else "N/A")
        c4.metric("Status", status_conf)

    categorias_validas = ["Alimentação", "Transporte", "Serviços", "Outros"]
    df_edit = st.data_editor(
        df_edit_base,
        num_rows="dynamic",
        width="stretch",
        column_config={
            "data": st.column_config.TextColumn("Data (YYYY-MM-DD)", help="Mantenha no formato YYYY-MM-DD"),
            "descricao": st.column_config.TextColumn("Descrição", width="large", required=True),
            "valor": st.column_config.NumberColumn("Valor", format="%.2f", required=True),
            "tipo": st.column_config.SelectboxColumn("Tipo", options=["entrada", "saida"], required=True),
            "categoria": st.column_config.SelectboxColumn("Categoria", options=categorias_validas, required=True),
            "is_resumo": st.column_config.CheckboxColumn("Linha de resumo?", help="Marque para Total/Pagamento/Desconto"),
        },
    )

    left, right = st.columns(2)
    with left:
        if st.button("Remover linhas marcadas como resumo"):
            if not df_edit.empty:
                df_edit = df_edit[~df_edit["is_resumo"]].reset_index(drop=True)
                st.success("Linhas de resumo removidas da revisão.")
    with right:
        st.caption("Dica: mantenha linhas de resumo apenas para conferência; elas não viram itens finais.")

    edited_payload_data = _payload_from_editor(df_edit)
    edited_json = st.text_area(
        "Payload final (JSON)",
        value=json.dumps(edited_payload_data, ensure_ascii=False, indent=2),
        height=280,
        key=f"{doc_key}_payload_json",
        help="Você ainda pode ajustar manualmente o JSON antes de salvar.",
    )
    reviewer = st.text_input("Reviewer", value="operador", key=f"{doc_key}_reviewer")
    decision = st.selectbox("Decisão", ["APPROVED", "CHANGES", "REJECTED"], key=f"{doc_key}_decision")
    notes = st.text_area("Notas", key=f"{doc_key}_notes")

    if st.button("Salvar revisão", key=f"{doc_key}_btn_save"):
        try:
            edited_payload = json.loads(edited_json)
            if not isinstance(edited_payload, list):
                st.error("Payload deve ser uma lista JSON.")
                return
            uri = submit_hitl_review(selected, reviewer, decision, edited_payload, notes)
            st.success(f"Revisão salva em {uri}")
            st.rerun()
        except json.JSONDecodeError as exc:
            st.error(f"JSON inválido: {exc}")


def render_finalize():
    st.title("FINALIZE")
    docs = list_ingest_documents([STATUS_FINALIZE_PENDING])
    if docs:
        st.dataframe(pd.DataFrame(docs), width="stretch")
    else:
        st.info("Nenhum documento pendente de finalização.")

    if st.button("✅ Finalizar aprovados"):
        result = finalize_pending_documents(limit=50)
        st.success(
            f"Finalização concluída. Encontrados={result['found']} Finalizados={result['finalized']} Falhas={result['failed']}"
        )
        st.rerun()


def render_history_section():
    st.title("📜 Histórico de Transações")
    st.subheader("Notas/Documentos (resumo)")

    df_resumos = get_document_summaries()
    selected_doc_id = None
    if df_resumos.empty:
        st.info("Nenhum resumo de documento encontrado.")
    else:
        df_resumos_view = df_resumos.copy()
        df_resumos_view["data_documento"] = pd.to_datetime(df_resumos_view["data_documento"], errors="coerce")
        df_resumos_view["status_total"] = df_resumos_view["total_confere"].map({1: "✅ confere", 0: "❌ divergente"}).fillna("⚠️ sem total declarado")
        st.dataframe(
            df_resumos_view[
                [
                    "document_id",
                    "fonte",
                    "data_documento",
                    "qtd_itens",
                    "total_itens",
                    "total_declarado",
                    "status_total",
                    "criado_em",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

        options = ["Todos"] + df_resumos["document_id"].astype(str).tolist()
        selected_doc_id = st.selectbox("Filtrar itens por documento", options=options, index=0)

    st.subheader("Itens dos documentos")
    filtro_doc = None if selected_doc_id in [None, "Todos"] else selected_doc_id
    df_itens = get_document_items(filtro_doc)
    if df_itens.empty:
        st.info("Nenhum item de documento encontrado.")
    else:
        df_itens["data"] = pd.to_datetime(df_itens["data"], errors="coerce")
        st.dataframe(
            df_itens.sort_values(["document_id", "data", "id"], ascending=[False, False, False]),
            width="stretch",
            hide_index=True,
        )

    st.subheader("Transações (legado/completo)")
    df_historico = get_all_transactions()
    if df_historico.empty:
        st.info("Nenhum registro encontrado.")
        return
    df_historico["data"] = pd.to_datetime(df_historico["data"], errors="coerce")
    st.dataframe(df_historico.sort_values("data", ascending=False), width="stretch", hide_index=True)


def render_income_entry():
    st.title("💸 Entrada de Receitas")

    st.caption("Registre entradas manualmente. Isso vai direto para o banco como transação do tipo 'entrada'.")

    hoje = datetime.date.today()
    categorias = ["Salário", "Freelance", "Reembolso", "Investimentos", "Venda", "Outros"]

    with st.form("form_receita", clear_on_submit=True):
        col1, col2, col3 = st.columns([1, 1, 2])

        with col1:
            data = st.date_input("Data", value=hoje)

        with col2:
            valor = st.number_input("Valor (R$)", min_value=0.0, step=10.0, format="%.2f")

        with col3:
            descricao = st.text_input("Descrição", placeholder="Ex: Salário empresa X / Pix recebido / Reembolso")

        col4, col5 = st.columns([1, 1])
        with col4:
            categoria = st.selectbox("Categoria", categorias, index=0)
        with col5:
            fonte = st.text_input("Fonte (opcional)", placeholder="Ex: Manual / Banco X / Cliente Y")

        submitted = st.form_submit_button("💾 Salvar Receita", type="primary")

    if submitted:
        if not descricao or not descricao.strip():
            st.error("Descrição é obrigatória.")
            return
        if valor <= 0:
            st.error("Valor precisa ser maior que zero.")
            return

        df = pd.DataFrame(
            [
                {
                    "data": pd.Timestamp(data).strftime("%Y-%m-%d"),
                    "valor": float(valor),
                    "descricao": descricao.strip(),
                    "categoria": categoria,
                    "fonte": (fonte.strip() if fonte and fonte.strip() else "Manual"),
                    "tipo": "entrada",
                }
            ]
        )

        try:
            insert_transactions(df)
            st.success("✅ Receita salva com sucesso!")
            st.toast("Receita registrada.", icon="✅")
        except Exception as exc:
            st.error(f"Falha ao salvar receita: {exc}")

    st.divider()
    st.subheader("📥 Entrada rápida (múltiplas receitas)")

    if "receitas_buffer" not in st.session_state:
        st.session_state.receitas_buffer = pd.DataFrame(
            columns=["data", "valor", "descricao", "categoria", "fonte", "tipo"]
        )

    df_edit = st.data_editor(
        st.session_state.receitas_buffer,
        num_rows="dynamic",
        width="stretch",
        column_config={
            "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY", required=True),
            "valor": st.column_config.NumberColumn("Valor (R$)", format="%.2f", required=True),
            "descricao": st.column_config.TextColumn("Descrição", width="large", required=True),
            "categoria": st.column_config.SelectboxColumn("Categoria", options=categorias, required=True),
            "fonte": st.column_config.TextColumn("Fonte", width="medium"),
            "tipo": st.column_config.TextColumn("Tipo", disabled=True),
        },
    )

    if not df_edit.empty:
        df_edit["tipo"] = "entrada"
        df_edit["fonte"] = df_edit["fonte"].fillna("Manual").astype(str)
        st.session_state.receitas_buffer = df_edit

    c1, c2, _ = st.columns([1, 1, 2])
    with c1:
        if st.button("💾 Salvar Lote", disabled=st.session_state.receitas_buffer.empty):
            df_final = st.session_state.receitas_buffer.copy()

            df_final["descricao"] = df_final["descricao"].astype(str).str.strip()
            df_final = df_final[df_final["descricao"].ne("")]

            df_final["data"] = pd.to_datetime(df_final["data"], errors="coerce")
            df_final["valor"] = pd.to_numeric(df_final["valor"], errors="coerce")

            df_final = df_final.dropna(subset=["data", "valor"])
            df_final = df_final[df_final["valor"] > 0]

            if df_final.empty:
                st.error("Nada válido para salvar (verifique data/valor/descrição).")
                return

            df_final["data"] = df_final["data"].dt.strftime("%Y-%m-%d")
            df_final["tipo"] = "entrada"
            df_final["categoria"] = df_final["categoria"].fillna("Outros")

            try:
                insert_transactions(df_final)
                st.success(f"✅ Lote salvo: {len(df_final)} receita(s).")
                st.toast("Receitas registradas.", icon="✅")
                st.session_state.receitas_buffer = st.session_state.receitas_buffer.iloc[0:0]
            except Exception as exc:
                st.error(f"Falha ao salvar lote: {exc}")

    with c2:
        if st.button("🗑️ Limpar Lote"):
            st.session_state.receitas_buffer = st.session_state.receitas_buffer.iloc[0:0]
            st.toast("Lote limpo.", icon="🗑️")


with st.sidebar:
    st.title("🚀 Extrator Pro")
    aba = st.radio("Navegação", ["Importar", "Receitas", "Pipeline", "Revisão", "Finalização", "Histórico"])

if aba == "Importar":
    render_import_store()
elif aba == "Receitas":
    render_income_entry()
elif aba == "Pipeline":
    render_pipeline()
elif aba == "Revisão":
    render_review()
elif aba == "Finalização":
    render_finalize()
else:
    render_history_section()
