import base64
import datetime
import io
import json

import pandas as pd
import streamlit as st
from localDB import (
    STATUS_FINALIZE_PENDING,
    STATUS_HITL_REVIEW,
    STATUS_PROCESSING,
    STATUS_STORED,
    finalize_pending_documents,
    get_all_transactions,
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
    st.title("HITL_REVIEW")
    docs = list_ingest_documents([STATUS_HITL_REVIEW])
    if not docs:
        st.info("Nenhum documento aguardando revisão.")
        return

    labels = {d["id"]: f"{d['original_name']} ({d['id'][:8]})" for d in docs}
    selected = st.selectbox("Documento", options=list(labels.keys()), format_func=lambda x: labels[x])

    payload_info = get_latest_extraction_payload(selected)
    if not payload_info:
        st.warning("Sem payload de extração para este documento.")
        return

    payload_uri, payload, checks = payload_info
    st.caption(f"Payload: {payload_uri}")
    st.json(checks)

    edited_json = st.text_area(
        "Edite o payload candidato (JSON)",
        value=json.dumps(payload, ensure_ascii=False, indent=2),
        height=320,
    )
    reviewer = st.text_input("Reviewer", value="operador")
    decision = st.selectbox("Decisão", ["APPROVED", "CHANGES", "REJECTED"])
    notes = st.text_area("Notas")

    if st.button("Salvar revisão"):
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
