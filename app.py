import base64
import datetime
import io

import pandas as pd
import streamlit as st
from extrator_regex import extrair_dados_financeiros
from llm_extractor import categorizar_transacoes_llm, extrair_dados_financeiros_llm
from localDB import (
    find_transaction_id,
    get_all_transactions,
    init_db,
    insert_document,
    insert_statement,
    insert_statement_lines,
    insert_transactions,
    link_entities,
)
from ocr import extrair_texto_imagem
from parsers.ofx_parser import StatementLine, parse_ofx_bytes
from pdfs import converter_pdf_para_imagens, extrair_texto_pdf
from planilhas import processar_planilha

# Inicializa o banco de dados
init_db()

# Configurações da página
st.set_page_config(page_title="Extrator Pro MVP", page_icon="💰", layout="wide")

# --- INICIALIZAÇÃO DO ESTADO ---
if "dados_para_revisar" not in st.session_state:
    st.session_state.dados_para_revisar = pd.DataFrame(columns=["data", "valor", "descricao", "tipo"])
if "ofx_preview_df" not in st.session_state:
    st.session_state.ofx_preview_df = pd.DataFrame()
if "ofx_doc_id" not in st.session_state:
    st.session_state.ofx_doc_id = None


def limpar_buffer():
    """Limpa os dados da área de preview e recarrega a página."""
    st.session_state.dados_para_revisar = pd.DataFrame(columns=["data", "valor", "descricao", "tipo"])
    st.rerun()


def render_preview(arq):
    """Renderiza preview de imagens, PDFs e planilhas antes do processamento."""
    extensao = arq.name.split(".")[-1].lower()

    if extensao in ["png", "jpg", "jpeg"]:
        st.image(arq, use_container_width=True)
        return

    if extensao == "pdf":
        bytes_pdf = arq.getvalue()
        b64 = base64.b64encode(bytes_pdf).decode("utf-8")
        st.components.v1.html(
            f"""
            <iframe
                src="data:application/pdf;base64,{b64}"
                width="100%"
                height="650"
                style="border: none;"
            ></iframe>
            """,
            height=650,
        )
        return

    if extensao in ["csv", "xlsx"]:
        try:
            if extensao == "csv":
                df_preview = pd.read_csv(io.BytesIO(arq.getvalue()))
            else:
                df_preview = pd.read_excel(io.BytesIO(arq.getvalue()))
            st.dataframe(df_preview.head(50), use_container_width=True)
        except Exception as exc:
            st.warning(f"Não foi possível renderizar preview tabular: {exc}")
        return

    st.info("Formato sem preview incorporado.")
    st.download_button("Baixar arquivo", data=arq.getvalue(), file_name=arq.name)


def render_upload_section():
    st.title("📂 Processamento de Documentos")

    arquivos = st.file_uploader(
        "Arraste planilhas, PDFs ou imagens aqui",
        type=["xlsx", "csv", "pdf", "png", "jpg", "jpeg"],
        accept_multiple_files=True,
    )

    if arquivos:
        st.subheader("👁️ Pré-visualização dos arquivos")
        for arq in arquivos:
            with st.expander(arq.name):
                render_preview(arq)

        if st.button("🚀 Iniciar Processamento"):
            novos_dados = []

            for arq in arquivos:
                extensao = arq.name.split(".")[-1].lower()
                document_id = None

                with st.spinner(f"Registrando documento {arq.name}..."):
                    file_bytes = arq.getvalue()
                    mime = arq.type or "application/octet-stream"
                    document_id = insert_document(arq.name, mime, file_bytes)

                # 1. Processamento de Planilhas
                if extensao in ["xlsx", "csv"]:
                    with st.spinner(f"Processando planilha {arq.name}..."):
                        df_plan, erro = processar_planilha(arq)
                        if not erro:
                            df_plan["fonte"] = arq.name

                            if "categoria" not in df_plan.columns:
                                df_plan["categoria"] = "Outros"

                            df_plan["document_id"] = document_id

                            novos_dados.append(df_plan[["data", "valor", "descricao", "fonte", "categoria", "tipo", "document_id"]])

                # 2. Processamento de PDFs e Imagens (OCR + Regex)
                else:
                    with st.spinner(f"Extraindo dados de {arq.name}..."):
                        texto_total = ""
                        if extensao == "pdf":
                            texto_pdf, is_scanned, erro_pdf = extrair_texto_pdf(arq)
                            if erro_pdf:
                                st.error(erro_pdf)
                                continue
                            if is_scanned:
                                imagens, erro_imagens = converter_pdf_para_imagens(arq)
                                if erro_imagens:
                                    st.error(erro_imagens)
                                    continue
                                with st.spinner(f"Executando OCR em {arq.name}..."):
                                    for img_buffer in imagens:
                                        t, _, erro_ocr = extrair_texto_imagem(img_buffer)
                                        if erro_ocr:
                                            st.error(erro_ocr)
                                            continue
                                        texto_total += t + "\n"
                            else:
                                texto_total = texto_pdf
                        else:
                            with st.spinner(f"Executando OCR em {arq.name}..."):
                                texto_total, _, erro_ocr = extrair_texto_imagem(arq)
                            if erro_ocr:
                                st.error(erro_ocr)
                                continue

                        dados_extraidos = extrair_dados_financeiros(texto_total)
                        if not dados_extraidos:
                            with st.spinner("Fallback LLM em execução..."):
                                dados_extraidos, erro_llm = extrair_dados_financeiros_llm(texto_total)
                            if erro_llm:
                                st.warning(f"{erro_llm} ({arq.name})")
                                continue
                            if not dados_extraidos:
                                st.warning(f"Nenhum dado financeiro identificado em {arq.name}.")
                                continue

                        if isinstance(dados_extraidos, dict):
                            dados_extraidos = [dados_extraidos]

                        dados_extraidos, erro_cat = categorizar_transacoes_llm(dados_extraidos)
                        if erro_cat:
                            st.info(f"Categorização automática indisponível: {erro_cat} ({arq.name})")

                        df_extraido = pd.DataFrame(dados_extraidos)

                        df_extraido["fonte"] = arq.name
                        df_extraido["document_id"] = document_id
                        if "tipo" not in df_extraido.columns:
                            df_extraido["tipo"] = "saida"
                        if "categoria" not in df_extraido.columns:
                            df_extraido["categoria"] = "Outros"
                        df_extraido["categoria"] = df_extraido["categoria"].fillna("Outros")
                        novos_dados.append(df_extraido)

            if novos_dados:
                df_acumulado = pd.concat(novos_dados, ignore_index=True)
                df_acumulado["data"] = pd.to_datetime(df_acumulado["data"], errors="coerce")
                df_acumulado["valor"] = pd.to_numeric(df_acumulado["valor"], errors="coerce")

                st.session_state.dados_para_revisar = pd.concat(
                    [st.session_state.dados_para_revisar, df_acumulado], ignore_index=True
                )
                st.success(f"{len(df_acumulado)} item(ns) adicionado(s) para revisão!")
            else:
                st.warning("Nenhum dado financeiro foi identificado nos arquivos.")

    if not st.session_state.dados_para_revisar.empty:
        st.divider()
        st.subheader("📋 Preview de Conferência (Validação)")
        st.info("Verifique os dados abaixo. Linhas com erros impedirão o salvamento.")

        df_editado = st.data_editor(
            st.session_state.dados_para_revisar,
            width="stretch",
            num_rows="dynamic",
            column_config={
                "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY", required=True),
                "valor": st.column_config.NumberColumn("Valor (R$)", format="%.2f", required=True),
                "descricao": st.column_config.TextColumn("Descrição", width="large", required=True),
                "categoria": st.column_config.SelectboxColumn(
                    "Categoria", options=["Alimentação", "Transporte", "Serviços", "Outros"], required=True
                ),
                "fonte": st.column_config.TextColumn("Fonte", disabled=True, width="small"),
                "document_id": st.column_config.NumberColumn("Doc ID", disabled=True, width="small"),
                "tipo": st.column_config.SelectboxColumn("Tipo", options=["saida", "entrada"], required=True),
            },
        )

        col1, col2, _ = st.columns([1, 1, 2])

        with col1:
            if st.button("💾 Confirmar e Salvar", type="primary"):
                with st.spinner("Validando dados..."):
                    df_final = df_editado.dropna(subset=["descricao"]).copy()

                    if df_final.empty:
                        st.warning("⚠️ Nenhuma transação válida (com descrição) para salvar.")
                    else:
                        df_final["data"] = pd.to_datetime(df_final["data"], errors="coerce")
                        df_final["valor"] = pd.to_numeric(df_final["valor"], errors="coerce").fillna(0.0)

                        erros_impeditivos = []
                        avisos = []
                        df_final["valor"] = df_final["valor"].abs()
                        if "tipo" not in df_final.columns:
                            df_final["tipo"] = "saida"
                        df_final["tipo"] = df_final["tipo"].astype(str).str.lower().str.strip()
                        df_final.loc[~df_final["tipo"].isin(["entrada", "saida"]), "tipo"] = "saida"

                        datas_futuras = df_final["data"].dt.date > datetime.date.today()
                        if datas_futuras.fillna(False).any():
                            avisos.append("⚠️ Atenção: Existem datas futuras nos registros.")

                        n_sem_data = df_final["data"].isna().sum()
                        if n_sem_data > 0:
                            avisos.append(f"ℹ️ {n_sem_data} transação(ões) sem data receberão a data de hoje.")
                            df_final["data"] = df_final["data"].fillna(pd.Timestamp(datetime.date.today()))

                        if erros_impeditivos:
                            for err in erros_impeditivos:
                                st.error(err)
                        else:
                            for warn in avisos:
                                st.toast(warn, icon="⚠️")

                            df_final["data"] = df_final["data"].dt.strftime("%Y-%m-%d")
                            insert_transactions(df_final)

                            for _, row in df_final.iterrows():
                                doc_id = row.get("document_id")
                                if pd.notna(doc_id):
                                    tx_id = find_transaction_id(
                                        row["data"],
                                        row["descricao"],
                                        float(row["valor"]),
                                        row["fonte"],
                                        row.get("tipo", "saida"),
                                    )
                                    if tx_id:
                                        link_entities("transaction", tx_id, "document", int(doc_id))

                            st.success("✅ Dados persistidos com sucesso!")
                            st.toast("Dados atualizados no banco com sucesso!", icon="✅")
                            limpar_buffer()

        with col2:
            if st.button("🗑️ Descartar Tudo"):
                limpar_buffer()


def render_ofx_import():
    st.title("💳 Importar Fatura via OFX")

    arq = st.file_uploader("Envie um arquivo OFX", type=["ofx"], key="ofx_uploader")
    if not arq:
        return

    ofx_bytes = arq.getvalue()
    doc_id = insert_document(arq.name, arq.type or "application/x-ofx", ofx_bytes)

    competencia_default = pd.Timestamp.today().strftime("%Y-%m")
    competencia = st.text_input("Competência (YYYY-MM)", value=competencia_default, key="ofx_competencia")
    banco = st.text_input("Banco (opcional)", value="", key="ofx_banco")
    cartao = st.text_input("Cartão (opcional)", value="", key="ofx_cartao")

    if st.button("⚙️ Processar OFX", key="processar_ofx"):
        lines = parse_ofx_bytes(ofx_bytes)
        preview = pd.DataFrame(
            [
                {
                    "data": line.data,
                    "descricao": line.descricao,
                    "valor": line.valor,
                    "parcela_atual": line.parcela_atual,
                    "parcela_total": line.parcela_total,
                    "merchant": line.merchant,
                }
                for line in lines
            ]
        )
        st.session_state.ofx_preview_df = preview
        st.session_state.ofx_doc_id = doc_id

    if not st.session_state.ofx_preview_df.empty:
        st.subheader("📋 Linhas extraídas (revise antes de salvar)")
        df_edit = st.data_editor(
            st.session_state.ofx_preview_df,
            num_rows="dynamic",
            use_container_width=True,
            key="editor_ofx",
        )

        if st.button("💾 Salvar fatura e linhas", key="salvar_ofx"):
            statement_id = insert_statement(
                st.session_state.ofx_doc_id,
                banco or None,
                cartao or None,
                competencia,
            )

            lines = []
            for _, row in df_edit.iterrows():
                data_value = None
                if pd.notna(row.get("data")):
                    parsed_date = pd.to_datetime(row.get("data"), errors="coerce")
                    data_value = parsed_date.strftime("%Y-%m-%d") if pd.notna(parsed_date) else None

                lines.append(
                    StatementLine(
                        data=data_value,
                        descricao=str(row.get("descricao") or "Não identificado"),
                        valor=float(row.get("valor") or 0.0),
                        merchant=str(row.get("merchant")) if pd.notna(row.get("merchant")) else None,
                        parcela_atual=int(row.get("parcela_atual")) if pd.notna(row.get("parcela_atual")) else None,
                        parcela_total=int(row.get("parcela_total")) if pd.notna(row.get("parcela_total")) else None,
                    )
                )

            inserted = insert_statement_lines(statement_id, competencia, lines)
            st.success(f"✅ Salvo! {inserted} linha(s) nova(s) inserida(s).")


def render_history_section():
    st.title("📜 Histórico de Transações")
    df_historico = get_all_transactions()

    if not df_historico.empty:
        df_historico["data"] = pd.to_datetime(df_historico["data"])
        st.dataframe(
            df_historico.sort_values("data", ascending=False),
            width="stretch",
            hide_index=True,
            column_config={
                "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                "valor": st.column_config.NumberColumn("Valor", format="R$ %.2f"),
            },
        )
    else:
        st.info("Nenhum registro encontrado.")


def render_dashboard_section():
    st.title("📊 Financial Insights Dashboard")
    df_historico = get_all_transactions()

    if df_historico.empty:
        st.info("Nenhum registro encontrado para gerar insights.")
        return

    df_historico["data"] = pd.to_datetime(df_historico["data"], errors="coerce")
    df_historico["valor"] = pd.to_numeric(df_historico["valor"], errors="coerce")

    total_transacoes = len(df_historico)
    total_valor = df_historico["valor"].sum()
    valor_medio = df_historico["valor"].mean()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total de Transações", f"{total_transacoes}")
    col2.metric("Valor Total (R$)", f"{total_valor:,.2f}")
    col3.metric("Valor Médio (R$)", f"{valor_medio:,.2f}")

    st.subheader("Gastos por Categoria")
    por_categoria = df_historico.groupby("categoria", dropna=False)["valor"].sum().sort_values(ascending=False)
    st.bar_chart(por_categoria, use_container_width=True)

    st.subheader("Evolução Mensal")
    por_mes = (
        df_historico.dropna(subset=["data"])
        .assign(mes=lambda df: df["data"].dt.to_period("M").dt.to_timestamp())
        .groupby("mes")["valor"]
        .sum()
        .sort_index()
    )
    st.line_chart(por_mes, use_container_width=True)


# --- NAVEGAÇÃO ---
with st.sidebar:
    st.title("🚀 Extrator Pro")
    aba = st.radio("Navegação", ["Início", "Faturas", "Histórico", "Dashboard"])

if aba == "Início":
    render_upload_section()
elif aba == "Faturas":
    render_ofx_import()
elif aba == "Dashboard":
    render_dashboard_section()
else:
    render_history_section()
