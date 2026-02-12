import base64
import datetime

import pandas as pd
import streamlit as st
from extrator_regex import extrair_dados_financeiros
from llm_extractor import extrair_dados_financeiros_llm, categorizar_transacoes_llm
from localDB import get_all_transactions, init_db, insert_transactions
from ocr import extrair_texto_imagem
from pdfs import converter_pdf_para_imagens, extrair_texto_pdf
from planilhas import processar_planilha

# Inicializa o banco de dados
init_db()

# Configurações da página
st.set_page_config(page_title="Extrator Pro MVP", page_icon="💰", layout="wide")

# --- INICIALIZAÇÃO DO ESTADO ---
if "dados_para_revisar" not in st.session_state:
    st.session_state.dados_para_revisar = pd.DataFrame(columns=["data", "valor", "descricao"])


def limpar_buffer():
    """Limpa os dados da área de preview e recarrega a página."""
    st.session_state.dados_para_revisar = pd.DataFrame(columns=["data", "valor", "descricao"])
    st.rerun()


def render_preview(arq):
    """Renderiza preview de imagens e PDFs antes do processamento."""
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

                # 1. Processamento de Planilhas
                if extensao in ["xlsx", "csv"]:
                    with st.spinner(f"Processando planilha {arq.name}..."):
                        df_plan, erro = processar_planilha(arq)
                        if not erro:
                            df_plan["fonte"] = arq.name

                            if "categoria" not in df_plan.columns:
                                df_plan["categoria"] = "Outros"

                            novos_dados.append(df_plan[["data", "valor", "descricao", "fonte", "categoria"]])

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
                        if "categoria" not in df_extraido.columns:
                            df_extraido["categoria"] = "Outros"
                        df_extraido["categoria"] = df_extraido["categoria"].fillna("Outros")
                        novos_dados.append(df_extraido)

            # Consolidação dos dados
            if novos_dados:
                df_acumulado = pd.concat(novos_dados, ignore_index=True)

                # Conversão robusta de tipos
                df_acumulado["data"] = pd.to_datetime(df_acumulado["data"], errors="coerce")
                df_acumulado["valor"] = pd.to_numeric(df_acumulado["valor"], errors="coerce")

                st.session_state.dados_para_revisar = pd.concat(
                    [st.session_state.dados_para_revisar, df_acumulado], ignore_index=True
                )
                st.success(f"{len(df_acumulado)} item(ns) adicionado(s) para revisão!")
            else:
                st.warning("Nenhum dado financeiro foi identificado nos arquivos.")

    # --- SEÇÃO DE PREVIEW E CONFERÊNCIA ---
    if not st.session_state.dados_para_revisar.empty:
        st.divider()
        st.subheader("📋 Preview de Conferência (Validação)")
        st.info("Verifique os dados abaixo. Linhas com erros impedirão o salvamento.")

        # Editor de dados
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
            },
        )

        col1, col2, _ = st.columns([1, 1, 2])

        with col1:
            if st.button("💾 Confirmar e Salvar", type="primary"):
                with st.spinner("Validando dados..."):
                    # 1. Limpeza Inicial
                    df_final = df_editado.dropna(subset=["descricao"]).copy()

                    if df_final.empty:
                        st.warning("⚠️ Nenhuma transação válida (com descrição) para salvar.")
                    else:
                        # 2. Conversão e Normalização
                        df_final["data"] = pd.to_datetime(df_final["data"], errors="coerce")
                        df_final["valor"] = pd.to_numeric(df_final["valor"], errors="coerce").fillna(0.0)

                        # 3. Validações de Regra de Negócio
                        erros_impeditivos = []
                        avisos = []

                        # Regra: Valores Negativos
                        if (df_final["valor"] < 0).any():
                            erros_impeditivos.append("❌ Existem valores negativos. Corrija para prosseguir.")

                        # Regra: Datas no Futuro (comparando apenas data calendário)
                        datas_futuras = df_final["data"].dt.date > datetime.date.today()
                        if datas_futuras.fillna(False).any():
                            avisos.append("⚠️ Atenção: Existem datas futuras nos registros.")

                        # Regra: Datas Vazias
                        n_sem_data = df_final["data"].isna().sum()
                        if n_sem_data > 0:
                            avisos.append(f"ℹ️ {n_sem_data} transação(ões) sem data receberão a data de hoje.")
                            df_final["data"] = df_final["data"].fillna(pd.Timestamp(datetime.date.today()))

                        # Decisão de Salvamento
                        if erros_impeditivos:
                            for err in erros_impeditivos:
                                st.error(err)
                        else:
                            for warn in avisos:
                                st.toast(warn, icon="⚠️")

                            # Formatação Final para SQLite
                            df_final["data"] = df_final["data"].dt.strftime("%Y-%m-%d")

                            insert_transactions(df_final)
                            st.success("✅ Dados persistidos com sucesso!")
                            st.toast("Dados atualizados no banco com sucesso!", icon="✅")
                            limpar_buffer()

        with col2:
            if st.button("🗑️ Descartar Tudo"):
                limpar_buffer()


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
    aba = st.radio("Navegação", ["Início", "Histórico", "Dashboard"])

if aba == "Início":
    render_upload_section()
elif aba == "Dashboard":
    render_dashboard_section()
else:
    render_history_section()
