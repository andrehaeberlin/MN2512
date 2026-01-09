# ============================================================
# FILE: app.py
# ============================================================
import pandas as pd
import streamlit as st
from localDB import init_db, insert_transactions, get_all_transactions
from planilhas import processar_planilha
from pdfs import extrair_texto_pdf

# Inicializa o banco de dados na primeira execução
init_db()

# Configurações da página
st.set_page_config(page_title="Extrator Pro MVP", page_icon="💰", layout="wide")

def render_upload_section():
    """Interface de upload e processamento de arquivos."""
    st.title("📂 Processamento de Documentos")
    st.write("O sistema agora aceita Planilhas, PDFs e Imagens para extração.")
    
    arquivos = st.file_uploader(
        "Formatos aceitos: XLSX, CSV, PDF, PNG, JPG, JPEG",
        type=["xlsx", "csv", "pdf", "png", "jpg", "jpeg"], 
        accept_multiple_files=True
    )
    
    if arquivos:
        dfs = []
        for arq in arquivos:
            extensao = arq.name.split('.')[-1].lower()
            
            # --- FLUXO PLANILHAS ---
            if extensao in ['xlsx', 'csv']:
                df, erro = processar_planilha(arq)
                if not erro: 
                    dfs.append(df)
                else: 
                    st.error(f"Erro em {arq.name}: {erro}")
            
            # --- FLUXO PDF (MN2512-14) ---
            elif extensao == 'pdf':
                with st.spinner(f"Lendo PDF: {arq.name}..."):
                    texto, is_scanned, erro = extrair_texto_pdf(arq)
                    
                    if erro:
                        st.error(f"Erro em {arq.name}: {erro}")
                    elif is_scanned:
                        # Critério: Disparar fluxo de OCR futuramente
                        st.warning(f"'{arq.name}' parece ser um PDF scaneado (imagem). O motor de OCR (MN2512-15) será necessário para ler este arquivo.")
                    else:
                        st.success(f"Texto extraído com sucesso de {arq.name}!")
                        # Placeholder para MN2512-9 (Regex)
                        with st.expander(f"Ver texto bruto extraído - {arq.name}"):
                            st.text(texto)
                            st.info("Próxima etapa: O motor de Regex (MN2512-9) irá converter este texto em uma tabela.")

        # Exibição de resultados das planilhas (como já tínhamos)
        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)
            st.subheader("📋 Preview dos Dados de Planilhas")
            df_editado = st.data_editor(df_final, width="stretch", num_rows="dynamic")
            
            if st.button("💾 Confirmar e Salvar Planilhas"):
                # Lógica de salvamento...
                with st.spinner("Salvando..."):
                    df_salvar = df_editado.dropna(subset=['data', 'valor', 'descricao']).copy()
                    if not df_salvar.empty:
                        df_salvar['data'] = pd.to_datetime(df_salvar['data']).dt.strftime('%Y-%m-%d')
                        insert_transactions(df_salvar)
                        st.success("Dados salvos!")
                        st.rerun()

def render_history_section():
    """Interface de Histórico."""
    st.title("📜 Histórico de Transações")
    df_historico = get_all_transactions()
    
    if df_historico.empty:
        st.info("💡 Nenhum registro encontrado.")
        return

    df_historico['data'] = pd.to_datetime(df_historico['data'])
    df_historico = df_historico.sort_values(by='data', ascending=False)

    st.dataframe(
        df_historico,
        width="stretch",
        hide_index=True,
        column_config={
            "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
            "valor": st.column_config.NumberColumn("Valor", format="R$ %.2f"),
        }
    )

# --- NAVEGAÇÃO LATERAL ---
with st.sidebar:
    st.title("🚀 Extrator Pro")
    aba_selecionada = st.radio("Navegação", ["Início", "Histórico"])
    st.caption("Fase: Milestone 2 - Cérebro 🧠")

# --- LÓGICA DE RENDERIZAÇÃO ---
if aba_selecionada == "Início":
    render_upload_section()
elif aba_selecionada == "Histórico":
    render_history_section()