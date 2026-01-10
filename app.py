# ============================================================
# FILE: app.py
# ============================================================
import pandas as pd
import streamlit as st
from localDB import init_db, insert_transactions, get_all_transactions
from planilhas import processar_planilha
from pdfs import extrair_texto_pdf, converter_pdf_para_imagens
from ocr import extrair_texto_imagem

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
            
            # --- FLUXO PDF (MN2512-14 e ) ---
            elif extensao == 'pdf':
                with st.spinner(f"Lendo PDF: {arq.name}..."):
                    texto, is_scanned, erro = extrair_texto_pdf(arq)
                    
                    if erro:
                        st.error(f"Erro em {arq.name}: {erro}")
                    elif is_scanned:
                        # --- INÍCIO DA TASK MN2512-15 ---
                        st.warning(f"'{arq.name}' é um PDF scaneado. Iniciando pipeline de OCR...")
                        
                        texto_ocr_total = ""
                        # Barra de progresso para múltiplas páginas
                        progresso = st.progress(0)
                        
                        # Converte e processa cada página individualmente para poupar RAM
                        imagens_paginas = list(converter_pdf_para_imagens(arq))
                        total_pags = len(imagens_paginas)

                        for i, img_buffer in enumerate(imagens_paginas):
                            with st.spinner(f"Processando página {i+1} de {total_pags}..."):
                                texto_pag, tempo, erro_ocr = extrair_texto_imagem(img_buffer)
                                if not erro_ocr:
                                    texto_ocr_total += texto_pag + "\n"
                            progresso.progress((i + 1) / total_pags)
                        
                        st.success(f"Extração OCR concluída!")
                        with st.expander(f"Ver texto extraído via OCR - {arq.name}"):
                            st.text(texto_ocr_total)
                            st.info("Próxima etapa: O motor de Regex (MN2512-9) irá converter este texto em uma tabela.")
                        # --- FIM DA TASK MN2512-15 ---
                    else:
                        st.success(f"Texto extraído com sucesso de {arq.name}!")
                        with st.expander(f"Ver texto bruto extraído - {arq.name}"):
                            st.text(texto)
            # --- FLUXO IMAGENS (MN2512-8) ---
            elif extensao in ['png', 'jpg', 'jpeg']:
                with st.spinner(f"Processando imagem: {arq.name}..."):
                    texto, tempo, erro = extrair_texto_imagem(arq)

                    if erro:
                        st.error(f"Erro em {arq.name}: {erro}")
                    else:
                        st.success(f"Texto extraído em {tempo:.2f}s!")
                        with st.expander(f"Ver texto bruto da imagem - {arq.name}"):
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