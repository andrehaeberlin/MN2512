import pandas as pd
import streamlit as st
from localDB import init_db, insert_transactions, get_all_transactions
from planilhas import processar_planilha
from extrator_regex import extrair_dados_financeiros
from pdfs import extrair_texto_pdf, converter_pdf_para_imagens
from ocr import extrair_texto_imagem

# Inicializa o banco de dados na primeira execução
init_db()

# Configurações da página
st.set_page_config(page_title="Extrator Pro MVP", page_icon="💰", layout="wide")

def render_upload_section():
    """Interface de upload e processamento de arquivos."""
    st.title("📂 Processamento de Documentos")
    st.write("O sistema aceita Planilhas, PDFs e Imagens para extração automática.")
    
    arquivos = st.file_uploader(
        "Formatos aceitos: XLSX, CSV, PDF, PNG, JPG, JPEG",
        type=["xlsx", "csv", "pdf", "png", "jpg", "jpeg"], 
        accept_multiple_files=True
    )
    
    if arquivos:
        dfs = []
        for arq in arquivos:
            extensao = arq.name.split('.')[-1].lower()
            texto_total = ""
            
            # --- FLUXO PLANILHAS ---
            if extensao in ['xlsx', 'csv']:
                df_plan, erro = processar_planilha(arq)
                if not erro: 
                    # Garante colunas mínimas para o merge
                    df_plan = df_plan[['data', 'valor', 'descricao']]
                    dfs.append(df_plan)
                else: 
                    st.error(f"Erro em {arq.name}: {erro}")
            
            # --- FLUXO PDF (Nativo ou Scaneado) ---
            elif extensao == 'pdf':
                with st.spinner(f"Processando PDF: {arq.name}..."):
                    texto_pdf, is_scanned, erro = extrair_texto_pdf(arq)
                    
                    if is_scanned:
                        st.warning(f"'{arq.name}' é um PDF scaneado. Iniciando OCR por página...")
                        for img_buffer in converter_pdf_para_imagens(arq):
                            t, _, _ = extrair_texto_imagem(img_buffer)
                            texto_total += t + "\n"
                    else:
                        texto_total = texto_pdf
                    
                    dados = extrair_dados_financeiros(texto_total)
                    
                    # Debug para ajudar a ajustar o Regex
                    with st.expander(f"🔍 Debug PDF: {arq.name}"):
                        c1, c2 = st.columns(2)
                        c1.text_area("Texto Extraído", texto_total, height=200)
                        c2.json(dados)
                    
                    dfs.append(pd.DataFrame([dados]))

            # --- FLUXO IMAGENS ---
            elif extensao in ['png', 'jpg', 'jpeg']:
                with st.spinner(f"Processando imagem: {arq.name}..."):
                    texto_img, _, erro = extrair_texto_imagem(arq)
                    if not erro:
                        dados = extrair_dados_financeiros(texto_img)
                        
                        # Debug para ajudar a ajustar o Regex
                        with st.expander(f"🔍 Debug Imagem: {arq.name}"):
                            c1, c2 = st.columns(2)
                            c1.text_area("Texto do OCR", texto_img, height=200)
                            c2.json(dados)
                            
                        dfs.append(pd.DataFrame([dados]))
                    else:
                        st.error(f"Erro no OCR de {arq.name}: {erro}")
        
        # Consolidação e Exibição
        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)
            
            # Garantir que a coluna data seja interpretada corretamente para o editor
            if 'data' in df_final.columns:
                df_final['data'] = pd.to_datetime(df_final['data'], errors='coerce')

            st.subheader("📋 Revisão dos Dados Extraídos")
            st.info("💡 Verifique os campos abaixo. Você pode editar os valores diretamente na tabela.")
            
            df_editado = st.data_editor(
                df_final, 
                width="stretch", 
                num_rows="dynamic",
                column_config={
                    "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                    "valor": st.column_config.NumberColumn("Valor (R$)", format="%.2f"),
                }
            )
            
            if st.button("💾 Confirmar e Salvar no Banco de Dados"):
                with st.spinner("Salvando transações..."):
                    # Limpa linhas totalmente vazias ou sem descrição
                    df_salvar = df_editado.dropna(subset=['descricao']).copy()
                    
                    if not df_salvar.empty:
                        # Converte data para string YYYY-MM-DD para o SQLite
                        df_salvar['data'] = df_salvar['data'].dt.strftime('%Y-%m-%d')
                        # Garante que valores nulos em 'valor' virem 0.0
                        df_salvar['valor'] = df_salvar['valor'].fillna(0.0)
                        
                        insert_transactions(df_salvar)
                        st.success(f"Sucesso! {len(df_salvar)} registros salvos.")
                        st.balloons()
                        st.rerun()
                    else:
                        st.warning("Nenhum dado válido para salvar.")

def render_history_section():
    """Interface de Histórico."""
    st.title("📜 Histórico de Transações")
    df_historico = get_all_transactions()
    
    if df_historico.empty:
        st.info("💡 Nenhum registro encontrado no banco de dados.")
        return

    # Tratamento para exibição
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
    st.divider()
    st.caption("Fase: Milestone 2 - Cérebro 🧠")
    st.info("O sistema utiliza OCR e Regex para identificar padrões financeiros automaticamente.")

# --- LÓGICA DE RENDERIZAÇÃO ---
if aba_selecionada == "Início":
    render_upload_section()
elif aba_selecionada == "Histórico":
    render_history_section()