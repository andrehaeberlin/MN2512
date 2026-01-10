import pandas as pd
import streamlit as st
from localDB import init_db, insert_transactions, get_all_transactions
from planilhas import processar_planilha
from extrator_regex import extrair_dados_financeiros
from pdfs import extrair_texto_pdf, converter_pdf_para_imagens
from ocr import extrair_texto_imagem

# Inicializa o banco de dados
init_db()

# Configurações da página
st.set_page_config(page_title="Extrator Pro MVP", page_icon="💰", layout="wide")

# --- INICIALIZAÇÃO DO ESTADO (MN2512-10) ---
if 'dados_para_revisar' not in st.session_state:
    # Criamos um DataFrame vazio com as colunas padrão
    st.session_state.dados_para_revisar = pd.DataFrame(columns=['data', 'valor', 'descricao'])

def limpar_buffer():
    """Limpa os dados da área de preview."""
    st.session_state.dados_para_revisar = pd.DataFrame(columns=['data', 'valor', 'descricao'])
    st.rerun()

def render_upload_section():
    st.title("📂 Processamento de Documentos")
    
    # Upload de arquivos
    arquivos = st.file_uploader(
        "Arraste planilhas, PDFs ou imagens aqui",
        type=["xlsx", "csv", "pdf", "png", "jpg", "jpeg"], 
        accept_multiple_files=True
    )
    
    if arquivos:
        if st.button("🚀 Iniciar Processamento"):
            novos_dados = []
            
            for arq in arquivos:
                extensao = arq.name.split('.')[-1].lower()
                
                # 1. Processamento de Planilhas
                if extensao in ['xlsx', 'csv']:
                    with st.spinner(f"Processando planilha {arq.name}..."):
                        df_plan, erro = processar_planilha(arq)
                        if not erro:
                            novos_dados.append(df_plan[['data', 'valor', 'descricao']])
                
                # 2. Processamento de PDFs e Imagens (OCR + Regex)
                else:
                    with st.spinner(f"Extraindo dados de {arq.name}..."):
                        texto_total = ""
                        if extensao == 'pdf':
                            texto_pdf, is_scanned, _ = extrair_texto_pdf(arq)
                            if is_scanned:
                                for img_buffer in converter_pdf_para_imagens(arq):
                                    t, _, _ = extrair_texto_imagem(img_buffer)
                                    texto_total += t + "\n"
                            else:
                                texto_total = texto_pdf
                        else:
                            texto_total, _, _ = extrair_texto_imagem(arq)
                        
                        # Aplica inteligência de Regex (MN2512-9)
                        dados_extraidos = extrair_dados_financeiros(texto_total)
                        novos_dados.append(pd.DataFrame([dados_extraidos]))

            # Consolida os novos dados no Session State para revisão
            if novos_dados:
                df_acumulado = pd.concat(novos_dados, ignore_index=True)
                # Converte coluna data para formato datetime para o editor
                df_acumulado['data'] = pd.to_datetime(df_acumulado['data'], errors='coerce')
                
                # Adiciona ao que já existia no buffer (permitindo múltiplos uploads)
                st.session_state.dados_para_revisar = pd.concat(
                    [st.session_state.dados_para_revisar, df_acumulado], 
                    ignore_index=True
                )
                st.success(f"{len(df_acumulado)} item(ns) adicionado(s) para revisão!")

    # --- SEÇÃO DE PREVIEW E CONFERÊNCIA (MN2512-10) ---
    if not st.session_state.dados_para_revisar.empty:
        st.divider()
        st.subheader("📋 Preview de Conferência (Human-in-the-loop)")
        st.info("Ajuste os dados abaixo antes de salvar permanentemente no banco de dados.")

        # Componente principal: st.data_editor
        # num_rows="dynamic" permite ao usuário excluir linhas erradas ou adicionar novas
        df_editado = st.data_editor(
            st.session_state.dados_para_revisar,
            width="stretch",
            num_rows="dynamic",
            column_config={
                "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                "valor": st.column_config.NumberColumn("Valor (R$)", format="%.2f"),
                "descricao": st.column_config.TextColumn("Descrição/Estabelecimento", width="large"),
            }
        )

        col1, col2, _ = st.columns([1, 1, 2])
        
        with col1:
            if st.button("💾 Confirmar e Salvar", type="primary"):
                with st.spinner("Salvando..."):
                    # Validação: Remove linhas onde a descrição está vazia
                    df_final = df_editado.dropna(subset=['descricao']).copy()
                    
                    if not df_final.empty:
                        # Normaliza datas para string antes do SQLite
                        df_final['data'] = pd.to_datetime(df_final['data']).dt.strftime('%Y-%m-%d')
                        df_final['valor'] = df_final['valor'].fillna(0.0)
                        
                        insert_transactions(df_final)
                        st.success("✅ Dados persistidos com sucesso!")
                        limpar_buffer() # Limpa após sucesso
                    else:
                        st.warning("Não há dados válidos para salvar.")
        
        with col2:
            if st.button("🗑️ Descartar Tudo"):
                limpar_buffer()

def render_history_section():
    st.title("📜 Histórico de Transações")
    df_historico = get_all_transactions()
    
    if not df_historico.empty:
        df_historico['data'] = pd.to_datetime(df_historico['data'])
        st.dataframe(
            df_historico.sort_values('data', ascending=False),
            width="stretch",
            hide_index=True,
            column_config={
                "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                "valor": st.column_config.NumberColumn("Valor", format="R$ %.2f"),
            }
        )
    else:
        st.info("Nenhum registro encontrado.")

# --- NAVEGAÇÃO ---
with st.sidebar:
    st.title("🚀 Extrator Pro")
    aba = st.radio("Navegação", ["Início", "Histórico"])

if aba == "Início":
    render_upload_section()
else:
    render_history_section()