import pandas as pd
import streamlit as st
from localDB import init_db, insert_transactions, get_all_transactions
from planilhas import processar_planilha
from extrator_regex import extrair_dados_financeiros
from pdfs import extrair_texto_pdf, converter_pdf_para_imagens
from ocr import extrair_texto_imagem
import datetime

# Inicializa o banco de dados
init_db()

# Configurações da página
st.set_page_config(page_title="Extrator Pro MVP", page_icon="💰", layout="wide")

# --- INICIALIZAÇÃO DO ESTADO ---
if 'dados_para_revisar' not in st.session_state:
    st.session_state.dados_para_revisar = pd.DataFrame(columns=['data', 'valor', 'descricao'])

def limpar_buffer():
    """Limpa os dados da área de preview e recarrega a página."""
    st.session_state.dados_para_revisar = pd.DataFrame(columns=['data', 'valor', 'descricao'])
    st.rerun()

def render_upload_section():
    st.title("📂 Processamento de Documentos")
    
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
                            # ADICIONADO: Garante colunas de integridade
                            df_plan['fonte'] = arq.name

                            if 'categoria' not in df_plan.columns:
                                df_plan['categoria'] = 'Outros'
                            
                            # Selecionamos TODAS as colunas necessárias
                            novos_dados.append(df_plan[['data', 'valor', 'descricao', 'fonte', 'categoria']])
                
                # 2. Processamento de PDFs e Imagens (OCR + Regex)
                else:
                    with st.spinner(f"Extraindo dados de {arq.name}..."):
                        texto_total = ""
                        if extensao == 'pdf':
                            texto_pdf, is_scanned, erro_pdf = extrair_texto_pdf(arq)
                            if erro_pdf:
                                st.error(erro_pdf)
                                continue
                            if is_scanned:
                                imagens, erro_imagens = converter_pdf_para_imagens(arq)
                                if erro_imagens:
                                    st.error(erro_imagens)
                                    continue
                                for img_buffer in imagens:
                                    t, _, erro_ocr = extrair_texto_imagem(img_buffer)
                                    if erro_ocr:
                                        st.error(erro_ocr)
                                        continue
                                    texto_total += t + "\n"
                            else:
                                texto_total = texto_pdf
                        else:
                            texto_total, _, erro_ocr = extrair_texto_imagem(arq)
                            if erro_ocr:
                                st.error(erro_ocr)
                                continue
                        
                        # NOVA LÓGICA: Recebe uma lista de dicionários
                        dados_extraidos = extrair_dados_financeiros(texto_total)
                        if not dados_extraidos:
                            st.warning(f"Nenhum dado financeiro identificado em {arq.name}.")
                            continue

                        if isinstance(dados_extraidos, dict):
                            dados_extraidos = [dados_extraidos]

                        df_extraido = pd.DataFrame(dados_extraidos)

                        # ADICIONADO: Metadados
                        df_extraido['fonte'] = arq.name
                        df_extraido['categoria'] = 'Não categorizado'
                        novos_dados.append(df_extraido)
            # Consolidação dos dados
            if novos_dados:
                df_acumulado = pd.concat(novos_dados, ignore_index=True)
                
                # Conversão robusta de tipos
                df_acumulado['data'] = pd.to_datetime(df_acumulado['data'], errors='coerce')
                df_acumulado['valor'] = pd.to_numeric(df_acumulado['valor'], errors='coerce')
                
                st.session_state.dados_para_revisar = pd.concat(
                    [st.session_state.dados_para_revisar, df_acumulado], 
                    ignore_index=True
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
            width=None, # Stretch automático
            num_rows="dynamic",
            column_config={
                "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY", required=True),
                "valor": st.column_config.NumberColumn("Valor (R$)", format="%.2f", required=True),
                "descricao": st.column_config.TextColumn("Descrição", width="large", required=True),
                # ADICIONADO: Selectbox para facilitar classificação
                "categoria": st.column_config.SelectboxColumn(
                    "Categoria",
                    options=["Alimentação", "Transporte", "Serviços", "Outros"],
                    required=True
                ),
                # ADICIONADO: Fonte apenas leitura para auditoria
                "fonte": st.column_config.TextColumn("Fonte", disabled=True, width="small"),
            }
        )

        col1, col2, _ = st.columns([1, 1, 2])
        
        with col1:
            if st.button("💾 Confirmar e Salvar", type="primary"):
                with st.spinner("Validando dados..."):
                    # 1. Limpeza Inicial
                    df_final = df_editado.dropna(subset=['descricao']).copy()
                    
                    if df_final.empty:
                        st.warning("⚠️ Nenhuma transação válida (com descrição) para salvar.")
                    else:
                        # 2. Conversão e Normalização
                        df_final['data'] = pd.to_datetime(df_final['data'], errors='coerce')
                        df_final['valor'] = pd.to_numeric(df_final['valor'], errors='coerce').fillna(0.0)

                        # 3. Validações de Regra de Negócio
                        erros_impeditivos = []
                        avisos = []

                        # Regra: Valores Negativos
                        if (df_final['valor'] < 0).any():
                            erros_impeditivos.append("❌ Existem valores negativos. Corrija para prosseguir.")

                        # Regra: Datas no Futuro
                        if (df_final['data'] > pd.Timestamp.now()).any():
                            avisos.append("⚠️ Atenção: Existem datas futuras nos registros.")

                        # Regra: Datas Vazias
                        n_sem_data = df_final['data'].isna().sum()
                        if n_sem_data > 0:
                            avisos.append(f"ℹ️ {n_sem_data} transação(ões) sem data receberão a data de hoje.")
                            df_final['data'] = df_final['data'].fillna(pd.Timestamp.now())

                        # Decisão de Salvamento
                        if erros_impeditivos:
                            for err in erros_impeditivos:
                                st.error(err)
                        else:
                            for warn in avisos:
                                st.toast(warn, icon="⚠️")
                            
                            # Formatação Final para SQLite
                            df_final['data'] = df_final['data'].dt.strftime('%Y-%m-%d')
                            
                            insert_transactions(df_final)
                            st.success("✅ Dados persistidos com sucesso!")
                            limpar_buffer()
        
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
