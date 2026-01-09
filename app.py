# ============================================================
# FILE: app.py
# ============================================================
import pandas as pd
import streamlit as st
from localDB import init_db, insert_transactions, get_all_transactions
from planilhas import processar_planilha

# Inicializa o banco de dados na primeira execução
init_db()

# Configurações da página para uma interface profissional
st.set_page_config(page_title="Extrator Pro MVP", page_icon="💰", layout="wide")

def render_upload_section():
    """Interface de upload e processamento de arquivos (Atualizada MN2512-16)."""
    st.title("📂 Processamento de Documentos")
    st.write("Suba seus arquivos para extração automática. O sistema agora suporta documentos e imagens!")
    
    # MN2512-16: Atualização do filtro de extensões e label informativa
    arquivos = st.file_uploader(
        "Formatos aceitos: XLSX, CSV, PDF, PNG, JPG, JPEG",
        type=["xlsx", "csv", "pdf", "png", "jpg", "jpeg"], 
        accept_multiple_files=True,
        help="Arraste e solte seus comprovantes, faturas ou planilhas aqui."
    )
    
    if arquivos:
        dfs = []
        for arq in arquivos:
            # Identificação da extensão para lógica futura
            extensao = arq.name.split('.')[-1].lower()
            
            if extensao in ['xlsx', 'csv']:
                df, erro = processar_planilha(arq)
                if not erro: 
                    dfs.append(df)
                else: 
                    st.error(f"Erro em {arq.name}: {erro}")
            else:
                # Placeholder para as próximas tarefas da Milestone 2 (PDF e OCR)
                st.warning(f"O arquivo '{arq.name}' foi aceito, mas o motor de extração para {extensao.upper()} será implementado nas próximas etapas (MN2512-14/MN2512-8).")

        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)
            st.subheader("📋 Preview dos Dados")
            st.info("Revise os dados extraídos das planilhas abaixo.")
            
            # Mantendo a correção de largura (width="stretch") para evitar warnings
            df_editado = st.data_editor(
                df_final, 
                width="stretch", 
                num_rows="dynamic"
            )
            
            if st.button("💾 Confirmar e Salvar no Banco"):
                with st.spinner("Persistindo dados no SQLite..."):
                    df_salvar = df_editado.dropna(subset=['data', 'valor', 'descricao']).copy()
                    
                    if not df_salvar.empty:
                        try:
                            df_salvar['data'] = pd.to_datetime(df_salvar['data']).dt.strftime('%Y-%m-%d')
                            novos = insert_transactions(df_salvar)
                            
                            if novos > 0:
                                st.success(f"Sucesso! {novos} novos registros foram adicionados.")
                                st.balloons()
                            else:
                                st.warning("Nenhum dado novo detectado.")
                            
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao salvar: {e}")
                    else:
                        st.warning("Não há dados válidos para salvar.")

def render_history_section():
    """Interface de Histórico."""
    st.title("📜 Histórico de Transações")
    st.write("Visualize e valide todos os registros já processados no sistema.")
    
    df_historico = get_all_transactions()
    
    if df_historico.empty:
        st.info("💡 Nenhum registro encontrado. Vá até a aba **Início** para subir seus arquivos!")
        return

    df_historico['data'] = pd.to_datetime(df_historico['data'])
    df_historico = df_historico.sort_values(by='data', ascending=False)

    st.dataframe(
        df_historico,
        width="stretch",
        hide_index=True,
        column_order=("data", "descricao", "valor", "fonte", "categoria"),
        column_config={
            "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
            "valor": st.column_config.NumberColumn("Valor", format="R$ %.2f"),
        }
    )

# --- NAVEGAÇÃO LATERAL ---
with st.sidebar:
    st.title("🚀 Extrator Pro")
    st.markdown("---")
    aba_selecionada = st.radio("Navegação", ["Início", "Histórico"])
    st.markdown("---")
    st.caption("Tot Assistente: Milestone 2 em curso 🧠")

# --- LÓGICA DE RENDERIZAÇÃO ---
if aba_selecionada == "Início":
    render_upload_section()
elif aba_selecionada == "Histórico":
    render_history_section()