# ============================================================
# FILE: app.py
# ============================================================
import pandas as pd
import streamlit as st
from localDB import init_db, insert_transactions, get_all_transactions
from planilhas import processar_planilha

# Inicializa o banco de dados na primeira execução
init_db()

# Configurações da página
st.set_page_config(page_title="Extrator Pro MVP", page_icon="💰", layout="wide")

def render_upload_section():
    """Interface de upload e processamento de arquivos."""
    st.title("📂 Processamento de Documentos")
    st.write("Suba suas planilhas para extração automática e salvamento no banco.")
    
    arquivos = st.file_uploader(
        "Selecione arquivos (.xlsx, .csv)",
        type=["xlsx", "csv"], 
        accept_multiple_files=True
    )
    
    if arquivos:
        dfs = []
        for arq in arquivos:
            df, erro = processar_planilha(arq)
            if not erro: 
                dfs.append(df)
            else: 
                st.error(f"Erro em {arq.name}: {erro}")

        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)
            st.subheader("📋 Preview dos Dados")
            st.info("Revise os dados abaixo antes de confirmar o salvamento.")
            
            # ATUALIZAÇÃO: Alterado de use_container_width=True para width="stretch"
            df_editado = st.data_editor(
                df_final, 
                width="stretch", 
                num_rows="dynamic"
            )
            
            if st.button("💾 Confirmar e Salvar no Banco"):
                with st.spinner("Processando registros..."):
                    df_salvar = df_editado.dropna(subset=['data', 'valor', 'descricao']).copy()
                    
                    if not df_salvar.empty:
                        try:
                            df_salvar['data'] = pd.to_datetime(df_salvar['data']).dt.strftime('%Y-%m-%d')
                            novos = insert_transactions(df_salvar)
                            
                            if novos > 0:
                                st.success(f"Sucesso! {novos} novos registros foram adicionados.")
                                st.balloons()
                            else:
                                st.warning("Nenhum dado novo detectado (registros duplicados ignorados).")
                            
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao salvar: {e}")
                    else:
                        st.warning("Não há dados válidos para salvar.")

def render_history_section():
    """Interface de Histórico (Task MN2512-7)."""
    st.title("📜 Histórico de Transações")
    st.write("Visualize e valide todos os registros já processados no sistema.")
    
    df_historico = get_all_transactions()
    
    if df_historico.empty:
        st.info("💡 Nenhum registro encontrado. Vá até a aba **Início** para subir seus arquivos!")
        return

    # Tratamento de dados
    df_historico['data'] = pd.to_datetime(df_historico['data'])
    df_historico = df_historico.sort_values(by='data', ascending=False)

    # ATUALIZAÇÃO: Alterado de use_container_width=True para width="stretch"
    st.dataframe(
        df_historico,
        width="stretch",
        hide_index=True,
        column_order=("data", "descricao", "valor", "fonte", "categoria"),
        column_config={
            "data": st.column_config.DateColumn(
                "Data da Transação",
                format="DD/MM/YYYY"
            ),
            "valor": st.column_config.NumberColumn(
                "Valor",
                format="R$ %.2f"
            ),
            "descricao": "Descrição",
            "fonte": "Origem do Arquivo",
            "categoria": "Categoria"
        }
    )

    st.divider()
    col1, col2 = st.columns(2)
    total = df_historico['valor'].sum()
    col1.metric("Volume Total Processado", f"R$ {total:,.2f}")
    col2.metric("Total de Registros", len(df_historico))

# --- NAVEGAÇÃO LATERAL ---
with st.sidebar:
    st.title("🚀 Extrator Pro")
    st.markdown("---")
    aba_selecionada = st.radio(
        "Navegação Principal", 
        ["Início", "Histórico"],
        index=0
    )
    st.markdown("---")
    st.caption("Especialista: Tot 🤖")

# --- LÓGICA DE RENDERIZAÇÃO ---
if aba_selecionada == "Início":
    render_upload_section()
elif aba_selecionada == "Histórico":
    render_history_section()