# ============================================================
# FILE: app.py
# ============================================================
import pandas as pd
import streamlit as st

from datetime import datetime
from localDB import init_db, insert_transactions, get_all_transactions
from planilhas import processar_planilha

init_db()

st.set_page_config(page_title="Extrator Pro MVP", page_icon="💰", layout="wide")

def render_upload_section():
    st.title("📂 Processamento de Documentos")
    st.write("Suba suas planilhas para extração automática.")
    
    arquivos = st.file_uploader(
        "Selecione arquivos (.xlsx, .csv)",
        type=["xlsx", "csv"], 
        accept_multiple_files=True
    )
    
    if arquivos:
        dfs = []
        for arq in arquivos:
            df, erro = processar_planilha(arq)
            if not erro: dfs.append(df)
            else: st.error(f"Erro em {arq.name}: {erro}")

        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)
            st.subheader("📋 Preview dos Dados")
            df_editado = st.data_editor(df_final, use_container_width=True, num_rows="dynamic")
            
            if st.button("💾 Confirmar e Salvar no Banco"):
                with st.spinner("Analisando duplicatas e salvando..."):
                    # Sanitização
                    df_salvar = df_editado.dropna(subset=['data', 'valor', 'descricao']).copy()
                    
                    if not df_salvar.empty:
                        try:
                            # Normalização de data para o SQLite
                            df_salvar['data'] = pd.to_datetime(df_salvar['data']).dt.strftime('%Y-%m-%d')
                            
                            # Execução da persistência inteligente
                            novos = insert_transactions(df_salvar)
                            
                            if novos > 0:
                                st.success(f"Excelente! {novos} novos registros foram adicionados.")
                                st.balloons()
                            else:
                                st.warning("Nenhum dado novo. Todos os registros já existiam no banco.")
                            
                            st.rerun()
                        except Exception as e:
                            st.error(f"Falha na persistência: {e}")
                    else:
                        st.warning("Não há dados válidos para salvar.")
    
    
# --- 3. SEÇÃO DE HISTÓRICO ---
def render_history_section():
    st.title("📜 Histórico de Transações")
    st.write("Aqui estão todos os dados armazenados no seu banco de dados local.")
    
    # Busca dados do SQLite
    df_historico = get_all_transactions()
    
    if not df_historico.empty:
        # Métricas rápidas para dar um ar profissional ao MVP
        total_gasto = df_historico['valor'].sum()
        qtd_transacoes = len(df_historico)
        
        col1, col2 = st.columns(2)
        col1.metric("Total Acumulado", f"R$ {total_gasto:,.2f}")
        col2.metric("Nº de Registros", qtd_transacoes)
        
        st.markdown("---")
        # Exibe a tabela do banco
        st.dataframe(df_historico, use_container_width=True)
        
        # Botão para baixar o que está no banco (opcional, mas útil)
        csv = df_historico.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Baixar Tudo em CSV", csv, "historico_financeiro.csv", "text/csv")
    else:
        st.warning("O banco de dados ainda está vazio. Vá para a aba 'Início' e faça um upload!")

# --- 4. NAVEGAÇÃO ---
with st.sidebar:
    st.title("🚀 Extrator Pro v1.0")
    st.markdown("---")
    aba_selecionada = st.radio("Navegação", ["Início", "Histórico", "Configurações"])
    st.markdown("---")
    st.caption("Desenvolvido com Tot 🤖")

# --- LÓGICA DE NAVEGAÇÃO ---
if aba_selecionada == "Início":
    render_upload_section()

elif aba_selecionada == "Histórico":
    render_history_section()

elif aba_selecionada == "Configurações":
    st.title("⚙️ Configurações")
    st.write("Configurações do Banco de Dados:")
    st.code(f"DB Path: ./dados_financeiros.db")