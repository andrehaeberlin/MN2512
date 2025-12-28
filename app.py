# ============================================================
# FILE: app.py
# ============================================================
import pandas as pd
import streamlit as st
from planilhas import processar_planilha

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Extrator Pro MVP", page_icon="💰", layout="wide")

def render_upload_section():
    st.title("📂 Processamento de Documentos")
    st.write("Suba suas planilhas para extração automática.")
    
    arquivos = st.file_uploader(
        "Selecione seus arquivos (.xlsx ou .csv)", 
        type=["xlsx", "csv"], 
        accept_multiple_files=True
    )
    
    if arquivos:
        dfs_processados = []
        erros = []
        
        # Processando cada arquivo com o motor do planilhas.py
        for arquivo in arquivos:
            df_resultado, erro = processar_planilha(arquivo)
            if erro:
                erros.append(erro)
            else:
                dfs_processados.append(df_resultado)
        
        # Exibe erros se houver
        for erro in erros:
            st.error(erro)
            
        if dfs_processados:
            # Consolida os dados
            df_final = pd.concat(dfs_processados, ignore_index=True)
            
            st.subheader("📋 Dados Normalizados (Preview)")
            st.info("💡 Você pode editar os valores na tabela antes de confirmar.")
            
            # Editor de dados - Crucial para o usuário validar o OCR/Planilha
            df_editado = st.data_editor(df_final, use_container_width=True, num_rows="dynamic")
            
            if st.button("💾 Confirmar e Salvar no Banco"):
                # Aqui entrará a lógica da Task MN2512-6
                st.balloons()
                st.success(f"Sucesso! {len(df_editado)} linhas prontas para o SQLite.")
                # st.session_state['dados_finais'] = df_editado # Dica: guardar para persistência

# --- SIDEBAR ---
with st.sidebar:
    st.title("🚀 Extrator Pro v1.0")
    st.markdown("---")
    aba_selecionada = st.radio("Navegação", ["Início", "Histórico", "Configurações"])

# --- LÓGICA DE NAVEGAÇÃO ---
if aba_selecionada == "Início":
    render_upload_section() # Chamando a função correta aqui!

elif aba_selecionada == "Histórico":
    st.title("📜 Histórico de Transações")
    st.info("Integração com SQLite pendente (Task MN2512-6).")

elif aba_selecionada == "Configurações":
    st.title("⚙️ Configurações")