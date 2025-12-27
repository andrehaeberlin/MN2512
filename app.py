import streamlit as st
import pandas as pd
import sqlite3

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Extrator Pro MVP", layout="wide")

def main():
    st.sidebar.title("🚀 Extrator de Dados v1.0")
    menu = st.sidebar.radio("Navegação", ["Upload", "Histórico"])

    if menu == "Upload":
        render_upload_section()
    else:
        render_history_section()

# --- LÓGICA DE NEGÓCIO: EXCEL ---
def render_upload_section():
    st.header("📤 Upload de Documentos")
    
    uploaded_file = st.file_uploader("Escolha uma planilha Excel", type=["xlsx"])
    
    if uploaded_file:
        df = pd.read_excel(uploaded_file)
        st.subheader("Preview dos Dados")
        # Usamos data_editor para permitir correções manuais rápidas
        edited_df = st.data_editor(df)
        
        if st.button("Salvar no Banco de Dados"):
            save_to_db(edited_df)
            st.success("Dados salvos com sucesso!")

# --- FUNÇÕES AUXILIARES (EM BREVE NO DATABASE.PY) ---
def save_to_db(df):
    conn = sqlite3.connect("financas.db")
    df.to_sql("transacoes", conn, if_exists="append", index=False)
    conn.close()

def render_history_section():
    st.header("📜 Histórico de Transações")
    # Lógica para ler o SQLite e exibir aqui
    st.info("Funcionalidade em desenvolvimento...")

if __name__ == "__main__":
    main()