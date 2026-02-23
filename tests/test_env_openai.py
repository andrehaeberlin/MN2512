import os
import streamlit as st

st.set_page_config(page_title="Teste OPENAI_API_KEY")

st.title("Diagnóstico de Variável de Ambiente")

api_key = os.getenv("OPENAI_API_KEY")

st.write("OPENAI_API_KEY exists:", bool(api_key))

if api_key:
    st.write("OPENAI_API_KEY prefix:", api_key[:7])
else:
    st.error("OPENAI_API_KEY não encontrada no ambiente.")