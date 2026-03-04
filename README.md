# MN2512
MVP Planejador Financeiro

## Fila assíncrona com RQ + Redis

Para tirar OCR/LLM da UI do Streamlit, o app agora enfileira documentos no Redis e um worker separado processa a pipeline.

### Dependências

Instale os pacotes do projeto (incluindo `rq` e `redis`):

```bash
pip install -r requirements.txt
```

### Execução local

Em terminais separados:

```bash
redis-server
python worker.py
streamlit run app.py
```

Opcionalmente configure a URL do Redis:

```bash
export REDIS_URL=redis://localhost:6379/0
```
