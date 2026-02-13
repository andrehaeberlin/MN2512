import csv
from io import StringIO

import pandas as pd


def _detectar_sep(csv_bytes: bytes) -> str:
    sample = csv_bytes[:4096].decode("utf-8", errors="ignore")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        return ";" if sample.count(";") > sample.count(",") else ","




def _to_numeric_br(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str)
        .str.replace("R$", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False),
        errors="coerce",
    )


def processar_planilha(uploaded_file):
    """Lê, mapeia e normaliza Excel/CSV para o schema interno da app."""
    try:
        nome = uploaded_file.name.lower()

        if nome.endswith(".csv"):
            raw = uploaded_file.getvalue()
            sep = _detectar_sep(raw)
            df = pd.read_csv(StringIO(raw.decode("utf-8", errors="ignore")), sep=sep, engine="python")
        else:
            df = pd.read_excel(uploaded_file)

        if df is None or df.empty:
            return None, f"O arquivo '{uploaded_file.name}' está vazio."

        df.columns = [str(c).strip().lower() for c in df.columns]

        mapeamento = {
            "data": ["data", "date", "dt", "vencimento", "dia", "período", "periodo", "dt. lançamento", "dt lancamento"],
            "descricao": [
                "descrição",
                "descricao",
                "item",
                "serviço",
                "servico",
                "histórico",
                "historico",
                "nome",
                "estabelecimento",
                "lançamento",
                "lancamento",
                "memo",
                "historico do lancamento",
            ],
            "valor": ["valor", "preço", "preco", "total", "amount", "pago", "valor pago", "vlr", "valor (r$)", "valor r$"],
        }

        sinonimos_debito = ["débito", "debito", "saída", "saida", "debit", "valor débito", "valor debito"]
        sinonimos_credito = ["crédito", "credito", "entrada", "credit", "valor crédito", "valor credito"]

        def achar_col(sinonimos):
            return next((s for s in sinonimos if s in df.columns), None)

        col_data = achar_col(mapeamento["data"])
        col_desc = achar_col(mapeamento["descricao"])
        col_val = achar_col(mapeamento["valor"])
        col_deb = achar_col(sinonimos_debito)
        col_cre = achar_col(sinonimos_credito)

        if not col_data:
            return None, f"Não encontramos a coluna de **data** no arquivo '{uploaded_file.name}'."
        if not col_desc:
            return None, f"Não encontramos a coluna de **descrição** no arquivo '{uploaded_file.name}'."

        out = pd.DataFrame()
        out["data"] = df[col_data]
        out["descricao"] = df[col_desc]

        if col_val:
            out["valor"] = df[col_val]
            out["tipo"] = None
        else:
            if not col_deb and not col_cre:
                return None, (
                    f"Não encontramos a coluna de **valor** (nem débito/crédito) no arquivo '{uploaded_file.name}'."
                )

            deb = _to_numeric_br(df[col_deb]) if col_deb else pd.Series(0, index=df.index, dtype=float)
            cre = _to_numeric_br(df[col_cre]) if col_cre else pd.Series(0, index=df.index, dtype=float)

            out["valor"] = (cre.fillna(0).abs() + deb.fillna(0).abs())
            has_deb = deb.fillna(0).abs() > 0
            has_cre = cre.fillna(0).abs() > 0
            out["tipo"] = "saida"
            out.loc[has_cre & ~has_deb, "tipo"] = "entrada"
            out.loc[has_cre & has_deb & (cre.fillna(0).abs() > deb.fillna(0).abs()), "tipo"] = "entrada"

        out["descricao"] = out["descricao"].astype(str).str.strip()
        out = out.dropna(subset=["data", "descricao"], how="all")

        out["data"] = pd.to_datetime(out["data"], errors="coerce", dayfirst=True)

        if out["valor"].dtype == object:
            out["valor"] = _to_numeric_br(out["valor"])
        out["valor"] = pd.to_numeric(out["valor"], errors="coerce")

        # Se veio valor com sinal, converte para modelo valor positivo + tipo
        signed_mask = out["valor"].notna()
        inferred = out.loc[signed_mask, "valor"]
        out.loc[signed_mask & (inferred < 0), "tipo"] = "saida"
        out.loc[signed_mask & (inferred > 0), "tipo"] = out.loc[signed_mask & (inferred > 0), "tipo"].fillna("entrada")
        out["valor"] = out["valor"].abs()

        out["fonte"] = uploaded_file.name
        out["categoria"] = "Outros"
        out["tipo"] = out["tipo"].fillna("saida")

        out = out.dropna(subset=["descricao"])
        out["valor"] = out["valor"].fillna(0.0)

        cols = ["data", "valor", "descricao", "fonte", "categoria", "tipo"]
        return out[cols], None

    except Exception as exc:
        return None, f"Erro crítico ao ler '{uploaded_file.name}': {str(exc)}"
