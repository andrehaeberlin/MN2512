import io

from planilhas import processar_planilha


class UploadStub(io.BytesIO):
    def __init__(self, data: bytes, name: str):
        super().__init__(data)
        self.name = name

    def getvalue(self):
        pos = self.tell()
        self.seek(0)
        data = self.read()
        self.seek(pos)
        return data


def test_processar_planilha_csv_semicolon_and_decimal_br():
    content = "data;descricao;valor\n01/02/2026;Padaria;12,34\n"
    file = UploadStub(content.encode("utf-8"), "extrato.csv")

    df, err = processar_planilha(file)

    assert err is None
    assert float(df.iloc[0]["valor"]) == 12.34
    assert df.iloc[0]["tipo"] in ["saida", "entrada"]


def test_processar_planilha_debito_credito_builds_tipo():
    content = "data;historico;debito;credito\n01/02/2026;Compra;-100,00;\n02/02/2026;Estorno;;30,00\n"
    file = UploadStub(content.encode("utf-8"), "fatura.csv")

    df, err = processar_planilha(file)

    assert err is None
    assert list(df["tipo"]) == ["saida", "entrada"]
    assert list(df["valor"]) == [100.0, 30.0]
