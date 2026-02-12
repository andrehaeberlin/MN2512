from parsers.ofx_parser import build_hash_linha, parse_ofx_bytes


def test_parse_ofx_fallback_blocks():
    content = b"""
<OFX>
<BANKMSGSRSV1>
<STMTTRNRS>
<STMTRS>
<BANKTRANLIST>
<STMTTRN>
<DTPOSTED>20260115
<TRNAMT>-123.45
<MEMO>COMPRA MERCADO 01/03</MEMO>
</STMTTRN>
<STMTTRN>
<DTPOSTED>20260116
<TRNAMT>89.90
<NAME>PAGAMENTO APP</NAME>
</STMTTRN>
</BANKTRANLIST>
</STMTRS>
</STMTTRNRS>
</BANKMSGSRSV1>
</OFX>
"""
    lines = parse_ofx_bytes(content)

    assert len(lines) == 2
    assert lines[0].data == "2026-01-15"
    assert lines[0].valor == -123.45
    assert lines[0].parcela_atual == 1
    assert lines[0].parcela_total == 3
    assert lines[1].descricao == "PAGAMENTO APP"


def test_hash_linha_consistente():
    line = parse_ofx_bytes(
        b"<STMTTRN><DTPOSTED>20260115<TRNAMT>10.00<MEMO>TESTE</MEMO></STMTTRN>"
    )[0]
    h1 = build_hash_linha("2026-01", line)
    h2 = build_hash_linha("2026-01", line)
    assert h1 == h2
