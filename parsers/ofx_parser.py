from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class StatementLine:
    data: Optional[str]
    descricao: str
    valor: float
    merchant: Optional[str] = None
    parcela_atual: Optional[int] = None
    parcela_total: Optional[int] = None


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def _norm_desc(text: str) -> str:
    normalized = re.sub(r"\s+", " ", (text or "").strip())
    return normalized[:400]


def _parse_ofx_date(raw: str) -> Optional[str]:
    if not raw:
        return None
    match = re.match(r"^(\d{8})", raw.strip())
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _extract_installments(desc: str) -> tuple[Optional[int], Optional[int]]:
    value = (desc or "").lower()

    for pattern in [
        r"\b(\d{1,2})\s*/\s*(\d{1,2})\b",
        r"\bparc(?:ela)?\s*(\d{1,2})\s*/\s*(\d{1,2})\b",
        r"\bparc(?:ela)?\s*(\d{1,2})\s*de\s*(\d{1,2})\b",
    ]:
        match = re.search(pattern, value)
        if match:
            return int(match.group(1)), int(match.group(2))

    return None, None


def parse_ofx_bytes(ofx_bytes: bytes) -> List[StatementLine]:
    text = ofx_bytes.decode("latin-1", errors="ignore")

    try:
        import io

        from ofxparse import OfxParser  # type: ignore

        ofx = OfxParser.parse(io.BytesIO(ofx_bytes))
        lines: List[StatementLine] = []

        for tx in ofx.account.statement.transactions:
            data = tx.date.strftime("%Y-%m-%d") if tx.date else None
            descricao = _norm_desc((tx.memo or tx.payee or tx.name or "").strip()) or "Não identificado"
            valor = float(tx.amount) if tx.amount is not None else 0.0
            p_atual, p_total = _extract_installments(descricao)
            merchant = _norm_desc(tx.payee or tx.name or "") or None

            lines.append(
                StatementLine(
                    data=data,
                    descricao=descricao,
                    valor=valor,
                    merchant=merchant,
                    parcela_atual=p_atual,
                    parcela_total=p_total,
                )
            )

        return lines
    except Exception:
        pass

    blocks = re.findall(r"<STMTTRN>(.*?)</STMTTRN>", text, flags=re.DOTALL | re.IGNORECASE)
    lines: List[StatementLine] = []

    def tag(block: str, name: str) -> Optional[str]:
        match = re.search(rf"<{name}>\s*([^\r\n<]+)", block, flags=re.IGNORECASE)
        return match.group(1).strip() if match else None

    for block in blocks:
        dtposted = tag(block, "DTPOSTED")
        trnamt = tag(block, "TRNAMT")
        memo = tag(block, "MEMO") or tag(block, "NAME") or tag(block, "PAYEE")

        data = _parse_ofx_date(dtposted or "")
        descricao = _norm_desc(memo or "") or "Não identificado"

        try:
            valor = float((trnamt or "0").replace(",", "."))
        except ValueError:
            valor = 0.0

        p_atual, p_total = _extract_installments(descricao)
        lines.append(
            StatementLine(
                data=data,
                descricao=descricao,
                valor=valor,
                merchant=None,
                parcela_atual=p_atual,
                parcela_total=p_total,
            )
        )

    return lines


def build_hash_linha(competencia: str, line: StatementLine) -> str:
    base = f"{competencia}|{line.data or ''}|{_norm_desc(line.descricao)}|{line.valor:.2f}"
    return _sha256(base)
