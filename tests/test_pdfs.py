import io

import pdfs


class _FakePage:
    def __init__(self, text, images=None):
        self._text = text
        self.images = images or []

    def extract_text(self):
        return self._text


class _FakePlumberDoc:
    def __init__(self, pages):
        self.pages = pages

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakePixmap:
    def __init__(self, width=4, height=4):
        self.width = width
        self.height = height
        self.samples = b"\xff\x00\x00" * (width * height)


class _FakeFitzPage:
    def get_pixmap(self, matrix=None, alpha=False):
        return _FakePixmap()


class _FakeFitzDoc:
    def __init__(self, total_pages=3):
        self.total_pages = total_pages

    def __len__(self):
        return self.total_pages

    def load_page(self, idx):
        return _FakeFitzPage()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_extrair_texto_pdf_detects_scanned_with_images(monkeypatch):
    pages = [_FakePage("curto", images=[{"x0": 0}])]
    monkeypatch.setattr(pdfs.pdfplumber, "open", lambda _: _FakePlumberDoc(pages))

    text, scanned, err = pdfs.extrair_texto_pdf(io.BytesIO(b"fake"), min_chars_por_pagina=30)

    assert err is None
    assert scanned is True
    assert "curto" in text


def test_extrair_texto_pdf_detects_native_text(monkeypatch):
    pages = [_FakePage("texto suficientemente longo para considerar pagina com texto valido", images=[])]
    monkeypatch.setattr(pdfs.pdfplumber, "open", lambda _: _FakePlumberDoc(pages))

    text, scanned, err = pdfs.extrair_texto_pdf(io.BytesIO(b"fake"), min_chars_por_pagina=30)

    assert err is None
    assert scanned is False
    assert "texto suficientemente" in text


def test_converter_pdf_para_imagens_respects_max_pages(monkeypatch):
    monkeypatch.setattr(pdfs.fitz, "open", lambda stream, filetype: _FakeFitzDoc(total_pages=5))

    images, err = pdfs.converter_pdf_para_imagens(io.BytesIO(b"fake"), max_pages=2, dpi=200)

    assert err is None
    assert len(images) == 2
