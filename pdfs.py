import io

import fitz  # PyMuPDF
import pdfplumber
from PIL import Image


def extrair_texto_pdf(arquivo_pdf, min_chars_por_pagina=30, min_paginas_com_texto=1):
    """
    Extrai texto de PDFs nativos e identifica PDFs escaneados.

    Retorna:
        (texto_total, is_scanned, erro)
    """
    texto_total = ""
    erro = None

    try:
        arquivo_pdf.seek(0)

        paginas_com_texto = 0
        paginas_com_imagens = 0

        with pdfplumber.open(arquivo_pdf) as pdf:
            total_paginas = len(pdf.pages)

            for pagina in pdf.pages:
                texto_pagina = (pagina.extract_text() or "").strip()

                if len(texto_pagina) >= min_chars_por_pagina:
                    paginas_com_texto += 1

                if texto_pagina:
                    texto_total += texto_pagina + "\n"

                try:
                    if getattr(pagina, "images", None) and len(pagina.images) > 0:
                        paginas_com_imagens += 1
                except Exception:
                    pass

        is_scanned = False
        if total_paginas > 0:
            if paginas_com_texto < min_paginas_com_texto and paginas_com_imagens > 0:
                is_scanned = True
            elif len(texto_total.strip()) < 80 and paginas_com_imagens > 0:
                is_scanned = True

    except Exception as exc:
        if "password" in str(exc).lower():
            erro = "Este PDF está protegido por senha."
        else:
            erro = f"Erro ao processar PDF: {str(exc)}"
        return "", False, erro

    return texto_total, is_scanned, None


def converter_pdf_para_imagens(arquivo_pdf, dpi=220, max_pages=None, grayscale=True, format="PNG"):
    """Converte páginas de PDF em imagens para OCR."""
    imagens = []
    erro = None

    try:
        arquivo_pdf.seek(0)
        pdf_bytes = arquivo_pdf.read()

        with fitz.open(stream=pdf_bytes, filetype="pdf") as pdf_documento:
            total_paginas = len(pdf_documento)
            limite = total_paginas if max_pages is None else min(total_paginas, int(max_pages))

            zoom = dpi / 72.0
            matriz = fitz.Matrix(zoom, zoom)

            for num_pagina in range(limite):
                pagina = pdf_documento.load_page(num_pagina)
                pixmap = pagina.get_pixmap(matrix=matriz, alpha=False)
                img_pil = Image.frombytes("RGB", [pixmap.width, pixmap.height], pixmap.samples)

                if grayscale:
                    img_pil = img_pil.convert("L")

                buffer = io.BytesIO()
                img_pil.save(buffer, format=format)
                buffer.seek(0)
                imagens.append(buffer)

    except Exception as exc:
        if "password" in str(exc).lower():
            erro = "Este PDF está protegido por senha."
        else:
            erro = f"Erro ao converter PDF em imagens: {str(exc)}"

    return imagens, erro
