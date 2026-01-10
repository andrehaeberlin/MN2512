import pdfplumber
import io
import fitz  # PyMuPDF
from PIL import Image

def extrair_texto_pdf(arquivo_pdf):
    """
    Extrai texto de PDFs nativos e identifica PDFs scaneados.
    """
    texto_total = ""
    is_scanned = False
    erro = None
    
    try:
        # Resetar ponteiro para garantir leitura do início
        arquivo_pdf.seek(0)
        with pdfplumber.open(arquivo_pdf) as pdf:
            for pagina in pdf.pages:
                texto_pagina = pagina.extract_text()
                if texto_pagina:
                    texto_total += texto_pagina + "\n"
        
        # Identificador de PDF scaneado (MN2512-15 Fallback)
        if len(texto_total.strip()) < 50:
            is_scanned = True
            
    except Exception as e:
        if "password" in str(e).lower():
            erro = "Este PDF está protegido por senha."
        else:
            erro = f"Erro ao processar PDF: {str(e)}"
            
    return texto_total, is_scanned, erro

def converter_pdf_para_imagens(arquivo_pdf, dpi=300):
    """
    MN2512-15: Converte páginas de PDF em imagens para OCR.
    Garante 300 DPI para qualidade de extração.
    """
    arquivo_pdf.seek(0)
    # Abre o PDF a partir dos bytes
    pdf_documento = fitz.open(stream=arquivo_pdf.read(), filetype="pdf")
    
    for num_pagina in range(len(pdf_documento)):
        pagina = pdf_documento.load_page(num_pagina)
        
        # Cálculo de zoom para atingir o DPI desejado (72 é o padrão do PDF)
        zoom = dpi / 72
        matriz = fitz.Matrix(zoom, zoom)
        pixmap = pagina.get_pixmap(matrix=matriz)
        
        # Converte para PIL Image e depois para BytesIO (compatível com ocr.py)
        img_pil = Image.frombytes("RGB", [pixmap.width, pixmap.height], pixmap.samples)
        buffer = io.BytesIO()
        img_pil.save(buffer, format="PNG")
        buffer.seek(0)
        
        # Atribui um nome para o log no ocr.py
        buffer.name = f"pagina_{num_pagina + 1}.png"
        
        yield buffer # Uso de generator para Gestão de Memória
    
    pdf_documento.close()