import pdfplumber
import io

def extrair_texto_pdf(arquivo_pdf):
    """
    Extrai texto de PDFs nativos e identifica PDFs scaneados.
    
    Args:
        arquivo_pdf: Objeto do arquivo (BytesIO do Streamlit)
        
    Returns:
        tuple: (texto_extraido, is_scanned, erro)
    """
    texto_total = ""
    is_scanned = False
    erro = None
    
    try:
        # Abrimos o PDF usando o buffer de memória do Streamlit
        with pdfplumber.open(arquivo_pdf) as pdf:
            # Percorrer todas as páginas (Critério: Tratamento de Multi-páginas)
            for pagina in pdf.pages:
                texto_pagina = pagina.extract_text()
                if texto_pagina:
                    texto_total += texto_pagina + "\n"
        
        # Identificador de Tipo de PDF (Critério: Identificar se é imagem/scaneado)
        # Se extrairmos pouco ou nenhum texto, provavelmente é uma imagem
        if len(texto_total.strip()) < 50:
            is_scanned = True
            
    except Exception as e:
        # Tratamento de Exceções (Critério: PDFs protegidos ou corrompidos)
        if "password" in str(e).lower():
            erro = "Este PDF está protegido por senha e não pode ser lido."
        else:
            erro = f"Erro ao processar PDF: {str(e)}"
            
    return texto_total, is_scanned, erro