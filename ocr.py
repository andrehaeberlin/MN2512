# ============================================================
# FILE: ocr.py
# ============================================================
import easyocr
import cv2
import numpy as np
import time
from PIL import Image
import io

# Inicializa o leitor para Português. 
# O download do modelo ocorre apenas na primeira execução.
reader = easyocr.Reader(['pt'])

def preprocessar_imagem_ocr(img_np):
    """
    Aplica pré-processamento para melhorar a qualidade do OCR.
    """
    img_gray = cv2.cvtColor(img_np, cv2.COLOR_RGB2GRAY)
    img_denoised = cv2.fastNlMeansDenoising(img_gray, h=30)
    img_eq = cv2.equalizeHist(img_denoised)
    img_thresh = cv2.adaptiveThreshold(
        img_eq,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31,
        2,
    )
    return img_thresh

def extrair_texto_imagem(arquivo_imagem):
    """
    Processa uma imagem e extrai o texto utilizando EasyOCR.
    
    Args:
        arquivo_imagem: Objeto do arquivo (BytesIO do Streamlit)
        
    Returns:
        tuple: (texto_extraido, tempo_processamento, erro)
    """
    try:
        inicio = time.time()
        
        # 1. Converter buffer do Streamlit para imagem OpenCV
        # Primeiro lemos com PIL e depois convertemos para array numpy
        img_pil = Image.open(arquivo_imagem)
        img_np = np.array(img_pil)
        
        # 2. Otimização de Imagem (Critério: Pré-processamento)
        img_preprocessada = preprocessar_imagem_ocr(img_np)
        
        # 3. Execução do OCR (Critério: Suporte a Idiomas/Português)
        # detail=0 retorna apenas o texto bruto consolidado
        resultados = reader.readtext(img_preprocessada, detail=0)
        texto_total = " ".join(str(item) for item in resultados)
        
        fim = time.time()
        tempo_total = fim - inicio
        
        # 4. Logs de Execução (Critério: Monitorar performance)
        print(f" [OCR] Arquivo: {arquivo_imagem.name} | Tempo: {tempo_total:.2f}s")
        
        return texto_total, tempo_total, None

    except Exception as e:
        return "", 0, f"Erro no motor de OCR: {str(e)}"
