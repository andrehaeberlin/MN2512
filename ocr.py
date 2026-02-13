import time
from functools import lru_cache
from typing import Iterable, Sequence

import cv2
import easyocr
import numpy as np
from PIL import Image, ImageOps


@lru_cache(maxsize=4)
def obter_leitor_ocr(idiomas: Sequence[str] = ("pt", "en"), gpu: bool = False):
    """Inicializa e mantém em cache o leitor do EasyOCR."""
    return easyocr.Reader(list(idiomas), gpu=gpu)


def _resize_max(img_np: np.ndarray, max_w: int = 1800) -> np.ndarray:
    h, w = img_np.shape[:2]
    if w <= max_w:
        return img_np

    scale = max_w / float(w)
    nh = int(h * scale)
    return cv2.resize(img_np, (max_w, nh), interpolation=cv2.INTER_AREA)


def preprocessar_imagem_ocr(img_np: np.ndarray) -> np.ndarray:
    """Aplica pré-processamento amigável para OCR de recibos/notas."""
    img_gray = cv2.cvtColor(img_np, cv2.COLOR_RGB2GRAY)
    img_denoised = cv2.fastNlMeansDenoising(img_gray, h=15)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    img_eq = clahe.apply(img_denoised)
    img_thresh = cv2.adaptiveThreshold(
        img_eq,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31,
        2,
    )
    return img_thresh


def _join_with_conf(resultados: Iterable, min_conf: float = 0.35) -> str:
    partes = []
    for item in resultados:
        if not isinstance(item, (list, tuple)) or len(item) < 3:
            continue

        texto = str(item[1]).strip()
        conf = float(item[2]) if item[2] is not None else 0.0
        if texto and conf >= min_conf:
            partes.append(texto)

    return " ".join(partes).strip()


def extrair_texto_imagem(arquivo_imagem, idiomas: Sequence[str] = ("pt", "en"), gpu: bool = False):
    """Processa imagem e extrai texto usando EasyOCR com fallback inteligente."""
    try:
        inicio = time.time()

        img_pil = Image.open(arquivo_imagem)
        img_pil = ImageOps.exif_transpose(img_pil)
        img_pil = img_pil.convert("RGB")

        img_np = np.array(img_pil)
        img_np = _resize_max(img_np, max_w=1800)

        reader = obter_leitor_ocr(tuple(idiomas), gpu=gpu)

        resultados = reader.readtext(img_np, detail=1)
        texto_total = _join_with_conf(resultados, min_conf=0.35)

        digits = sum(c.isdigit() for c in texto_total)
        if (not texto_total) or (len(texto_total) < 40) or (digits < 6):
            img_preprocessada = preprocessar_imagem_ocr(img_np)
            resultados_pre = reader.readtext(img_preprocessada, detail=1)
            texto_pre = _join_with_conf(resultados_pre, min_conf=0.30)
            if len(texto_pre) > len(texto_total):
                texto_total = texto_pre

        tempo_total = time.time() - inicio

        nome_arquivo = getattr(arquivo_imagem, "name", "BytesIO")
        print(f"[OCR] Arquivo: {nome_arquivo} | Tempo: {tempo_total:.2f}s | Len: {len(texto_total)}")

        return texto_total, tempo_total, None

    except Exception as exc:
        return "", 0, f"Erro no motor de OCR: {str(exc)}"
