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


def is_blank_or_low_density(image: np.ndarray, threshold: float = 0.02) -> bool:
    """Detecta páginas com baixa densidade de tinta (potencialmente irrelevantes)."""
    if image.size == 0:
        return True

    if len(image.shape) == 2:
        gray = image
    else:
        gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)

    _, binary = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY_INV)
    ink_pixels = int(np.sum(binary == 255))
    total_pixels = int(binary.size) or 1
    density = ink_pixels / total_pixels
    return density < threshold


def crop_roi(image: np.ndarray, top_ratio: float = 0.1, bottom_ratio: float = 0.1, side_ratio: float = 0.05) -> np.ndarray:
    """Recorta margens superior/inferior/laterais para focar na região útil."""
    h, w = image.shape[:2]
    if h == 0 or w == 0:
        return image

    top = int(h * top_ratio)
    bottom = int(h * (1 - bottom_ratio))
    left = int(w * side_ratio)
    right = int(w * (1 - side_ratio))

    if top >= bottom or left >= right:
        return image
    return image[top:bottom, left:right]


def normalize_scale(image: np.ndarray, target_width: int = 1600) -> np.ndarray:
    """Padroniza largura máxima para reduzir custo mantendo legibilidade."""
    h, w = image.shape[:2]
    if w <= target_width or w == 0 or h == 0:
        return image

    scale_ratio = target_width / float(w)
    new_height = max(1, int(h * scale_ratio))
    return cv2.resize(image, (target_width, new_height), interpolation=cv2.INTER_AREA)


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

        if is_blank_or_low_density(img_np):
            tempo_total = time.time() - inicio
            nome_arquivo = getattr(arquivo_imagem, "name", "BytesIO")
            print(f"[OCR] Arquivo: {nome_arquivo} | Tempo: {tempo_total:.2f}s | Página ignorada por baixa densidade")
            return "", tempo_total, None

        img_np = crop_roi(img_np)
        img_np = normalize_scale(img_np, target_width=1600)
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
