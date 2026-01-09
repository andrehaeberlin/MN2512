# ============================================================
# UTILITY: gerar_png_teste.py
# Objetivo: Criar uma imagem PNG com texto para testar o motor de OCR.
# ============================================================
from PIL import Image, ImageDraw, ImageFont

def criar_imagem_teste():
    """
    Gera uma imagem PNG simulando um recibo financeiro.
    Nota: Requer a biblioteca Pillow (pip install Pillow).
    """
    # Configurações da imagem (Fundo branco)
    largura, altura = 600, 400
    imagem = Image.new('RGB', (largura, altura), color=(255, 255, 255))
    draw = ImageDraw.Draw(imagem)

    # Tenta carregar uma fonte padrão. 
    # Em ambientes reais, poderíamos carregar um ficheiro .ttf específico.
    try:
        # Tenta usar a fonte padrão do Pillow
        fonte_titulo = ImageFont.load_default()
        fonte_corpo = ImageFont.load_default()
    except Exception:
        print("Aviso: Não foi possível carregar fontes customizadas. Usando padrão.")

    # Desenhar um "cabeçalho" de recibo
    draw.rectangle([20, 20, 580, 80], outline=(0, 0, 0), width=2)
    draw.text((200, 40), "RECIBO DE PAGAMENTO - TESTE OCR", fill=(0, 0, 0))

    # Conteúdo do Recibo (Dados para o motor de Regex validar depois)
    linhas_texto = [
        "-----------------------------------------",
        "DATA DA TRANSACAO: 12/01/2026",
        "DESCRICAO: Almoco de Negocios - Restaurante Tot",
        "CATEGORIA: Alimentacao",
        "VALOR TOTAL: R$ 85,90",
        "-----------------------------------------",
        "AUTENTICACAO: 987654321-ABC",
        "ID DO TERMINAL: TM-001"
    ]

    y_pos = 120
    for linha in linhas_texto:
        draw.text((50, y_pos), linha, fill=(0, 0, 0))
        y_pos += 30

    # Guardar o ficheiro
    output_path = "comprovativo_teste.png"
    imagem.save(output_path)
    print(f"✅ Imagem de teste gerada com sucesso: {output_path}")

if __name__ == "__main__":
    # Certifique-se de que tem a biblioteca instalada: pip install Pillow
    try:
        criar_imagem_teste()
    except ImportError:
        print("❌ Erro: A biblioteca 'Pillow' não está instalada.")
        print("Execute: pip install Pillow")
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado: {e}")