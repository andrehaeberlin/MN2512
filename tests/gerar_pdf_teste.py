# ============================================================
# UTILITY: gerar_pdf_teste.py
# Objetivo: Criar um PDF nativo com texto para testar o extrator.
# ============================================================
from fpdf import FPDF

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 15)
        self.cell(0, 10, 'EXTRATO BANCÁRIO FICTÍCIO - TESTE MVP', 0, 1, 'C')
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Página {self.page_no()}', 0, 0, 'C')

def criar_extrato_teste():
    pdf = PDF()
    pdf.add_page()
    pdf.set_font('Arial', '', 12)

    # Cabeçalho de "Tabela"
    pdf.set_fill_color(200, 220, 255)
    pdf.cell(40, 10, 'Data', 1, 0, 'C', 1)
    pdf.cell(100, 10, 'Descrição', 1, 0, 'C', 1)
    pdf.cell(40, 10, 'Valor (R$)', 1, 1, 'C', 1)

    # Dados de Exemplo (Formatos variados para testar o futuro Regex)
    dados = [
        ("10/01/2026", "Pagamento Fornecedor ABC", "1500,50"),
        ("12/01/2026", "Venda Cliente XPTO", "3200,00"),
        ("15-01-2026", "Assinatura Software Cloud", "99,90"),
        ("2026-01-20", "Reembolso Despesas", "450,25"),
        ("22/01/26", "Taxa de Manutenção", "15,00"),
    ]

    for data, desc, valor in dados:
        pdf.cell(40, 10, data, 1)
        pdf.cell(100, 10, desc, 1)
        pdf.cell(40, 10, valor, 1)

    # Adicionando uma segunda página para testar o critério "Multi-páginas"
    pdf.add_page()
    pdf.cell(0, 10, 'Continuação do Extrato - Página 2', 0, 1)
    pdf.ln(5)
    pdf.cell(40, 10, "25/01/2026", 1)
    pdf.cell(100, 10, "Compra Papelaria", 1)
    pdf.cell(40, 10, "120,00", 1)

    output_path = "extrato_teste.pdf"
    pdf.output(output_path)
    print(f"✅ PDF de teste gerado com sucesso: {output_path}")

if __name__ == "__main__":
    # Certifique-se de ter instalado: pip install fpdf2
    try:
        criar_extrato_teste()
    except ImportError:
        print("❌ Erro: A biblioteca 'fpdf2' não está instalada.")
        print("Execute: pip install fpdf2")