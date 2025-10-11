from openai import OpenAI
from pdf2image import convert_from_path
import base64, json, pandas as pd

client = OpenAI(api_key="SUA_CHAVE_API_AQUI")

# Converte PDF em imagens
pages = convert_from_path(r"C:\Users\Victor\Downloads\NFe - Cleuber Marcos de Oliveira.pdf")

dados_extraidos = []

for i, page in enumerate(pages):
    # Converte imagem para base64
    page.save("page_temp.png", "PNG")
    with open("page_temp.png", "rb") as f:
        img_b64 = base64.b64encode(f.read()).decode("utf-8")

    # Envia pra API
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": """
                    Identifique o tipo de documento (nota fiscal, recebimento, ou outro).
                    Se for nota, extraia CNPJ, número da nota, data e valor.
                    Se for recebimento, extraia data de vencimento e número da nota relacionada.
                    Responda em JSON.
                    """},
                    {"type": "image_url", "image_url": f"data:image/png;base64,{img_b64}"}
                ]
            }
        ]
    )

    texto = response.choices[0].message.content
    try:
        dados = json.loads(texto)
        dados["pagina"] = i + 1
        dados_extraidos.append(dados)
    except:
        print(f"Erro ao interpretar JSON da página {i+1}")

# Monta DataFrame e exporta
df = pd.DataFrame(dados_extraidos)
df.to_excel("resultado_notas.xlsx", index=False)
