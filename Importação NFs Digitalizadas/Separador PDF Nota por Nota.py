import os
import re
import shutil
from pdf2image import convert_from_path
import pytesseract
import concurrent.futures
from PyPDF2 import PdfReader, PdfWriter
import unicodedata

# ---------- OCR ----------
def process_page(image, page_num, lang="por"):
    try:
        text = pytesseract.image_to_string(image, lang=lang)
        print(f"Página {page_num} concluída.")
        return text or ""
    except Exception as e:
        print(f"Erro no OCR da página {page_num}: {e}")
        return ""

def ocr_pdf_pages(pdf_path, dpi=150, lang="por"):
    try:
        images = convert_from_path(pdf_path, dpi=dpi)
    except Exception as e:
        print(f"Erro ao converter '{pdf_path}' em imagens: {e}")
        return []

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_page, img, i, lang): i for i, img in enumerate(images, start=1)}
        results = [None] * len(images)
        for future in concurrent.futures.as_completed(futures):
            idx = futures[future] - 1
            results[idx] = future.result()
    return results

# ---------- Detecção / Segmentação ----------
def normalize_text(s: str) -> str:
    """
    Remove acentos, baixa caixa, remove pontuação e comprime espaços,
    para permitir matching mais flexível.
    """
    s = s or ""
    # remove acentos
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.category(ch).startswith("M"))
    # caixa baixa
    s = s.lower()
    # remove pontuação (mantém letras/números/espaço)
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    # troca underline por espaço e comprime múltiplos espaços
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def contains_ignored(text_norm: str, raw_text: str, ignore_keywords: list[str]) -> bool:
    """
    Retorna True se o texto contiver alguma palavra proibida.
    - Tolerante a acentos/pontuação/caixa via normalize_text.
    - Para termos numéricos (CNPJ/códigos), faz comparação por dígitos.
    """
    raw_digits = digits_only(raw_text)
    for kw in ignore_keywords:
        # match por texto normalizado
        if normalize_text(kw) in text_norm:
            return True
        # match por dígitos quando houver números no kw
        if any(ch.isdigit() for ch in kw):
            kw_digits = digits_only(kw)
            if kw_digits and kw_digits in raw_digits:
                return True
    return False

def has_any_kw(text_norm: str, keywords: list[str]) -> bool:
    # Normaliza cada keyword da mesma forma do texto para lidar com acentos/pontuação
    for kw in keywords:
        if normalize_text(kw) in text_norm:
            return True
    return False

def is_recebimento(text_norm: str, raw_text: str = "") -> bool:
    """
    Detecta 'Recebimento de Mercadoria' com flexibilidade:
    - 'Recebimento de Mercadoria'
    - 'Nº/No/N0 Recebimento' (checado no texto cru para pegar 'º')
    - 'Solicitação de Pagamento' (variações sem acento)
    - 'Relatório Impresso por SAP Business'
    - 'Impresso por SAP Business One'
    """
    # 1) Frase canônica (já normalizada)
    if "recebimento de mercadoria" in text_norm:
        return True

    # 2) Nº/No/N0 Recebimento (olha no texto cru por causa de 'º')
    if re.search(r"\bn[ºo0]\s*recebimento\b", raw_text or "", flags=re.IGNORECASE):
        return True

    # 3) Novas frases (sem acentos / pontuação)
    # solicitacao de pagamento
    if "solicitacao de pagamento" in text_norm:
        return True

    # relatorio impresso por sap business (com/sem 'one')
    if re.search(r"\brelatorio\s+impresso\s+por\s+sap\s+business(\s+one)?\b", text_norm):
        return True

    # impresso por sap business (com/sem 'one')
    if re.search(r"\bimpresso\s+por\s+sap\s+business(\s+one)?\b", text_norm):
        return True

    return False

def is_nota(text_norm: str, nf_keywords: list[str]) -> bool:
    return has_any_kw(text_norm, nf_keywords)

def find_names(text_norm: str, names_keywords: list[str]) -> set[str]:
    found = set()
    for name in names_keywords:
        if name.lower() in text_norm:
            found.add(name)
    return found

def build_segments(pages_text: list[str], nf_keywords: list[str], names_keywords: list[str], ignore_keywords: list[str]):
    """
    MODO ESTRITO:
    Cria um segmento POR PÁGINA marcada. Apenas páginas que contenham
    'recebimento de mercadoria' (ou variações) ou palavras-chave de NF
    viram segmentos. Páginas sem marca NÃO são anexadas a nenhum segmento.
    """
    receb_list = []
    nota_list = []
    page_types = []  # 'receb', 'nota', or None
    page_names = []

    for i, t in enumerate(pages_text):
        tn = normalize_text(t)
        names = find_names(tn, names_keywords)  # set()

        # << NOVO: checar palavras proibidas >>
        ignored = contains_ignored(tn, t, ignore_keywords)

        if is_recebimento(tn, t):
            if ignored:
                page_types.append(None)
                page_names.append(names)
                print(f"[IGNORADO] Página {i+1}: Recebimento ignorado por palavra bloqueada.")
                continue
            page_types.append('receb')
            page_names.append(names)
            receb_list.append({'type': 'receb', 'pages': [i], 'names': set(names)})

        elif is_nota(tn, nf_keywords):
            if ignored:
                page_types.append(None)
                page_names.append(names)
                print(f"[IGNORADO] Página {i+1}: NF ignorada por palavra bloqueada.")
                continue
            page_types.append('nota')
            page_names.append(names)
            nota_list.append({'type': 'nota', 'pages': [i], 'names': set(names)})

        else:
            page_types.append(None)
            page_names.append(names)

    return receb_list, nota_list, page_types, page_names

# ---------- Pareamento e Export ----------
def choose_owner_name(names_from_segments: list[set[str]], names_priority: list[str]) -> str | None:
    # Junta todos os nomes encontrados e escolhe pelo primeiro que aparece na ordem de prioridade
    union = set()
    for s in names_from_segments:
        union.update(s)
    for name in names_priority:
        if name in union:
            return name
    return None

def ensure_dir(p):
    if not os.path.exists(p):
        os.makedirs(p, exist_ok=True)

def unique_outpath(out_dir: str, original_pdf_path: str) -> str:
    base = os.path.splitext(os.path.basename(original_pdf_path))[0]
    candidate = os.path.join(out_dir, f"{base}.pdf")
    if not os.path.exists(candidate):
        return candidate
    # Se já existe, incrementa _1, _2, _3...
    i = 1
    while True:
        candidate = os.path.join(out_dir, f"{base}_{i}.pdf")
        if not os.path.exists(candidate):
            return candidate
        i += 1

def export_pdf_segments(original_pdf_path: str,
                        output_base: str,
                        pair_index: int,
                        segs_pages: list[list[int]],
                        seg_labels: list[str],
                        owner_name: str | None):
    """
    Exporta APENAS as páginas dos segmentos recebidos (NF/Recebimento), em um novo PDF.
    O nome do arquivo de saída preserva o nome do PDF original; se já existir, acrescenta _1, _2...
    """
    reader = PdfReader(original_pdf_path)
    writer = PdfWriter()

    # Adiciona SOMENTE as páginas dos segmentos, na MESMA ORDEM do PDF original
    flat_pages = []
    for pages in segs_pages:
        flat_pages.extend(pages)

    # Mantém apenas índices válidos e ordena pelo número da página (ordem original)
    valid_sorted = sorted({p for p in flat_pages if 0 <= p < len(reader.pages)})

    for p in valid_sorted:
        writer.add_page(reader.pages[p])

    titular = owner_name if owner_name else "Sem_Titular"
    out_dir = os.path.join(output_base, titular)
    ensure_dir(out_dir)

    out_path = unique_outpath(out_dir, original_pdf_path)  # mantém nome original; numera se necessário
    with open(out_path, "wb") as f:
        writer.write(f)
    print(f"Gerado: {out_path}")

def pair_and_export(pdf_path: str,
                    separated_base_folder: str,
                    receb_list: list[dict],
                    nota_list: list[dict],
                    names_keywords: list[str]):
    """
    Pareia Recebimento->NF na ordem.
    Se houver RECEBIMENTOS SOBRANDO e existir ao menos 1 par no PDF,
    ANEXA os recebimentos restantes ao ÚLTIMO par daquele PDF.
    Se não houver nenhum par, recebimentos sobrando NÃO são movidos.
    NFs sobrando continuam saindo solo.
    """
    min_pairs = min(len(receb_list), len(nota_list))
    pair_idx = 1

    if min_pairs == 0:
        # Não há par algum; NFs, se houver, saem solo; Recebimentos não se movem.
        for k in range(0, len(nota_list)):
            n = nota_list[k]
            owner = choose_owner_name([n['names']], names_keywords)
            export_pdf_segments(
                original_pdf_path=pdf_path,
                output_base=separated_base_folder,
                pair_index=pair_idx,
                segs_pages=[n['pages']],
                seg_labels=["nf"],
                owner_name=owner
            )
            pair_idx += 1

        if len(receb_list) > 0:
            for j in range(0, len(receb_list)):
                print(f"[AVISO] Recebimento sem NF (segmento #{j+1}) — mantido no PDF original.")
        return

    # Monta os pares básicos
    pairs = []
    for i in range(min_pairs):
        r = receb_list[i]
        n = nota_list[i]
        pairs.append({
            "pages_list": [r['pages'], n['pages']],
            "labels":     ["receb", "nf"],
            "names_sets": [r['names'], n['names']]
        })

    # Se houver recebimentos sobrando, ANEXA todos ao ÚLTIMO par
    if len(receb_list) > min_pairs:
        last_pair = pairs[-1]
        for j in range(min_pairs, len(receb_list)):
            r_extra = receb_list[j]
            last_pair["pages_list"].append(r_extra['pages'])
            last_pair["labels"].append("receb")          # mantém a marcação
            last_pair["names_sets"].append(r_extra['names'])
            print(f"[INFO] Recebimento extra anexado ao último par (segmento extra #{j+1}).")

    # Exporta os pares (o último pode ter anexos extras)
    for p in pairs:
        owner = choose_owner_name(p["names_sets"], names_keywords)
        export_pdf_segments(
            original_pdf_path=pdf_path,
            output_base=separated_base_folder,
            pair_index=pair_idx,
            segs_pages=p["pages_list"],
            seg_labels=p["labels"],
            owner_name=owner
        )
        pair_idx += 1

    # NFs sobrando saem solo
    for k in range(min_pairs, len(nota_list)):
        n = nota_list[k]
        owner = choose_owner_name([n['names']], names_keywords)
        export_pdf_segments(
            original_pdf_path=pdf_path,
            output_base=separated_base_folder,
            pair_index=pair_idx,
            segs_pages=[n['pages']],
            seg_labels=["nf"],
            owner_name=owner
        )
        pair_idx += 1

# ---------- Pipeline ----------
def process_pdfs(source_folder,
                 separated_base_folder,
                 general_keywords,
                 ignore_keywords,
                 names_keywords,
                 dpi=150,
                 lang="por"):

    print(f"Processando arquivos no diretório: {source_folder}")

    ensure_dir(separated_base_folder)

    total_files = 0
    processed = 0

    for file in os.listdir(source_folder):
        if not file.lower().endswith(".pdf"):
            continue

        total_files += 1
        file_path = os.path.join(source_folder, file)
        print(f"\n--- OCR '{file_path}' ---")
        pages_text = ocr_pdf_pages(file_path, dpi=dpi, lang=lang)
        if not pages_text:
            print(f"[!] Não foi possível extrair texto de '{file_path}'. Pulando.")
            continue

        # Segmenta
        receb_list, nota_list, page_types, page_names = build_segments(
            pages_text, general_keywords, names_keywords, ignore_keywords
        )

        if not receb_list and not nota_list:
            print("Nenhum 'Recebimento' nem 'NF' detectado. Pulando.")
            continue

        print(f"Documentos detectados -> Recebimentos: {len(receb_list)} | Notas: {len(nota_list)}")
        pair_and_export(
            pdf_path=file_path,
            separated_base_folder=separated_base_folder,
            receb_list=receb_list,
            nota_list=nota_list,
            names_keywords=names_keywords
        )
        processed += 1

    print(f"\nTotal de PDFs encontrados: {total_files}")
    print(f"Total de PDFs processados: {processed}")
    print("Concluído!")

# ---------- MAIN ----------
if __name__ == "__main__":
    # Ajuste seus diretórios aqui:
    source_folder = r"C:\Users\conta\OneDrive\Documentos\NOTAS DE SERVICO\SEPARADOS"
    separated_base_folder = r"C:\Users\conta\OneDrive\Documentos\NOTAS DE SERVICO\SEPARADOS"

    # Palavras que caracterizam NF/NFS-e (as suas originais):
    general_keywords = [
        "prefeitura municipal", "tomador de serviços", "CNAES", "ISS DEVIDO", "NFS-e",
        "TOMADOR  DO SERVIÇO", "Codigo de Tributaçao Nacional", "EMITENTE DA NFS-e",
        "Documento Auxiliar da NFS-e", "Chave de Acesso da NFS-e", "Dococumento Auxiliar da NFS-e"
    ]

    # Palavras/padrões que, se aparecerem numa página marcada como NF ou Recebimento, fazem a página ser ignorada
    ignore_keywords = [
        "DACTE",
        "DUAM",
        "ARRECADAÇÃO MUNICIPAL",  # tolerante a 'ARRECADAÇAO' por normalização
        "RECIBO DO PAGADOR",
        "CONHECIMENTOS DE TRANSPORTE",
        "CAPÍTULO XII",
        "C M DE OLIVEIRA",
        "09.688.164/001-28",
        "36.372.676/0001-53",
    ]

    # Mantido como você pediu:
    names_keywords = ["Gilson", "Lucas", "Adriana", "Cleuber"]

    process_pdfs(
        source_folder=source_folder,
        separated_base_folder=separated_base_folder,
        general_keywords=general_keywords,
        ignore_keywords=ignore_keywords,
        names_keywords=names_keywords,
        dpi=150,
        lang="por"
    )
