import os
import re
import shutil
from pdf2image import convert_from_path
import pytesseract
import concurrent.futures
from PyPDF2 import PdfReader, PdfWriter
import unicodedata

# ---------- HELPERS DE LOG E RESUMO (compatível com a UI) ----------
import os, time
from typing import List

# ---------- CANCELAMENTO + helpers de log ----------
def _cancelled() -> bool:
    try:
        cb = globals().get("is_cancelled", None)
        return bool(cb and callable(cb) and cb())
    except Exception:
        return False

def _divider():
    print("─" * 60, flush=True)  # separador visual

# "negrito" universal via caracteres Unicode em negrito (funciona em UI de texto puro)
_BOLD_TRANS = str.maketrans({
    # A–Z
    **{c: b for c, b in zip(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
        "𝗔𝗕𝗖𝗗𝗘𝗙𝗚𝗛𝗜𝗝𝗞𝗟𝗠𝗡𝗢𝗣𝗤𝗥𝗦𝗧𝗨𝗩𝗪𝗫𝗬𝗭"
    )},
    # a–z
    **{c: b for c, b in zip(
        "abcdefghijklmnopqrstuvwxyz",
        "𝗮𝗯𝗰𝗱𝗲𝗳𝗴𝗵𝗶𝗷𝗸𝗹𝗺𝗻𝗼𝗽𝗾𝗿𝘀𝘁𝘂𝘃𝘄𝘅𝘆𝘇"
    )},
    # 0–9 (bold sans-serif)
    **{c: b for c, b in zip(
        "0123456789",
        "𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟟𝟠𝟡"
    )}
})
def _bold(s: str) -> str:
    return (s or "").translate(_BOLD_TRANS)

def _short_path(p: str, max_len: int = 90) -> str:
    p = str(p)
    if len(p) <= max_len: return p
    return f"{p[:14]}...{p[-50:]}"

def _first_page_idx(seg: dict) -> int:
    try:
        return int((seg.get("pages") or [])[0])
    except Exception:
        return -1


def _format_pairs(receb_list: list, nota_list: list) -> str:
    # monta algo tipo: "Receb[0] → NF[1]; Receb[2] → NF[3]"
    m = min(len(receb_list), len(nota_list))
    if m == 0:
        return "—"
    pairs = []
    for i in range(m):
        rpg = receb_list[i].get('pages', [])[0] if receb_list[i].get('pages') else '?'
        npg = nota_list[i].get('pages', [])[0] if nota_list[i].get('pages') else '?'
        pairs.append(f"Receb[{rpg}] → NF[{npg}]")
    return "; ".join(pairs)

def print_resumo_arquivo(filename: str,
                         paginas: int,
                         dpi: int,
                         threads: int,
                         receb_qtd: int,
                         nf_qtd: int,
                         ign_qtd: int,
                         pairing_str: str,
                         out_paths: List[str],
                         duracao_s: float):
    out_display = out_paths[0] if out_paths else "—"
    print(
        "📄 " + filename + "\n"
        f"   🧱 Páginas: {paginas} | 🎚️ DPI: {dpi} | 🧵 Threads: {threads}\n"
        f"   🧩 Segm.: Receb={receb_qtd} | NF={nf_qtd} | Ign={ign_qtd}\n"
        f"   🤝 Pareamento: {pairing_str}\n"
        f"   📤 PDF: {_short_path(out_display)}\n"
        f"   🕒 Duração: {int(round(duracao_s))}s",
        flush=True
    )

# ---------- OCR ----------
def process_page(image, page_num, lang="por"):
    if _cancelled():
        return ""
    try:
        # OCR mais robusto e menos ruído
        text = pytesseract.image_to_string(
            image,
            lang=lang,                # recomendo lang="por" (ou "por+eng" se tiver muito texto em inglês)
            config="--oem 3 --psm 6"
        )
        return text or ""
    except Exception:
        return ""


def ocr_pdf_pages(pdf_path, dpi=300, lang="por"):
    # Retorna (texts:list[str], num_pages:int, threads:int)
    if _cancelled():
        return [], 0, 0
    try:
        images = convert_from_path(pdf_path, dpi=dpi)
    except Exception as e:
        print(f"❌ Erro ao converter '{pdf_path}' em imagens: {e}", flush=True)
        return [], 0, 0

    num_pages = len(images)
    max_workers = max(1, min((os.cpu_count() or 4), 8))
    if _cancelled():
        return [], num_pages, max_workers

    results = [None] * num_pages
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_page, img, i, lang): i for i, img in enumerate(images, start=1)}
        for future in concurrent.futures.as_completed(futures):
            if _cancelled():
                return [], num_pages, max_workers
            idx = futures[future] - 1
            try:
                results[idx] = future.result()
            except Exception:
                results[idx] = ""
    return [t or "" for t in results if t is not None], num_pages, max_workers

def _first_page_idx(seg: dict) -> int:
    try:
        return int((seg.get("pages") or [])[0])
    except Exception:
        return -1

def print_note_summary(file: str,
                       num_pages: int,
                       dpi: int,
                       threads: int,
                       receb_count: int,
                       nf_count: int,
                       ign_count: int,
                       pairs_text: str,
                       out_paths: list[str],
                       duration_s: float):
    print(f"📄 {_bold(file)}", flush=True)
    print(f"   🧱 Páginas: {num_pages} | 🎚️ DPI: {dpi} | 🧵 Threads: {threads}", flush=True)
    print(f"   🧩 Segm.: Receb={receb_count} | NF={nf_count} | Ign={ign_count}", flush=True)
    print(f"   🤝 Pareamento: {pairs_text or '—'}", flush=True)
    print(f"   📤 PDF: {(_short_path(out_paths[0]) if out_paths else '—')}", flush=True)
    print(f"   🕒 Duração: {int(round(duration_s))}s", flush=True)
    _divider()
    print("", flush=True)  # 🔹 uma linha em branco entre blocos

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

# ✔️ NOVO: marcadores fortes e regex de CNPJ
CNPJ_14DIG_RE = re.compile(r"\d{14}")

NFSE_STRONG_MARKERS = [
    "nfs e",
    "nota fiscal de servico eletronica",
    "documento auxiliar da nfs e",
    "chave de acesso",          # muitas prefeituras
    "numero da nfs e",
    "codigo de verificacao",
]
NFSE_PARTIES = [
    "prestador de servico",
    "tomador de servico",
]
NFSE_PUBLIC = [
    "prefeitura municipal",
    "municipio",
]

def _contains_any(text_norm: str, words: list[str]) -> int:
    return sum(1 for w in words if w in text_norm)

def is_nota(text_norm: str, raw_text: str, nf_keywords: list[str]) -> bool:
    """
    Modo estrito: exige pelo menos:
      - 1 marcador forte  (NFSE_STRONG_MARKERS)  E
      - 1 marcador de partes (NFSE_PARTIES)      E
      - presença de um CNPJ (14 dígitos no bruto)
    OU
      - ( "documento auxiliar da nfs e" E "chave de acesso" E CNPJ )
    """
    strong = _contains_any(text_norm, NFSE_STRONG_MARKERS)
    parties = _contains_any(text_norm, NFSE_PARTIES)
    public  = _contains_any(text_norm, NFSE_PUBLIC)

    has_cnpj = bool(CNPJ_14DIG_RE.search(digits_only(raw_text)))

    has_doc_aux = ("documento auxiliar da nfs e" in text_norm)
    has_chave   = ("chave de acesso" in text_norm)

    rule_strict = (strong >= 1 and parties >= 1 and has_cnpj) or (has_doc_aux and has_chave and has_cnpj)

    # ❗Ignora 'general_keywords' como gatilho único; usamos só como apoio (opcional):
    if not rule_strict:
        return False

    # Opcional: reforça com marca público/município quando existir
    if public == 0 and strong == 1:
        # muito no limite? segure
        return False

    return True

def is_recebimento(text_norm: str, raw_text: str = "") -> bool:
    """
    Apertado para reduzir falso positivo:
    - exige 'recebimento de mercadoria' E referência ao SAP
    - aceita 'nº/no/n0 recebimento' no texto cru, mas também exige SAP
    """
    sap = ("sap business" in text_norm) or ("sap business one" in text_norm)

    if "recebimento de mercadoria" in text_norm and sap:
        return True

    if sap and re.search(r"\bn[ºo0]\s*recebimento\b", raw_text or "", flags=re.IGNORECASE):
        return True

    # ❌ removido: 'solicitacao de pagamento' e 'impresso por sap...' (causavam ruído)
    return False

def find_names(text_norm: str, names_keywords: list[str]) -> set[str]:
    found = set()
    for name in names_keywords:
        if normalize_text(name) in text_norm:
            found.add(name)
    return found


def build_segments(pages_text: list[str], nf_keywords: list[str], names_keywords: list[str], ignore_keywords: list[str]):
    receb_list, nota_list = [], []
    page_types, page_names = [], []
    ignoradas = 0

    for i, t in enumerate(pages_text):
        tn = normalize_text(t)
        names = find_names(tn, names_keywords)
        ignored = contains_ignored(tn, t, ignore_keywords)

        if is_recebimento(tn, t):
            if ignored:
                page_types.append(None); page_names.append(names); ignoradas += 1; continue
            page_types.append('receb'); page_names.append(names)
            receb_list.append({'type': 'receb', 'pages': [i], 'names': set(names)})

        elif is_nota(tn, t, nf_keywords):  # << aqui entra o raw_text
            if ignored:
                page_types.append(None); page_names.append(names); ignoradas += 1; continue
            page_types.append('nota'); page_names.append(names)
            nota_list.append({'type': 'nota', 'pages': [i], 'names': set(names)})

        else:
            page_types.append(None); page_names.append(names)

    return receb_list, nota_list, page_types, page_names, ignoradas


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
    if _cancelled():
        return None

    reader = PdfReader(original_pdf_path)
    writer = PdfWriter()

    flat_pages = []
    for pages in segs_pages:
        flat_pages.extend(pages or [])

    valid_sorted = sorted({p for p in flat_pages if 0 <= p < len(reader.pages)})
    for p in valid_sorted:
        if _cancelled(): return None
        writer.add_page(reader.pages[p])

    titular = owner_name if owner_name else "Sem_Titular"
    out_dir = os.path.join(output_base, titular)
    ensure_dir(out_dir)

    out_path = unique_outpath(out_dir, original_pdf_path)
    if _cancelled():
        return None
    with open(out_path, "wb") as f:
        writer.write(f)
    return out_path

def pair_and_export(pdf_path: str,
                    separated_base_folder: str,
                    receb_list: list[dict],
                    nota_list: list[dict],
                    names_keywords: list[str]):
    out_paths = []
    if _cancelled(): return out_paths

    min_pairs = min(len(receb_list), len(nota_list))
    pair_idx = 1

    if min_pairs == 0:
        for n in nota_list:
            if _cancelled(): return out_paths
            owner = choose_owner_name([n['names']], names_keywords)
            p = export_pdf_segments(pdf_path, separated_base_folder, pair_idx, [n['pages']], ["nf"], owner)
            if p: out_paths.append(p)
            pair_idx += 1
        return out_paths

    pairs = []
    for i in range(min_pairs):
        if _cancelled(): return out_paths
        r, n = receb_list[i], nota_list[i]
        pairs.append({"pages_list": [r['pages'], n['pages']], "names_sets": [r['names'], n['names']]})

    if len(receb_list) > min_pairs:
        last = pairs[-1]
        for extra in receb_list[min_pairs:]:
            if _cancelled(): return out_paths
            last["pages_list"].insert(0, extra['pages'])
            last["names_sets"].insert(0, extra['names'])

    for pinfo in pairs:
        if _cancelled(): return out_paths
        owner = choose_owner_name(pinfo["names_sets"], names_keywords)
        p = export_pdf_segments(pdf_path, separated_base_folder, pair_idx, pinfo["pages_list"], ["receb","nf"], owner)
        if p: out_paths.append(p)
        pair_idx += 1

    for k in range(min_pairs, len(nota_list)):
        if _cancelled(): return out_paths
        n = nota_list[k]
        owner = choose_owner_name([n['names']], names_keywords)
        p = export_pdf_segments(pdf_path, separated_base_folder, pair_idx, [n['pages']], ["nf"], owner)
        if p: out_paths.append(p)
        pair_idx += 1

    return out_paths

# ---------- Pipeline ----------
def process_pdfs(source_folder,
                 separated_base_folder,
                 general_keywords,
                 ignore_keywords,
                 names_keywords,
                 dpi=300,
                 lang="por"):

    # (a UI já mostra "Iniciando…"; não duplicar)
    print(f"🔎 Pasta de origem: {source_folder}", flush=True)
    ensure_dir(separated_base_folder)

    # === AGREGADOR TXT (inicializa) ===
    agg_lines = []
    agg_out = os.path.join(separated_base_folder, "_OCR_AGREGADO.txt")

    for file in os.listdir(source_folder):
        if _cancelled():
            print("⚠️ Cancelado pelo usuário.", flush=True)
            break
        if not file.lower().endswith(".pdf"):
            continue

        file_path = os.path.join(source_folder, file)

        import time
        t0 = time.perf_counter()

        texts, num_pages, threads = ocr_pdf_pages(file_path, dpi=dpi, lang=lang)
        if _cancelled():
            print("⚠️ Cancelado pelo usuário.", flush=True)
            break
        if not texts:
            print(f"❌ Nenhum texto extraído de '{file_path}'. Pulando.", flush=True)
            _divider(); print("", flush=True)
            # === AGREGADOR: adiciona leitura bruta deste PDF ===
            agg_lines.append(f"===== ARQUIVO: {file} =====\n")
            for i, page_txt in enumerate(texts, start=1):
                agg_lines.append(f"-- Página {i} --\n{(page_txt or '').strip()}\n")
            agg_lines.append("\n")

            continue

        # ✅ VETO GLOBAL: se QUALQUER página tiver uma ignore_keyword, NÃO copia nada deste PDF
        has_ignore_anywhere = any(
            contains_ignored(normalize_text(t), t, ignore_keywords)  # usa suas funções já declaradas
            for t in texts
        )
        if has_ignore_anywhere:
            print(f"🚫 Ignorado (palavra proibida encontrada): {file}", flush=True)
            _divider(); print("", flush=True)
            continue

        # ⬇️ CORREÇÃO: build_segments retorna 5 valores (inclui 'ignoradas')
        receb_list, nota_list, page_types, page_names, ignoradas = build_segments(
            texts, general_keywords, names_keywords, ignore_keywords
        )

        if not receb_list and not nota_list:
            print("⚠️ Nenhum 'Recebimento' nem 'NF' detectado. Pulando.", flush=True)
            _divider(); print("", flush=True)
            continue

        # string de pareamento para o resumo
        m = min(len(receb_list), len(nota_list))
        pairs_text = ", ".join(
            [f"Receb[{_first_page_idx(receb_list[i])}] → NF[{_first_page_idx(nota_list[i])}]" for i in range(m)]
        )

        out_paths = pair_and_export(
            pdf_path=file_path,
            separated_base_folder=separated_base_folder,
            receb_list=receb_list,
            nota_list=nota_list,
            names_keywords=names_keywords
        )

        dt = time.perf_counter() - t0
        print_note_summary(
            file=file,
            num_pages=num_pages,
            dpi=dpi,
            threads=threads,
            receb_count=len(receb_list),
            nf_count=len(nota_list),
            ign_count=ignoradas,        # ✅ usa o valor correto
            pairs_text=pairs_text,
            out_paths=out_paths,
            duration_s=dt
        )

        # === AGREGADOR: grava TXT único ===
        if agg_lines:
            try:
                with open(agg_out, "w", encoding="utf-8") as f:
                    f.write("\n".join(agg_lines))
                print(f"📝 TXT consolidado salvo em: {agg_out}", flush=True)
            except Exception as e:
                print(f"❌ Falha ao salvar TXT consolidado: {e}", flush=True)

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
        "Preço do Frete",
        "Técnica - Campo",
        "Diagnóstico Técnico",
        "Hs de Moto",
        "Clásula",
        "COMPRADOR(A,ES)",
        "DANFE",
        "DOCUMENTO AUXILIAR DA NOTA FISCAL",
        "Proposta Comercial",
        "82.413.816/0001-01",
        "INOVAGE"

    ]

    # Mantido como você pediu:
    names_keywords = ["Gilson", "Lucas", "Adriana", "Cleuber"]

    process_pdfs(
        source_folder=source_folder,
        separated_base_folder=separated_base_folder,
        general_keywords=general_keywords,
        ignore_keywords=ignore_keywords,
        names_keywords=names_keywords,
        dpi=300,
        lang="por"
    )
