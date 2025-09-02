# participantes_extract.py
# Lê "PAGAMENTOS" no mesmo diretório e acrescenta registos únicos no arquivo "participantes"
# Formato das linhas gravadas: <cpf_cnpj_digits>|<NOME>|<tipo>
#   tipo: 1 = Pessoa Jurídica (14 dígitos), 2 = Pessoa Física (11 dígitos)

import os, sys, re
from collections import OrderedDict

# ----------------- Helpers -----------------
def only_digits(s: str) -> str:
    return re.sub(r'\D', '', s or '')

def collapse_spaces(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or '').strip())

def extract_name_from_historico(h: str) -> str:
    """
    Regras para descobrir o NOME com base no histórico:
      1) Se houver parênteses, pega o ÚLTIMO conteúdo: (... NOME ...)
      2) Se houver "NF <n>", pega o que vier após esse número
      3) Senão, pega o que vier após o ÚLTIMO token que contenha dígito
      4) Fallback: histórico inteiro (limpo)
    """
    if not h:
        return ""
    # 1) Parênteses
    par = re.findall(r"\(([^)]+)\)", h)
    if par:
        cand = collapse_spaces(par[-1])
        if cand:
            return cand
    # 2) Após "NF <n>"
    m = re.search(r"\bNF\b[^\d]*\d+\s+(.+)$", h, flags=re.IGNORECASE)
    if m:
        cand = collapse_spaces(m.group(1))
        if cand:
            return cand
    # 3) Após o último token com dígito
    tokens = h.split()
    last_digit_idx = -1
    for i, tok in enumerate(tokens):
        if any(ch.isdigit() for ch in tok):
            last_digit_idx = i
    if last_digit_idx >= 0 and last_digit_idx + 1 < len(tokens):
        cand = collapse_spaces(" ".join(tokens[last_digit_idx + 1:]))
        if cand:
            return cand
    # 4) Fallback
    return collapse_spaces(h)

def tipo_from_digits(doc: str) -> int:
    n = len(doc)
    if n == 14: return 1  # PJ
    if n == 11: return 2  # PF
    return 0

def parse_layout_pagamentos_line(line: str):
    """
    Espera layout (12 colunas):
    data|imovel|conta|num_doc|tipo_doc|historico|cpf_cnpj|tipo_lanc|ent|sai|saldo|nat
    """
    parts = [p.strip() for p in line.rstrip("\r\n").split("|")]
    if len(parts) < 7:
        return None
    historico = parts[5]
    raw_doc   = parts[6]
    doc = only_digits(raw_doc)
    t = tipo_from_digits(doc)
    if t == 0:
        return None
    nome = extract_name_from_historico(historico).upper()
    return doc, nome, t

def read_pagamentos_file():
    """Procura 'PAGAMENTOS' dentro da pasta 'Importação DANFE' (mesmo nível de layout importacao)."""
    # Pega a pasta atual (onde está o script)
    base = os.path.dirname(os.path.abspath(__file__))

    # Sobe dois níveis: sai de participantes -> layout importacao -> SISTEMA LCDPR
    root = os.path.dirname(os.path.dirname(base))

    # Caminho da pasta Importação DANFE (irmã de layout importacao)
    import_dir = os.path.join(root, "Importação DANFE")

    # Possíveis nomes do arquivo
    candidates = ["PAGAMENTOS", "PAGAMENTOS.txt", "pagamentos", "pagamentos.txt"]

    for name in candidates:
        path = os.path.join(import_dir, name)
        if os.path.exists(path) and os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read().splitlines()

    sys.stderr.write(f"Arquivo 'PAGAMENTOS' não encontrado em {import_dir}.\n")
    sys.exit(1)

def find_participantes_path():
    """Retorna o caminho do arquivo de saída e quais já existem (por doc)."""
    base = os.path.dirname(os.path.abspath(__file__))
    path_a = os.path.join(base, "participantes")
    path_b = os.path.join(base, "participantes.txt")
    path = path_a
    if os.path.exists(path_b) and os.path.isfile(path_b):
        path = path_b
    elif os.path.exists(path_a) and os.path.isfile(path_a):
        path = path_a
    # Coleta docs já existentes
    existing_docs = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for ln in f:
                parts = [p.strip() for p in ln.rstrip("\r\n").split("|")]
                if len(parts) >= 1:
                    doc = only_digits(parts[0])
                    if doc:
                        existing_docs.add(doc)
    return path, existing_docs

# ----------------- Main -----------------
def main():
    lines = read_pagamentos_file()

    # Dedup interno do arquivo PAGAMENTOS por documento.
    # Mantém o primeiro, mas substitui se aparecer um nome mais “rico” (mais longo).
    dedup = OrderedDict()  # doc -> (nome, tipo)
    for line in lines:
        if not line.strip():
            continue
        parsed = parse_layout_pagamentos_line(line)
        if not parsed:
            continue
        doc, nome, t = parsed
        if doc not in dedup or (len(nome) > len(dedup[doc][0])):
            dedup[doc] = (nome, t)

    # Caminho do arquivo de saída + docs já existentes
    out_path, existing_docs = find_participantes_path()

    # Filtra apenas docs que ainda não existem no arquivo de saída
    novos = [(doc, nm, tp) for doc, (nm, tp) in dedup.items() if doc not in existing_docs]

    if not novos:
        print("Nada a acrescentar. Todos os documentos já existem em 'participantes'.")
        return

    # Acrescenta (append) no arquivo de saída
    with open(out_path, "a", encoding="utf-8") as f:
        for doc, nome, t in novos:
            f.write(f"{doc}|{nome}|{t}\n")

    print(f"Acrescentados {len(novos)} registro(s) em: {out_path}")

if __name__ == "__main__":
    main()
