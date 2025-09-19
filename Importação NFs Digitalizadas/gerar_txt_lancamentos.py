#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import re
from openpyxl import load_workbook

DEFAULT_XLSX = r"C:\Users\conta\Downloads\PNG_\lancamentos.xlsx"

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def cnpj_is_valid(cnpj: str) -> bool:
    d = only_digits(cnpj)
    if len(d) != 14 or d == d[0] * 14:
        return False
    pesos1 = [5,4,3,2,9,8,7,6,5,4,3,2]
    soma1 = sum(int(d[i]) * pesos1[i] for i in range(12))
    dv1 = 11 - (soma1 % 11)
    dv1 = 0 if dv1 >= 10 else dv1
    pesos2 = [6,5,4,3,2,9,8,7,6,5,4,3,2]
    soma2 = sum(int(d[i]) * pesos2[i] for i in range(13))
    dv2 = 11 - (soma2 % 11)
    dv2 = 0 if dv2 >= 10 else dv2
    return d[12] == str(dv1) and d[13] == str(dv2)

def norm_date_dash(s: str) -> str:
    """
    Aceita 'dd/mm/aaaa' ou 'dd-mm-aaaa' e retorna sempre 'dd-mm-aaaa'.
    Não formata se não reconhecer.
    """
    s = (s or "").strip()
    m = re.search(r"(\d{2})[\/\-](\d{2})[\/\-](\d{4})", s)
    if not m:
        return s
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

def main():
    # Caminho da planilha:
    # 1) 1º argumento de linha de comando
    # 2) se não houver, usa DEFAULT_XLSX (C:\Users\conta\Downloads\PNG_\lancamentos.xlsx)
    xlsx_path = sys.argv[1] if len(sys.argv) >= 2 else DEFAULT_XLSX
    if not os.path.exists(xlsx_path):
        print(f"[ERRO] Planilha não encontrada: {xlsx_path}")
        sys.exit(1)

    # Caminho do TXT de saída (opcional 2º argumento)
    out_path = sys.argv[2] if len(sys.argv) >= 3 else os.path.join(os.path.dirname(xlsx_path), "saida_lancamentos.txt")

    try:
        wb = load_workbook(xlsx_path, data_only=True)
    except Exception as e:
        print(f"[ERRO] Falha ao abrir planilha: {e}")
        sys.exit(1)

    # tenta aba 'lancamentos', senão pega a primeira
    sh = wb["lancamentos"] if "lancamentos" in wb.sheetnames else wb[wb.sheetnames[0]]

    # LAYOUT SUPORTADO:
    #  A:Data | B:CodFazenda | C:Conta | D:NumeroNF | E:Historico | F:CNPJ | G:Tipo | H:Padrao | [I:CaminhoNF opcional] | (Valor1) | (Valor2) | (Flag)
    #  -> Se "CaminhoNF" existir, Valor1/Valor2/Flag ficam em J/K/L; caso contrário, em I/J/K.

    # mapa de cabeçalhos por nome (case-insensitive)
    header_idx = {}
    for c in range(1, sh.max_column + 1):
        name = str(sh.cell(1, c).value or "").strip().lower()
        if name:
            header_idx[name] = c

    def col(name: str, default_if_missing: int) -> int:
        return header_idx.get(name.lower(), default_if_missing)

    i_data    = col("Data",       1)
    i_codfaz  = col("CodFazenda", 2)
    i_conta   = col("Conta",      3)
    i_numero  = col("NumeroNF",   4)
    i_hist    = col("Historico",  5)
    i_cnpj    = col("CNPJ",       6)
    i_tipo    = col("Tipo",       7)
    i_padrao  = col("Padrao",     8)

    tem_caminho = "caminhonf" in header_idx
    i_valor1 = col("Valor1", 10 if tem_caminho else 9)
    i_valor2 = col("Valor2", i_valor1 + 1)
    i_flag   = col("Flag",   i_valor2 + 1)

    rows = []
    for r in range(2, sh.max_row + 1):
        data            = norm_date_dash(str(sh.cell(r, i_data).value   or "").strip())
        cod_faz         = str(sh.cell(r, i_codfaz).value                or "").strip()
        conta           = str(sh.cell(r, i_conta).value                 or "").strip() or "001"
        numero_nf       = str(sh.cell(r, i_numero).value                or "").strip()
        historico       = str(sh.cell(r, i_hist).value                  or "").strip()
        cnpj_raw        = str(sh.cell(r, i_cnpj).value                  or "").strip()
        tipo            = str(sh.cell(r, i_tipo).value                  or "").strip() or "2"
        padrao          = str(sh.cell(r, i_padrao).value                or "").strip() or "000"
        valor1          = str(sh.cell(r, i_valor1).value                or "").strip()
        valor2          = str(sh.cell(r, i_valor2).value                or "").strip()
        flag            = str(sh.cell(r, i_flag).value                  or "").strip() or "N"

        # mínimos obrigatórios para o TXT (se faltar, pula a linha)
        if not data or not cod_faz or not numero_nf or not valor1 or not valor2:
            continue

        # CNPJ no TXT: somente dígitos se válido; caso contrário 'INVALIDO'
        cnpj_out = "INVALIDO"
        if cnpj_raw and cnpj_raw.upper() != "INVALIDO":
            d = only_digits(cnpj_raw)
            cnpj_out = d if (d and cnpj_is_valid(d)) else "INVALIDO"

        linha = f"{data}|{cod_faz}|{conta}|{numero_nf}|1|{historico}|{cnpj_out}|{tipo}|{padrao}|{valor1}|{valor2}|{flag}"
        rows.append(linha)


    if not rows:
        print("[AVISO] Nenhuma linha válida encontrada para gerar TXT.")
        sys.exit(0)

    try:
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            for ln in rows:
                f.write(ln + "\n")
        print(f"[OK] TXT gerado: {out_path} ({len(rows)} linha(s))")
    except Exception as e:
        print(f"[ERRO] Falha ao escrever TXT: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
