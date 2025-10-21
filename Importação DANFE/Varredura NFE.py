import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys
# --- NOVOS IMPORTS ---
from difflib import SequenceMatcher
import unicodedata
import xml.etree.ElementTree as ET
import re
from glob import glob
from collections import defaultdict, deque

# --- Força stdout/stderr em UTF-8, independente da code page do Windows ---
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
# --------------------------------------------------------------------------

from pathlib import Path  # NOVO
import json               # NOVO

# =====================================================================
# CONFIGURAÇÕES INICIAIS
# =====================================================================
try:
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import PatternFill, Font, Border, Side, Alignment
    from openpyxl.utils import get_column_letter
    from openpyxl.worksheet.table import Table, TableStyleInfo
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False
    print("⚠️ Aviso: openpyxl não instalado. Formatação de tabelas não estará disponível.")
    print("Instale com: pip install openpyxl")

def _load_json_config():
    """Tenta ler json/config.json no mesmo diretório do script e retorna (base, testes)."""
    try:
        cfg_path = Path(__file__).parent / "json" / "config.json"
        if cfg_path.exists():
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            return cfg.get("base_dados_path"), cfg.get("testes_path")
    except Exception as e:
        print(f"⚠️ Aviso: falha ao ler config.json: {e}")
    return None, None

def _resolve_paths():
    if len(sys.argv) >= 3:
        return sys.argv[1], sys.argv[2]
    base_json, testes_json = _load_json_config()
    if base_json and testes_json:
        return base_json, testes_json
    return (r"\\rilkler\LIVRO CAIXA\TESTE\BASE DE DADOS.xlsx",
            r"\\rilkler\LIVRO CAIXA\TESTE\TESTES.xlsx")

def _resolve_notas_recebidas_path(testes_path: str) -> str | None:
    """
    Tenta obter o caminho do arquivo 'NOTAS RECEBIDAS.xlsx'.
    Prioridade:
      1) Se foi passado como 3º argumento na linha de comando.
      2) Se existir um arquivo com esse nome na MESMA pasta do RELATÓRIO (testes_path).
    """
    try:
        if len(sys.argv) >= 4 and os.path.exists(sys.argv[3]):
            return sys.argv[3]
    except Exception:
        pass
    try:
        from pathlib import Path
        cand = Path(testes_path).parent / "NOTAS RECEBIDAS.xlsx"
        if cand.exists():
            return str(cand)
    except Exception:
        pass
    return None

# Pastas onde estão os XMLs (ajuste/adicione caminhos conforme sua estrutura)
XML_DIRS = [
    r"\\rilkler\LIVRO CAIXA\ISENTOS",         # <— exemplo
    r"\\rilkler\LIVRO CAIXA\OUTROS_XMLS"      # <— adicione mais se precisar
]
SIMILARIDADE_MIN_NOME = 0.80  # 80%

def _norm_txt(s: str) -> str:
    s = str(s or "").upper().strip()
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    s = re.sub(r"\s+", " ", s)
    return s

def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, _norm_txt(a), _norm_txt(b)).ratio()

NS_NFE = {"nfe": "http://www.portalfiscal.inf.br/nfe"}

def _parse_xml_info(xml_path: str):
    """
    Retorna dict com:
        cnpj_emit, xnome_emit, nnf, vnf (float), infcpl (str), ref_list (list[str])
    Ignora/retorna None se algo der muito errado.
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        # atende nfeProc/NFe/infNFe
        inf = root.find(".//nfe:infNFe", NS_NFE)

        # se não for NFe, tente NFSe (padrão nacional)
        if inf is None:
            NS_NFSE = {"nfse": "http://www.sped.fazenda.gov.br/nfse"}
            infs = root.find(".//nfse:infNFSe", NS_NFSE)
            if infs is not None:
                # campos básicos no bloco infNFSe
                cnpj_emit = (infs.findtext(".//nfse:emit/nfse:CNPJ", default="", namespaces=NS_NFSE) or "").strip()
                xnome_emit = infs.findtext(".//nfse:emit/nfse:xNome", default="", namespaces=NS_NFSE) or ""

                # número da nota de serviço: prefira nNFSe; se não houver, caia para nDFSe
                nnf = (infs.findtext(".//nfse:nNFSe", default="", namespaces=NS_NFSE) or "").strip()
                if not nnf:
                    nnf = (infs.findtext(".//nfse:nDFSe", default="", namespaces=NS_NFSE) or "").strip()

                # valor: alguns municípios usam <valores><vLiq>, outros somente no DPS
                vliq_txt = infs.findtext(".//nfse:valores/nfse:vLiq", default="", namespaces=NS_NFSE) or ""
                if vliq_txt:
                    vnf = float(str(vliq_txt).replace(",", "."))
                else:
                    vserv_txt = root.findtext(".//nfse:DPS//nfse:valores//nfse:vServ", default="0", namespaces=NS_NFSE) or "0"
                    vnf = float(str(vserv_txt).replace(",", "."))

                # descrição do serviço costuma vir no DPS
                infcpl = root.findtext(".//nfse:DPS//nfse:xDescServ", default="", namespaces=NS_NFSE) or ""

                # NFSe pode referenciar NF-e em chaves (normalmente não vem); mantemos a lista vazia
                ref_list = []

                return {
                    "cnpj_emit": re.sub(r"\D", "", cnpj_emit).zfill(14),
                    "xnome_emit": xnome_emit,
                    "nnf": nnf,
                    "vnf": vnf,
                    "infcpl": infcpl,
                    "ref_list": ref_list,
                    "path": xml_path,
                }
            # se não achou nem NFe nem NFSe, siga para o return None atual

        if inf is None:
            return None

        cnpj_emit = (inf.findtext(".//nfe:emit/nfe:CNPJ", default="", namespaces=NS_NFE) or "").strip()
        xnome_emit = inf.findtext(".//nfe:emit/nfe:xNome", default="", namespaces=NS_NFE) or ""
        nnf = (inf.findtext(".//nfe:ide/nfe:nNF", default="", namespaces=NS_NFE) or "").strip()
        vnf_txt = inf.findtext(".//nfe:total/nfe:ICMSTot/nfe:vNF", default="0", namespaces=NS_NFE) or "0"
        vnf = float(str(vnf_txt).replace(",", "."))
        infcpl = inf.findtext(".//nfe:infAdic/nfe:infCpl", default="", namespaces=NS_NFE) or ""

        ref_list = []
        for r in inf.findall(".//nfe:NFref/nfe:refNFe", NS_NFE):
            if r is not None and r.text:
                ref_list.append(r.text.strip())

        return {
            "cnpj_emit": re.sub(r"\D", "", cnpj_emit).zfill(14),
            "xnome_emit": xnome_emit,
            "nnf": nnf,
            "vnf": vnf,
            "infcpl": infcpl,
            "ref_list": ref_list,
            "path": xml_path,
        }
    except Exception:
        return None

def _iter_xmls(dirs):
    for d in dirs:
        p = Path(d)
        if not p.exists():
            continue
        for xmlf in p.rglob("*.xml"):
            info = _parse_xml_info(str(xmlf))
            if info:
                yield info

def _should_use_data_nota(nota_row, data_pagamento):
    """
    Retorna True se devemos usar a data da NOTA (planilha) em vez da data de pagamento (base),
    quando o pagamento é do ANO ANTERIOR ao ano de referência do processamento.
    - Ano de referência: coluna 'ANO' da planilha, se existir; caso contrário, ano de data_nota.
    """
    try:
        # 1) tenta a coluna ANO da planilha
        alvo = nota_row.get('ANO', None)
        if pd.notna(alvo):
            alvo = int(str(alvo).strip())
        else:
            alvo = None
    except Exception:
        alvo = None

    # 2) fallback: ano da própria data da nota
    if alvo is None and pd.notna(nota_row.get('data_nota', pd.NaT)):
        alvo = int(nota_row['data_nota'].year)

    # 3) decide
    if alvo is not None and pd.notna(data_pagamento):
        return int(data_pagamento.year) < int(alvo)

    return False

def _xml_menciona_nf_do_mesmo_fornecedor(cnpj_emit: str, nnf_procurada: str, xml_info: dict) -> bool:
    """
    Verdadeiro se o XML é do mesmo fornecedor e 'menciona' a NF procurada:
    - mesmo CNPJ do emitente
    - e a sequência/numero da NF alvo aparece em infCpl OU consta em NFref (chave tem a NF original).
    """
    if xml_info["cnpj_emit"] != cnpj_emit:
        return False
    alvo = re.sub(r"\D", "", str(nnf_procurada))
    infcpl = xml_info.get("infcpl", "") or ""
    if alvo and alvo in re.sub(r"\D", "", infcpl):
        return True
    for ch in xml_info.get("ref_list", []):
        if alvo and alvo in (re.sub(r"\D", "", ch) or ""):
            return True
    return False

base_dados_path, testes_path = _resolve_paths()

# (Opcional) Validação imediata:
if not os.path.exists(base_dados_path):
    print(f"❌ Base de dados não encontrada: {base_dados_path}")
    sys.exit(1)
if not os.path.exists(testes_path):
    print(f"❌ Planilha de testes/relatório não encontrada: {testes_path}")
    sys.exit(1)

print("")
print(f"🗂️  base_dados_path = {base_dados_path}")
print(f"🗂️  testes_path     = {testes_path}")

MAP_CONTAS = {
    "Caixa Geral": "001",
    "Cheques a Compensar": "001",
    "Fundo Fixo - Gildevan": "001",
    "Fundo Fixo - Cleidson Alves": "001",
    "Fundo Fixo - Rodrigo": "001",
    "Fundo Fixo - Wandres": "001",
    "Fundo Fixo - Cezar Dias": "001",
    "Fundo Fixo - Geraldo": "001",
    "Fundo Fixo - Daniel": "001",
    "Fundo Fixo - Hadlaim": "001",
    "Fundo Fixo - Lourival": "001",
    "Fundo Fixo - Rogeris": "001",
    "Fundo Fixo - Joaquim": "001",
    "Caixa Dobrado": "001",
    "Fundo Fxo - Douglas": "001",
    "Fundo Fixo - Samuel": "001",
    "Fundo Fixo - Adarildo": "001",
    "Fundo Fixo - Fabricio": "001",
    "Fundo Fixo - Fernando": "001",
    "Fundo Fixo - Orivan": "001",
    "Fundo Fixo - Saimon": "001",
    "Fundo Fxo - Eduardo": "001",
    "Fundo Fixo - Melquiades": "001",
    "Fundo Fixo - Anivaldo": "001",
    "Fundo Fixo - Cida": "001",
    "Caixa Dobrado - Cobrança": "001",
    "Fundo Fixo - Neto": "001",
    "Conta Rotative Gilson": "001",
    "Fundo Fixo - Osvaldo": "001",
    "Fundo Fixo - Cleto Zanatta": "001",
    "Fundo Fixo - Edison": "001",
    "Fundo Fixo - Phelipe": "001",
    "Caixa Deposito": "001",
    "Fundo Fixo - Valdivino": "001",
    "Fundo Fixo - Jose Domingos": "001",
    "Fudo Fixo - Stenyo": "001",
    "Fundo Fixo - Marcos": "001",
    "Fundo Fixo - ONR": "001",
    "Fundo Fixo - Marcelo Dutra": "001",
    "Fundo Fixo - Gustavo": "001",
    "Fundo Fixo - Delimar": "001",
    "Caixa Contábil": "001",
    "Banco Sicoob_Frutacc_597": "001",
    "Banco Bradesco_Frutacc_28.751": "001",
    "Banco do Brasil_Gilson_21252": "001",
    "Banco do Brasil_Cleuber_24585": "004",
    "Banco da Amazonia_Cleuber_34472": "001",
    "Caixa Economica_Cleuber_20573": "001",
    "Caixa Economica_Adriana_20590": "001",
    "Banco Bradesco_Cleuber_22102": "003",
    "Banco Bradesco_Gilson_27014": "001",
    "Banco Bradesco_Adriana_29260": "001",
    "Banco Bradesco_Lucas 29620": "001",
    "Banco Itau_Gilson_26059": "001",
    "Banco Sicoob_Cleuber_052": "002",
    "Banco Sicoob_Gilson_781": "001",
    "Caixa Economica_Cleuber_25766": "001",
    "Banco Santander_Cleuber_1008472": "001",
    "Banco Sicredi_Cleuber_36120": "001",
    "Banco Sicredi_Gilson_39644": "001",
    "Banco Itau_Cleuber_63206": "001",
    "Banco Sicoob_Cleuber_81934": "002",
    "Caixa Economica_Cleuber_20177": "001",
    "Banco Itau_Frutacc_16900": "001",
    "Banco Sicredi_Anne_27012": "001",
    "Não Mapeado": "0000"
}

# Mapeamento de fazendas para códigos
MAP_FAZENDAS = {
    "Frutacc": "1",
    "União": "2",
    "L3": "3",
    "Primavera": "4",
    "Aliança": "5",
    "Arm. Primavera": "6",
    "Estrela": "7",
    "Barragem": "8",
    "Guara": "9",
    "B. Grande": "8",  # Mapeado para o mesmo código que Barragem
    "Frutacc III": "1",  # Mapeado para o mesmo código que Frutacc
    "Primavera Retiro": "4",  # Mapeado para o mesmo código que Primavera
    "Siganna": "1"  # Mapeado para o mesmo código que Frutacc
}

# Lista de produtos especiais (combustíveis e lubrificantes) - busca por substring
PRODUTOS_ESPECIAIS = ["GASOLINA COMUM", "GASOLINA C-COMUM", "GASOLINA ADITIVADA",
                      "GASOLINA C COMUM", "BC:03- GASOLINA C ADITIVADA",
                      "OLEO DIESEL", "DIESEL", "ETANOL", "MOBILGREASE"]

# --- MAPEA CONTAS por nome -> código (usa normalização e tolerância) ---
_MAP_CONTAS_NORM = { 
    # chave normalizada -> código
    unicodedata.normalize("NFKD", k).encode("ASCII","ignore").decode("ASCII").upper().strip(): v
    for k, v in MAP_CONTAS.items()
}
def _norm_simple(s:str)->str:
    return unicodedata.normalize("NFKD", str(s or "")).encode("ASCII","ignore").decode("ASCII").upper().strip()

def _conta_codigo(nome:str)->str:
    """Retorna código da conta em MAP_CONTAS. Tenta: (1) match exato normalizado;
       (2) 'contém' nos dois sentidos; (3) similaridade >= 0.85; (4) 'Não Mapeado'/0000."""
    if not nome:
        return MAP_CONTAS.get("Não Mapeado", "0000")
    n = _norm_simple(nome)
    # 1) exato
    if n in _MAP_CONTAS_NORM:
        return _MAP_CONTAS_NORM[n]
    # 2) contém/contido
    for k_norm, cod in _MAP_CONTAS_NORM.items():
        if n in k_norm or k_norm in n:
            return cod
    # 3) similaridade
    best_cod, best_sc = None, 0.0
    for k_norm, cod in _MAP_CONTAS_NORM.items():
        sc = SequenceMatcher(None, n, k_norm).ratio()
        if sc > best_sc:
            best_sc, best_cod = sc, cod
    if best_sc >= 0.85 and best_cod:
        return best_cod
    # 4) fallback
    return MAP_CONTAS.get("Não Mapeado", "0000")

# =====================================================================
# LEITURA DA PLANILHA DE NOTAS
# =====================================================================
try:
    print("Processando planilha de notas...")
    df_notas = pd.read_excel(testes_path, sheet_name='RELATORIO', header=5)
    df_despesas = df_notas[df_notas['DESPESAS'].notna()].copy()
    
    # Criar colunas auxiliares
    df_despesas['num_nf_busca'] = df_despesas['Nº NF'].astype(str).str.strip().str.replace(' ', '').str.replace('.', '').str.upper()
    # CNPJ com fallback (se a coluna CNPJ vier vazia/IE, extrai da Chave de Acesso - XML)
    def _cnpj_from_row(row):
        # 1) tenta a coluna CNPJ
        cnpj = re.sub(r'\D', '', str(row.get('CNPJ', '')))
        if len(cnpj) == 14 and cnpj != '0'*14:
            return cnpj

        # 2) tenta extrair da CHAVE DE ACESSO (44 dígitos) na coluna 'XML'
        #    CNPJ do emitente = posições 7–20 (1-based) => fatia [6:20] (0-based)
        chave = re.sub(r'\D', '', str(row.get('XML', '')))
        if len(chave) == 44:
            cnpj_xml = chave[6:20]
            if len(cnpj_xml) == 14:
                return cnpj_xml

        # 3) varre a linha inteira procurando algum bloco de 14 dígitos (ex.: colunas auxiliares)
        for v in row.values:
            s = re.sub(r'\D', '', str(v))
            m = re.search(r'(?<!\d)(\d{14})(?!\d)', s)
            if m:
                return m.group(1)

        # 4) último recurso: zero-fill
        return cnpj.zfill(14)

    df_despesas['cnpj_busca'] = df_despesas.apply(_cnpj_from_row, axis=1)

    df_despesas['valor_busca'] = pd.to_numeric(df_despesas['DESPESAS'], errors='coerce')
    
    # Converter data da nota com formato DD/MM/AAAA
    df_despesas['data_nota'] = pd.to_datetime(
        df_despesas['DATA'], 
        dayfirst=True,
        errors='coerce'
    )
    
    # Obter nome do fornecedor (coluna EMITENTE)
    df_despesas['fornecedor'] = df_despesas['EMITENTE'].astype(str).str.strip()

    df_despesas['fornecedor_norm'] = df_despesas['fornecedor'].apply(_norm_txt)

    # Obter código da fazenda
    df_despesas['cod_fazenda'] = df_despesas['FAZENDA'].map(MAP_FAZENDAS).fillna('0')
    
    # Verificar produtos especiais (busca por substring em qualquer parte do texto)
    df_despesas['produto_upper'] = df_despesas['PRODUTO'].astype(str).str.upper()
    df_despesas['produto_especial'] = df_despesas['produto_upper'].apply(
        lambda x: any(produto in x for produto in PRODUTOS_ESPECIAIS)
    )

    # === NOVO: pular linhas já marcadas de verde (pagas) na aba RELATORIO ===
    df_to_process = df_despesas  # fallback

    try:
        if OPENPYXL_AVAILABLE:
            wb_chk = load_workbook(testes_path, data_only=True)
            ws_chk = wb_chk['RELATORIO']

            deslocamento = 7          # mesmo usado para marcar (C-Q)
            col_inicio, col_fim = 3, 17
            HEX_VERDE = "C6EFCE"

            verdes_idx = set()
            for idx in df_despesas.index:
                row_excel = deslocamento + idx
                is_green = False
                for col in range(col_inicio, col_fim + 1):
                    cell = ws_chk.cell(row=row_excel, column=col)
                    # tenta em start_color / fgColor; aceita 'FFC6EFCE' ou '00C6EFCE' etc.
                    rgb = (
                        getattr(cell.fill.start_color, "rgb", None)
                        or getattr(cell.fill.fgColor, "rgb", None)
                        or ""
                    )
                    if isinstance(rgb, str) and rgb.endswith(HEX_VERDE):
                        is_green = True
                        break
                if is_green:
                    verdes_idx.add(idx)

            if verdes_idx:
                print(f"ℹ️ Pulando {len(verdes_idx)} notas já marcadas em verde (pagas).")
            df_to_process = df_despesas.loc[~df_despesas.index.isin(verdes_idx)]

            if df_to_process.empty:
                print("✅ Todas as notas já possuem pagamento associado (marcadas em verde). Nada a processar agora.")
        else:
            print("⚠️ openpyxl indisponível — não foi possível detectar linhas verdes; processando todas.")
    except Exception as e:
        print(f"⚠️ Não foi possível verificar marcações verdes: {e}")
        df_to_process = df_despesas
    
    print(f"✅ {len(df_despesas)} despesas encontradas | ➡️ a processar: {len(df_to_process)}")

except Exception as e:
    raise ValueError(f"❌ Erro em notas: {str(e)}")

# =====================================================================
# LEITURA DA BASE DE PAGAMENTOS
# =====================================================================
try:
    print("\nProcessando base de pagamentos...")
    df_base = pd.read_excel(base_dados_path, sheet_name='Planilha1', header=None)
    
    header_row = None
    for idx, row in df_base.iterrows():
        if 'Nº NF' in row.values:
            header_row = idx
            break
    
    if header_row is None:
        raise ValueError("Cabeçalho não encontrado na planilha")
    
    df_base = pd.read_excel(base_dados_path, sheet_name='Planilha1', header=header_row)
    df_base = df_base.dropna(subset=['Nº NF']).reset_index(drop=True)
    
    # Mapear colunas (renomeia apenas as que existirem; não faz subset!)
    col_map = {
        'Nº NF': 'num_nf',
        'CPF/CNPJ': 'cnpj',
        'ValorParcela': 'valor',
        'NF Cancelada': 'nota_cancelada',
        'Pagamento Cancelado': 'pagamento_cancelado',
        'Conta pag.': 'banco',
        'Data do Pagamento': 'data_pagamento',
        'Data venc.': 'data_vencimento'
    }
    presentes = {k: v for k, v in col_map.items() if k in df_base.columns}
    df_base.rename(columns=presentes, inplace=True)

    # === NOVO: colunas auxiliares para regras pedidas ===

    # (a) N° Primário → identifica o mesmo pagamento, mesmo se houver linhas múltiplas
    if 'N° Primário' in df_base.columns:
        df_base.rename(columns={'N° Primário': 'num_primario'}, inplace=True)
    else:
        df_base['num_primario'] = np.nan  # se a coluna não existir

    # (b) Nome do fornecedor na base (títulos variam—pegamos o que houver)
    col_for = None
    for cand in ['Fornecedor', 'Favorecido', 'Razão Social', 'Emitente', 'Nome Fornecedor']:
        if cand in df_base.columns:
            col_for = cand
            break
    if col_for is None:
        df_base['fornecedor_base'] = ''
    else:
        df_base.rename(columns={col_for: 'fornecedor_base'}, inplace=True)

    # normalização do nome (para similaridade)
    df_base['fornecedor_base_norm'] = df_base['fornecedor_base'].apply(_norm_txt)

    # Normalização
    df_base['num_nf'] = df_base['num_nf'].astype(str).str.strip().str.replace(' ', '').str.replace('.', '').str.upper()
    df_base['cnpj'] = df_base['cnpj'].astype(str).apply(
        lambda x: ''.join(filter(str.isdigit, x)).zfill(14)
    )
    df_base['valor'] = pd.to_numeric(df_base['valor'], errors='coerce')
    
    # Converter datas com formato DD/MM/AAAA
    df_base['data_pagamento'] = pd.to_datetime(
        df_base['data_pagamento'], 
        dayfirst=True,
        errors='coerce'
    )
    df_base['data_vencimento'] = pd.to_datetime(
        df_base['data_vencimento'], 
        dayfirst=True,
        errors='coerce'
    )
    
    # Normalizar coluna de cancelamento
    df_base['pagamento_cancelado'] = df_base['pagamento_cancelado'].astype(str).str.strip().str.upper()
    df_base['nota_cancelada'] = df_base['nota_cancelada'].astype(str).str.strip().str.upper()
    
    # Adicionar coluna de associação
    df_base['associada'] = False
    
    # Calcular total de parcelas por NF
    df_base['total_parcelas'] = df_base.groupby(['num_nf', 'cnpj'])['num_nf'].transform('size')

    # Ordem da parcela por NF + CNPJ (prioriza data de pagamento; fallback: vencimento; depois valor/primário)
    df_base['_ord_data'] = df_base['data_pagamento'].fillna(df_base['data_vencimento'])
    df_base.sort_values(['num_nf', 'cnpj', '_ord_data', 'valor', 'num_primario'], inplace=True, na_position='last')
    df_base['parcela_idx'] = df_base.groupby(['num_nf', 'cnpj']).cumcount() + 1


    print(f"✅ {len(df_base)} pagamentos encontrados")

except Exception as e:
    print("Erro detalhado:", str(e))
    raise ValueError(f"❌ Erro em pagamentos: {str(e)}")

# =====================================================================
# PROCESSAMENTO COM COMPARAÇÃO DE DATAS DE VENCIMENTO
# =====================================================================
results = []
txt_lines = []
pagamentos_associados = 0
parcelas_nao_pagas = 0
produtos_especiais = 0
produtos_especiais_cancelados = 0
linhas_pagas_idx = []  # Índices das linhas pagas na planilha original
# NOVO: índices das linhas de RECEITA (coluna RECEITAS) que foram “pagas” pelos recebimentos
linhas_receitas_pagas_idx = []

print("\nAssociando pagamentos usando datas de vencimento...")
for i, nota in df_to_process.iterrows():
    # Criar cópia da linha original
    result_row = nota.to_dict()
    
    # === [SUBSTITUIR TODO O BLOCO A PARTIR DAQUI] ===
    # CAMADA 1: NF + CNPJ + não cancelado + não associada
    data_nota = nota['data_nota']

    # >>> PATCH: preparar normalizações/valores de referência para as guardas
    nome_nota_norm = _norm_txt(nota.get('fornecedor', ''))
    valor_nota = float(nota.get('valor_busca') or 0.0)

    def _tolerancia_valor_para_cnpj_diferente(v):
        # tolerância dinâmica: max(R$10, 10% do valor da nota)
        return max(40.0, 0.40 * max(v, 0.0))


    parcela_encontrada = None
    
    mask1 = (
        (df_base['num_nf'] == nota['num_nf_busca']) &
        (df_base['cnpj'] == nota['cnpj_busca']) &
        (~df_base['associada']) &
        (df_base['pagamento_cancelado'] != 'SIM')
    )
    cands = df_base[mask1].copy()

    # Excluir candidatos cujo N° Primário conste como cancelado para essa NF
    if 'num_primario' in df_base.columns and not cands.empty:
        grupo_nf = df_base.loc[(df_base['num_nf'] == nota['num_nf_busca'])].copy()
        primarios_cancelados = set(
            grupo_nf.loc[grupo_nf['pagamento_cancelado'] == 'SIM', 'num_primario']
                    .dropna().astype(str).unique().tolist()
        )
        if primarios_cancelados:
            cands = cands.loc[~cands['num_primario'].astype(str).isin(primarios_cancelados)].copy()
    
    
    # CAMADA 2: mesma NF (ignorando CNPJ) + não cancelado + não associada,
    #           MAS agora exigindo semelhança de nome e coerência de valor/data.
    if cands.empty:
        grupo_nf = df_base.loc[
            (df_base['num_nf'] == nota['num_nf_busca']) &
            (~df_base['associada'])
        ].copy()
    
        # respeitar N° Primário cancelado
        if 'num_primario' in grupo_nf.columns:
            primarios_cancelados = set(
                grupo_nf.loc[grupo_nf['pagamento_cancelado'] == 'SIM', 'num_primario']
                        .dropna().astype(str).unique().tolist()
            )
            grupo_nf = grupo_nf.loc[
                (grupo_nf['pagamento_cancelado'] != 'SIM') &
                (~grupo_nf['num_primario'].astype(str).isin(primarios_cancelados))
            ].copy()
        else:
            grupo_nf = grupo_nf.loc[(grupo_nf['pagamento_cancelado'] != 'SIM')].copy()
    
        # >>> PATCH: exigir similaridade de nome quando CNPJ for diferente
        if not grupo_nf.empty and ('fornecedor_base_norm' in grupo_nf.columns):
            grupo_nf['sim_nome'] = grupo_nf['fornecedor_base_norm'].apply(
                lambda x: _sim(x, nome_nota_norm)
            )
            # Mantém CNPJ IGUAL sem exigir similaridade;
            # Para CNPJ DIFERENTE: exige sim >= 0.80
            grupo_nf = grupo_nf.loc[
                (grupo_nf['cnpj'] == nota['cnpj_busca']) |
                (grupo_nf['sim_nome'] >= SIMILARIDADE_MIN_NOME)
            ].copy()
    
        # >>> PATCH: exigir coerência de VALOR quando CNPJ for diferente
        if not grupo_nf.empty and valor_nota > 0:
            tol = _tolerancia_valor_para_cnpj_diferente(valor_nota)
            # Mantém pagamento se (CNPJ igual) ou (diferença de valor dentro da tolerância)
            grupo_nf['diff_val'] = (grupo_nf['valor'] - valor_nota).abs()
            grupo_nf = grupo_nf.loc[
                (grupo_nf['cnpj'] == nota['cnpj_busca']) |
                (grupo_nf['diff_val'] <= tol)
            ].copy()
    
        # >>> PATCH: exigir coerência de DATA quando CNPJ for diferente (janela ±120 dias)
        if not grupo_nf.empty and not pd.isna(data_nota) and 'data_vencimento' in grupo_nf.columns:
            janela = pd.Timedelta(days=120)
            delta = (grupo_nf['data_vencimento'] - data_nota).abs()
            grupo_nf = grupo_nf.loc[
                (grupo_nf['cnpj'] == nota['cnpj_busca']) |
                (delta <= janela)
            ].copy()
    
        cands = grupo_nf.copy()
    
    
    # CAMADA 3: fallback por NOME (≥80%) se houver coluna de fornecedor na base
    if cands.empty and ('fornecedor_base_norm' in df_base.columns):
        grupo_nf = df_base.loc[
            (df_base['num_nf'] == nota['num_nf_busca']) &
            (~df_base['associada']) &
            (df_base['pagamento_cancelado'] != 'SIM')
        ].copy()
    
        try:
            nome_nota_norm = _norm_txt(nota.get('fornecedor', ''))
            grupo_nf['sim_nome'] = grupo_nf['fornecedor_base_norm'].apply(lambda x: _sim(x, nome_nota_norm))
            cands = grupo_nf.loc[grupo_nf['sim_nome'] >= SIMILARIDADE_MIN_NOME].copy()
        except NameError:
            # _norm_txt/_sim não estão definidos (se o item 3 ainda não foi aplicado); ignore similaridade
            pass
            
    # RANQUEAR candidatos e escolher o melhor
    if not cands.empty:
        cands['score'] = 0.0
        # Preferir CNPJ igual
        cands['score'] += (cands['cnpj'] == nota['cnpj_busca']).astype(float) * 2.0
        # Penalizar CNPJ diferente
        cands['score'] -= (cands['cnpj'] != nota['cnpj_busca']).astype(float) * 1.0

        # Preferir data de vencimento = data da nota
        if not pd.isna(data_nota):
            cands['score'] += (cands['data_vencimento'].dt.date == data_nota.date()).astype(float) * 1.5
        # Aproximação por valor
        cands['diff_val'] = (cands['valor'] - float(nota['valor_busca'])).abs()
        cands['score'] += (np.isclose(cands['valor'], nota['valor_busca'], atol=0.01)).astype(float) * 1.0
        cands['score'] -= (cands['diff_val'] > 5.0).astype(float) * 0.5
        # Similaridade de nome (se existir)
        if 'sim_nome' in cands.columns:
            cands['score'] += cands['sim_nome']

        # Preferir quem tem data de pagamento e banco
        cands['score'] += (~cands['data_pagamento'].isna()).astype(float) * 1.0
        cands['score'] += (cands['banco'].astype(str).str.strip() != '').astype(float) * 1.0

        # Leve preferência se data_pagamento = data_nota
        if not pd.isna(data_nota):
            cands['score'] += (cands['data_pagamento'].dt.date == data_nota.date()).astype(float) * 0.5

        cands = cands.sort_values(['score', 'diff_val', 'data_pagamento'],
                                  ascending=[False, True, False])
        idx_sel = cands.index[0]
        cand = cands.loc[idx_sel]

        # >>> PATCH: safety gate – se CNPJ for diferente, a similaridade precisa existir e ser >=80%
        if str(cand.get('cnpj','')) != str(nota['cnpj_busca']):
            sim_ok = False
            if 'sim_nome' in cand.index and not pd.isna(cand['sim_nome']):
                sim_ok = float(cand['sim_nome']) >= SIMILARIDADE_MIN_NOME
            if not sim_ok:
                parcela_encontrada = None
            else:
                parcela_encontrada = cand
                df_base.at[idx_sel, 'associada'] = True
        else:
            parcela_encontrada = cand
            df_base.at[idx_sel, 'associada'] = True
    # === [FIM DO BLOCO SUBSTITUÍDO] ===
    
    
    # Processar resultado
    if parcela_encontrada is not None:
        # Verificar se a parcela tem dados de pagamento válidos
        if pd.isna(parcela_encontrada['data_pagamento']) or str(parcela_encontrada['banco']).strip() == '':
            # Parcela encontrada mas sem dados de pagamento
            result_row.update({
                'Status Nota': "Ativa",
                'Status Pagamento': 'Não pago',
                'Banco': '',
                'Data Pagamento': '',
                'Observações': 'Parcela encontrada sem dados de pagamento'
            })
            parcelas_nao_pagas += 1
        else:
            # Determinar status
            status_nota = "Cancelada" if "CANCELADA" in str(parcela_encontrada['nota_cancelada']).upper() else "Ativa"
            status_pag = "Pago"
            
            # Formatar data pagamento
            data_pgto = parcela_encontrada['data_pagamento']
            usa_data_nota = _should_use_data_nota(nota, data_pgto)
            data_base = nota['data_nota'] if (usa_data_nota or pd.isna(data_pgto)) else data_pgto
            data_str = data_base.strftime('%d%m%Y') if not pd.isna(data_base) else ""
            
            # Obter código do banco
            banco_nome = str(parcela_encontrada['banco']).strip()
            cod_banco = _conta_codigo(banco_nome)
            
            # >>> PATCH: tornar a 'origem' mais explícita
            origem = (
                "Associada por CNPJ"
                if str(parcela_encontrada.get('cnpj', '')) == str(nota['cnpj_busca'])
                else "Associada por NF + Nome≈" + f"{float(parcela_encontrada.get('sim_nome',0)): .0%}".replace(" ","")
            )

            # Se CNPJ não bateu mas o N° Primário é diferente (ou seja, outra linha válida)
            if (
                hasattr(parcela_encontrada, "index")
                and 'num_primario' in parcela_encontrada.index
                and pd.notna(parcela_encontrada['num_primario'])
                and str(parcela_encontrada.get('cnpj', '')) != str(nota['cnpj_busca'])
            ):
                origem = "Associada por NF (N° Primário distinto)"
            
            # Se você aplicou o passo de similaridade (item 4/3), acrescenta o % aproximado
            if (
                hasattr(parcela_encontrada, "index")
                and 'sim_nome' in parcela_encontrada.index
                and not pd.isna(parcela_encontrada['sim_nome'])
            ):
                try:
                    origem += " + Nome≈" + f"{float(parcela_encontrada['sim_nome']):.0%}"
                except Exception:
                    pass
                
            # Atualizar linha de resultado (apenas troca o campo 'Observações')
            result_row.update({
                'Status Nota': status_nota,
                'Status Pagamento': status_pag,
                'Banco': cod_banco,  # Agora apenas o código do banco
                'Data Pagamento': data_str,
                'Observações': origem
            })

            
            # Gerar linha para TXT com novo formato
            if status_nota == "Ativa" and status_pag == "Pago" and data_str:
                # === NOVO FORMATO (PIPE '|') ===
                # Ex.: 01-01-2025|006|001|14209|1|PAGAMENTO NF 14209 HOHL MAQUINAS AGRICOLAS LTDA|01608488001250|2|000|573500|573500|N
                data_fmt = data_base.strftime('%d-%m-%Y') if not pd.isna(data_base) else nota['data_nota'].strftime('%d-%m-%Y')
                fornecedor = nota['fornecedor']
                cod_fazenda3 = str(nota['cod_fazenda']).zfill(3)  # "006"
                num_nf = nota['num_nf_busca']
                cnpj = nota['cnpj_busca']
                
                # valor em centavos, sem separadores (ex.: 5735,00 -> "573500")
                valor_cent = str(int(round(float(parcela_encontrada['valor']) * 100)))
                
                # número real da parcela (1, 2, 3, …) vindo da base
                parcela_num = int(parcela_encontrada.get('parcela_idx', 1))
                
                # sufixo no número do documento (ex.: 1350-1, 1350-2, 1350-3)
                num_nf_txt = f"{num_nf}-{parcela_num}"
                
                # descrição com “(PARCELA n)”
                descricao = f"PAGAMENTO NF {num_nf}".upper()
                
                # campo parcela no TXT = n
                parcela_txt = str(parcela_num)
                
                conta_cod_pg = _conta_codigo(parcela_encontrada.get('banco', '') if 'parcela_encontrada' in locals() else '')
                txt_line = [
                    data_fmt,           # 1 - data (DD-MM-YYYY)
                    cod_fazenda3,       # 2 - fazenda (3 dígitos)
                    (conta_cod_pg or "001"),  # 3 - conta (código mapeado; fallback 001)
                    num_nf_txt,         # 4 - número do doc (ex.: 1350-1)
                    parcela_txt,        # 5 - nº da parcela
                    descricao,          # 6 - descrição
                    cnpj,               # 7 - CNPJ (14 dígitos)
                    "2",                # 8 - Tipo (2 = Despesa/Pagamento)
                    "000",              # 9 - Centro/Histórico (fixo)
                    valor_cent,         # 10 - Saída (centavos, sem separador)
                    valor_cent,         # 11 - Valor total (idem)
                    "N"                 # 12 - Marcador
                ]
                txt_lines.append("|".join(txt_line))
                pagamentos_associados += 1
                
                # Registrar como linha paga
                linhas_pagas_idx.append(nota.name)
    else:
        # Verificar se existem parcelas canceladas não associadas
        mask_canceladas = (
            (df_base['num_nf'] == nota['num_nf_busca']) &
            (df_base['cnpj'] == nota['cnpj_busca']) &
            (~df_base['associada']) &
            (df_base['pagamento_cancelado'] == 'SIM')
        )
        parcelas_canceladas = df_base[mask_canceladas]
        
        if not parcelas_canceladas.empty:
            result_row.update({
                'Status Nota': "Ativa",
                'Status Pagamento': 'Cancelado',
                'Banco': '',
                'Data Pagamento': '',
                'Observações': 'Parcela encontrada mas cancelada'
            })
        else:
            result_row.update({
                'Status Nota': "Ativa",
                'Status Pagamento': 'Não pago',
                'Banco': '',
                'Data Pagamento': '',
                'Observações': 'Pagamento não realizado para esta nota'
            })
        parcelas_nao_pagas += 1
    
    # TRATAMENTO ESPECIAL PARA PRODUTOS ESPECIAIS (após processamento normal)
    if nota['produto_especial'] and result_row['Status Pagamento'] == 'Não pago':
        produtos_especiais += 1
        
        # Produto especial SEM PAGAMENTO - aplicar pagamento automático
        data_nota = nota['data_nota']
        data_str = data_nota.strftime('%d%m%Y') if not pd.isna(data_nota) else ""
        
        # Preencher resultado para produto especial
        result_row.update({
            'Status Nota': "Ativa",
            'Status Pagamento': 'Pago',
            'Banco': '001',
            'Data Pagamento': data_str,
            'Observações': 'Produto especial (combustível/lubrificante) - Pagamento automático'
        })
        
        # Gerar linha para TXT
        # === NOVO FORMATO (PIPE '|') — PRODUTOS ESPECIAIS ===
        data_fmt = nota['data_nota'].strftime('%d-%m-%Y')  # DD-MM-AAAA
        fornecedor = nota['fornecedor']
        cod_fazenda3 = str(nota['cod_fazenda']).zfill(3)
        num_nf = nota['num_nf_busca']
        cnpj = nota['cnpj_busca']
        
        # número de parcela para produto especial: tratar como avulso
        parcela_num = 1
        num_nf_txt = f"{num_nf}"  # sem sufixo aqui
        descricao = f"PAGAMENTO NF {num_nf}".upper()
        parcela_txt = str(parcela_num)
        
        # >>> PATCH: corrigir valor_cent para produtos especiais
        valor_cent = str(int(round(float(valor_nota) * 100)))  # usa o valor da própria nota

        # Para produtos especiais não há pagamento na base; use conta "Não Mapeado" como fallback
        conta_cod_pg = '001'
        txt_line = [
            data_fmt, cod_fazenda3, conta_cod_pg,
            num_nf, parcela_txt, descricao, cnpj, "2", "000", valor_cent, valor_cent, "N"
        ]
        txt_lines.append("|".join(txt_line))
        pagamentos_associados += 1
        linhas_pagas_idx.append(nota.name)
        
    
    results.append(result_row)

print(f"\n🔍 Resultados da associação:")
print(f"- Total de despesas processadas: {len(df_despesas)}")
print(f"- Produtos especiais (combustível/lubrificante): {produtos_especiais}")
print(f"  > Produtos especiais cancelados: {produtos_especiais_cancelados}")
print(f"- Parcelas associadas: {pagamentos_associados}")
print(f"- Parcelas não pagas: {parcelas_nao_pagas}")

# =========================
# PASSO EXTRA (pós-associação):
#   Para notas ainda "Não pago", procurar XML do mesmo fornecedor que
#   mencione a NF original; calcular a DIFERENÇA e buscar pagamento igual a essa diferença.
# =========================
print("\nAnalisando XMLs para diferenças (devolução/reefaturamento)...")

# Mapa rápido: índice da nota -> posição no results
idx_to_results_pos = {}
for pos, r in enumerate(results):
    idx_to_results_pos[df_to_process.index[pos]] = pos  # assume ordem idêntica

# Carregar (lazy) infos de XMLs apenas 1x
xml_infos = list(_iter_xmls(XML_DIRS))

ajustes_por_xml = 0

for i, nota in df_to_process.iterrows():
    # se já está pago, ignore
    pos_res = idx_to_results_pos.get(i)
    if pos_res is None:
        continue
    if results[pos_res].get('Status Pagamento') == 'Pago':
        continue

    nf_alvo = str(nota['num_nf_busca'])
    cnpj_alvo = nota['cnpj_busca']
    valor_nf = float(nota['valor_busca'] or 0.0)

    # achar um XML do mesmo fornecedor que referencia essa NF
    achado = None
    for x in xml_infos:
        if _xml_menciona_nf_do_mesmo_fornecedor(cnpj_alvo, nf_alvo, x):
            achado = x
            break

    if not achado:
        continue  # nenhum XML do mesmo fornecedor mencionando esta NF

    # diferença entre a NF original e a NF 'referenciante'
    diferenca = round(abs(valor_nf - float(achado['vnf'])), 2)
    if diferenca <= 0.01:
        continue

    # procurar um pagamento NÃO associado com exatamente esse valor (tolerância centavos)
    #   prioridade: mesmo CNPJ; senão, por nome ≥80% (mesma NF)
    cand = df_base.loc[
        (~df_base['associada']) &
        (df_base['pagamento_cancelado'] != 'SIM') &
        (np.isclose(df_base['valor'], diferenca, atol=0.01))
    ].copy()

    if cand.empty:
        continue

    # priorização: mesma NF alvo, depois CNPJ, depois similaridade por nome
    cand['score'] = 0.0
    cand['score'] += (cand['num_nf'] == nf_alvo).astype(float) * 2.0
    cand['score'] += (cand['cnpj'] == cnpj_alvo).astype(float) * 1.5
    if 'fornecedor_base_norm' in cand.columns:
        cand['sim_nome'] = cand['fornecedor_base_norm'].apply(lambda x: _sim(x, nota['fornecedor_norm']))
        cand.loc[cand['sim_nome'] >= SIMILARIDADE_MIN_NOME, 'score'] += cand['sim_nome']

    cand = cand.sort_values('score', ascending=False)
    j = cand.index[0]
    pgto = cand.loc[j]
    df_base.at[j, 'associada'] = True

    # montar resultado como PAGO (TXT usa valor do PAGAMENTO, não o da NF)
    status_nota = "Ativa"
    status_pag = "Pago"
    # Ano de referência (planilha): tenta 'ANO'; se não houver/estiver vazio, usa ano de data_nota
    try:
        ref_ano = int(nota['ANO']) if ('ANO' in nota.index and pd.notna(nota['ANO'])) else None
    except Exception:
        ref_ano = None
    if ref_ano is None:
        ref_ano = int(nota['data_nota'].year) if ('data_nota' in nota.index and pd.notna(nota['data_nota'])) else None
    
    # Data do pagamento (base)
    data_pg = pgto['data_pagamento']  # pode ser NaT
    
    # Regra: se o pagamento for de ANO ANTERIOR ao ano de referência, usar a data da NOTA
    usar_data_nota = (pd.notna(data_pg) and ref_ano is not None and int(data_pg.year) < ref_ano)
    
    # Data “base” para planilha e TXT
    data_base = nota['data_nota'] if (usar_data_nota or pd.isna(data_pg)) else data_pg
    
    # → Planilha
    data_str = data_base.strftime('%d%m%Y') if pd.notna(data_base) else ""
    banco_nome = str(pgto['banco']).strip()
    cod_banco = _conta_codigo(banco_nome)
    
    results[pos_res].update({
        'Status Nota': status_nota,
        'Status Pagamento': status_pag,
        'Banco': cod_banco,
        'Data Pagamento': data_str,
        'Observações': f"Diferença via XML (NF ref.: {achado.get('nnf','?')} | {Path(achado['path']).name})"
    })
    
    # → TXT com valor do PAGAMENTO (diferença)
    data_fmt = (data_base.strftime('%d-%m-%Y') if pd.notna(data_base)
                else (nota['data_nota'].strftime('%d-%m-%Y') if pd.notna(nota.get('data_nota', pd.NaT)) else ""))
    fornecedor = nota['fornecedor']
    cod_fazenda3 = str(nota['cod_fazenda']).zfill(3)
    num_nf = nf_alvo
    cnpj = cnpj_alvo
    valor_cent = str(int(round(float(diferenca) * 100)))
    descricao = f"PAGAMENTO NF {num_nf}".upper()
    parcela_txt = "1"

    txt_line = [
        data_fmt, cod_fazenda3, MAP_CONTAS.get("Não Mapeado","0000"), num_nf, parcela_txt,
        descricao, cnpj, "2", "000", valor_cent, valor_cent, "N"
    ]
    txt_lines.append("|".join(txt_line))
    pagamentos_associados += 1
    linhas_pagas_idx.append(i)
    ajustes_por_xml += 1

print(f"✅ Ajustes por XML (diferença): {ajustes_por_xml}")

# =====================================================================
# SALVAR RESULTADOS FORMATADOS
# =====================================================================
try:
    print("\nSalvando resultados formatados...")
    df_result = pd.DataFrame(results)
    
    # Remover colunas auxiliares e completamente vazias
    colunas_remover = [
        'num_nf_busca', 'cnpj_busca', 'valor_busca', 'data_nota',
        'produto_upper', 'produto_especial'
    ]
    df_result = df_result.drop(columns=[c for c in colunas_remover if c in df_result.columns], errors='ignore')
    
    # Remover colunas completamente vazias
    df_result = df_result.dropna(axis=1, how='all')
    
    # Selecionar apenas colunas relevantes para a tabela final
    colunas_relevantes = [
        'DATA', 'MÊS', 'ANO', 'Nº NF', 'EMITENTE', 'CNPJ', 'PRODUTO', 
        'CFOP', 'DESPESAS', 'NATUREZA', 'XML', 'FAZENDA', 'Status Nota', 
        'Status Pagamento', 'Banco', 'Data Pagamento', 'Observações'
    ]
    
    # Manter apenas colunas que existem no DataFrame
    colunas_finais = [c for c in colunas_relevantes if c in df_result.columns]
    df_result = df_result[colunas_finais]
    
    # Formatar colunas de data
    if 'Data Pagamento' in df_result.columns:
        df_result['Data Pagamento'] = df_result['Data Pagamento'].apply(
            lambda x: f"{x[:2]}/{x[2:4]}/{x[4:]}" if x and len(x) == 8 else x
        )
    
    # Salvar planilha formatada como tabela (se openpyxl disponível)
    output_excel = "RESULTADO_PAGAMENTOS.xlsx"
    df_result.to_excel(output_excel, index=False)
    
    if OPENPYXL_AVAILABLE:
        try:
            # Carregar o workbook
            wb = load_workbook(output_excel)
            ws = wb.active
            
            # Criar tabela formatada
            tab = Table(displayName="TabelaResultados", ref=f"A1:{get_column_letter(ws.max_column)}{ws.max_row}")
            
            # Adicionar estilo
            style = TableStyleInfo(
                name="TableStyleMedium9", 
                showFirstColumn=False,
                showLastColumn=False, 
                showRowStripes=True, 
                showColumnStripes=False
            )
            tab.tableStyleInfo = style
            ws.add_table(tab)
            
            # Ajustar largura das colunas
            for col in ws.columns:
                max_length = 0
                column = col[0].column_letter
                for cell in col:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2) * 1.2
                ws.column_dimensions[column].width = adjusted_width
            
            # Centralizar cabeçalho
            for cell in ws[1]:
                cell.alignment = Alignment(horizontal='center', vertical='center')
                cell.font = Font(bold=True)
            
            wb.save(output_excel)
            print(f"✅ Planilha formatada salva como tabela: {output_excel}")
        except Exception as e:
            print(f"⚠️ Erro ao formatar tabela: {str(e)}")
    else:
        print(f"✅ Planilha formatada salva: {output_excel}")

    # === PÓS-PROCESSAMENTO: renumeração por NF+CNPJ neste LOTE ===
    # - Se houver 1 só ocorrência de (NF, CNPJ): remove "-1" e o texto "(PARCELA 1)"
    # - Se houver 2+ ocorrências: numera 1..n na ordem da data (DD-MM-YYYY)
    if txt_lines:
        import re
        from datetime import datetime
        from collections import defaultdict

        # Parse das linhas já montadas
        recs = []
        for i, line in enumerate(txt_lines):
            parts = line.split("|")
            if len(parts) < 12:
                continue
            data_txt = parts[0]                # DD-MM-YYYY
            num_nf_field = parts[3]            # pode estar "1234-2"
            cnpj = parts[6]
            base_nf = num_nf_field.split("-", 1)[0]  # "1234"
            try:
                dt = datetime.strptime(data_txt, "%d-%m-%Y")
            except Exception:
                dt = None
            recs.append({"i": i, "parts": parts, "base_nf": base_nf, "cnpj": cnpj, "dt": dt})

        # Agrupa por (NF base, CNPJ)
        grupos = defaultdict(list)
        for r in recs:
            grupos[(r["base_nf"], r["cnpj"])].append(r)

        # Ajusta cada grupo
        for key, lst in grupos.items():
            # ordena por data (asc); se sem data, manda pro fim
            lst.sort(key=lambda r: (r["dt"] is None, r["dt"]))
            n = len(lst)
            if n == 1:
                r = lst[0]
                # tira sufixo -1 e remove "(PARCELA 1)" da descrição
                r["parts"][3] = r["base_nf"]
                r["parts"][5] = re.sub(r"\s*\(PARCELA\s+\d+\)\s*", "", r["parts"][5], flags=re.I)
                # deixa o campo parcela como já estava (normalmente "1")
            else:
                for idx, r in enumerate(lst, start=1):
                    r["parts"][3] = f"{r['base_nf']}-{idx}"   # num_nf com sufixo correto
                    r["parts"][4] = str(idx)                  # campo parcela
                    # garante "(PARCELA n)" na descrição (primeiro remove qualquer anterior)
                    r["parts"][5] = re.sub(r"\s*\(PARCELA\s+\d+\)\s*", "", r["parts"][5], flags=re.I)
                    r["parts"][5] = f"{r['parts'][5]} (PARCELA {idx})"

        # Recria txt_lines na ordem original
        new_lines = [""] * len(txt_lines)
        for r in recs:
            new_lines[r["i"]] = "|".join(r["parts"])
        txt_lines = new_lines
    # === FIM do pós-processamento ===

    # Salvar arquivo TXT
    if txt_lines:
        with open("PAGAMENTOS.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(txt_lines))
        print(f"✅ Arquivo TXT gerado com {len(txt_lines)} pagamentos válidos")
    else:
        print("⚠️  Nenhum pagamento válido para gerar TXT")

    # =====================================================================
    # RECEBIMENTOS (iterar RECEITAS usando NOTAS RECEBIDAS.xlsx)
    # =====================================================================
    try:
        # 1) Caminho do arquivo NOTAS RECEBIDAS
        notas_recebidas_path = _resolve_notas_recebidas_path(testes_path) if "_resolve_notas_recebidas_path" in globals() else (sys.argv[3] if len(sys.argv) >= 4 else None)
        if not notas_recebidas_path or not os.path.exists(notas_recebidas_path):
            print("ℹ️ NOTAS RECEBIDAS.xlsx não encontrado — pulando RECEBIMENTOS.")
        else:
            print(f"\nProcessando RECEBIMENTOS a partir de: {notas_recebidas_path}")

            # 2) Selecionar RECEITAS (>0) na planilha RELATORIO já carregada (df_notas)
            if 'RECEITAS' not in df_notas.columns:
                print("ℹ️ A aba RELATORIO não possui coluna 'RECEITAS' — pulando RECEBIMENTOS.")
            else:
                df_receitas = df_notas.copy()
                df_receitas['valor_receita'] = pd.to_numeric(df_receitas['RECEITAS'], errors='coerce').fillna(0.0)
                df_receitas = df_receitas[df_receitas['valor_receita'] > 0].copy()

                if df_receitas.empty:
                    print("ℹ️ Não há RECEITAS (>0) na RELATORIO. Nada a pagar via recebimentos.")
                else:
                    # 3) Descobrir colunas relevantes no RELATORIO
                    # Nº NF
                    col_nf = 'Nº NF' if 'Nº NF' in df_receitas.columns else ('NF' if 'NF' in df_receitas.columns else None)
                    if not col_nf:
                        print("⚠️ Não encontrei coluna de 'Nº NF' para RECEITAS — pulando RECEBIMENTOS.")
                    else:
                        # Participante (cliente)
                        cand_part_cols = ['CLIENTE','Destinatário','DESTINATÁRIO','DESTINATARIO','PN','Participante','Favorecido','EMITENTE']
                        col_pn = next((c for c in cand_part_cols if c in df_receitas.columns), None) or df_receitas.columns[0]

                        # Outros campos úteis
                        col_data = 'DATA' if 'DATA' in df_receitas.columns else None
                        col_faz = 'FAZENDA' if 'FAZENDA' in df_receitas.columns else None
                        col_cnpj = 'CNPJ' if 'CNPJ' in df_receitas.columns else None

                        # Normalizações e ordenação
                        df_receitas['__pn'] = df_receitas[col_pn].astype(str)
                        df_receitas['__pn_norm'] = df_receitas['__pn'].apply(_norm_txt)
                        df_receitas['__nf_ord'] = pd.to_numeric(
                            df_receitas[col_nf].astype(str).str.replace(r'\D','', regex=True),
                            errors='coerce'
                        )
                        df_receitas = df_receitas.sort_values(['__pn_norm','__nf_ord'], kind='stable')

                        # 4) Ler NOTAS RECEBIDAS e somar recebimentos por PN
                        df_r = pd.read_excel(notas_recebidas_path, sheet_name=0, header=1)

                        col_pn_r = next((c for c in ['PN','Participante','Cliente'] if c in df_r.columns), None)
                        if col_pn_r is None:
                            col_pn_r = 'Unnamed: 4' if 'Unnamed: 4' in df_r.columns else None

                        col_valor_r = next((c for c in ['Valor','VALOR'] if c in df_r.columns), None)
                        if col_valor_r is None:
                            col_valor_r = 'Unnamed: 6' if 'Unnamed: 6' in df_r.columns else None

                        if not col_pn_r or not col_valor_r:
                            print("⚠️ NOTAS RECEBIDAS.xlsx sem colunas de PN/Valor — pulando RECEBIMENTOS.")
                        else:
                            df_r['__pn_norm'] = df_r[col_pn_r].astype(str).apply(_norm_txt)
                            df_r['__valor'] = pd.to_numeric(df_r[col_valor_r], errors='coerce').fillna(0.0)
                            
                            df_r = df_r[df_r['__valor'] > 0].copy()

                            col_conta_r = next((c for c in ['CONTA','Conta','conta'] if c in df_r.columns), None)

                            # header=1 => cabeçalho na linha 2; dados começam na linha 3
                            OFFSET_NR = 3
                            # Coluna Excel (1-based) do "Valor" na planilha NOTAS RECEBIDAS:
                            VALOR_COL_XL = df_r.columns.get_loc(col_valor_r) + 1

                            # normaliza uma funçãozinha para mapear a conta -> código
                            def _map_conta(c):
                                nome = str(c or '').strip()
                                if not nome:
                                    return "0000", ""
                                cod = MAP_CONTAS.get(nome, MAP_CONTAS.get("Não Mapeado", "0000"))
                                return cod, nome

                            if '__data_r' not in df_r.columns and 'Data' in df_r.columns:
                                df_r['__data_r'] = pd.to_datetime(df_r['Data'], dayfirst=True, errors='coerce')
                            elif '__data_r' not in df_r.columns:
                                df_r['__data_r'] = pd.NaT

                            wb_nr = load_workbook(notas_recebidas_path) if OPENPYXL_AVAILABLE else None
                            ws_nr = wb_nr.worksheets[0] if wb_nr else None

                            filas_por_pn = defaultdict(deque)
                            for i, r in df_r.reset_index(drop=True).iterrows():
                                if float(r["__valor"]) > 0:
                                    excel_row = OFFSET_NR + i
                                    conta_cod, conta_nome = _map_conta(r[col_conta_r] if col_conta_r else "")
                                    filas_por_pn[r["__pn_norm"]].append({
                                        "row": excel_row,
                                        "valor": float(r["__valor"]),
                                        "data": r.get("__data_r", pd.NaT),
                                        "conta_cod": conta_cod,      # ← código mapeado (ex.: 003, 004…)
                                        "conta_nome": conta_nome     # ← nome da planilha (coluna CONTA)
                                    })

                            # --- NOVO: localizar CNPJ no NOTAS RECEBIDAS e mapear PN -> CNPJ (apenas dígitos, 14 casas) ---
                            col_cnpj_r = next((c for c in ['CPF/CNPJ','CNPJ','CPF','Documento'] if c in df_r.columns), None)

                            cnpj_por_pn = {}
                            if col_cnpj_r:
                                df_r['__cnpj_r'] = df_r[col_cnpj_r].astype(str).str.replace(r'\D', '', regex=True).str.zfill(14)
                                # Descartar zeros
                                mask_valid = df_r['__cnpj_r'].ne('0'*14)
                                if mask_valid.any():
                                    # Se tiver vários por PN, pega o mais frequente (modo)
                                    cnpj_por_pn = (
                                        df_r.loc[mask_valid]
                                            .groupby('__pn_norm')['__cnpj_r']
                                            .agg(lambda s: s.value_counts().idxmax())
                                            .to_dict()
                                    )
                            # --- FIM NOVO ---

                            # --- NOVO: preparar filas (NOTAS RECEBIDAS) por PN e abrir workbook p/ gravar ---
                            from collections import defaultdict, deque
                            
                            wb_nr = load_workbook(notas_recebidas_path) if OPENPYXL_AVAILABLE else None
                            ws_nr = wb_nr.worksheets[0] if wb_nr else None
                            
                            # header=1 => cabeçalho na linha 2; dados começam na linha 3
                            OFFSET_NR = 3
                            # Coluna Excel (1-based) do "Valor" na planilha NOTAS RECEBIDAS:
                            VALOR_COL_XL = df_r.columns.get_loc(col_valor_r) + 1
                            
                            # Fila por PN com (row_excel, valor_restante)
                            filas_por_pn = defaultdict(deque)
                            for i, r in df_r.reset_index(drop=True).iterrows():
                                if float(r["__valor"]) > 0:
                                    excel_row = OFFSET_NR + i
                                    filas_por_pn[r["__pn_norm"]].append({"row": excel_row, "valor": float(r["__valor"])})
                            
                            rows_receb_atualizados = set()
                            rows_receb_zerados = set()
                            green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid") if OPENPYXL_AVAILABLE else None

                            # identificar a coluna de data no NOTAS RECEBIDAS
                            col_data_r = next((c for c in ['Data','DATA','Data Pagamento','Dt'] if c in df_r.columns), None)

                            # normaliza a coluna de data (se existir)
                            if col_data_r:
                                df_r['__data_r'] = pd.to_datetime(df_r[col_data_r], dayfirst=True, errors='coerce')
                            else:
                                df_r['__data_r'] = pd.NaT

                            # Fila por PN com (row_excel, valor_restante, data_receb)
                            filas_por_pn = defaultdict(deque)
                            for i, r in df_r.reset_index(drop=True).iterrows():
                                if float(r["__valor"]) > 0:
                                    excel_row = OFFSET_NR + i
                                    filas_por_pn[r["__pn_norm"]].append({
                                        "row": excel_row,
                                        "valor": float(r["__valor"]),
                                        "data": r["__data_r"]  # <- data do recebimento dessa linha
                                    })

                            def saldo_total_pn(pn_norm: str) -> float:
                                """Soma quanto ainda resta na fila desse PN em NOTAS RECEBIDAS."""
                                return sum(item["valor"] for item in filas_por_pn.get(pn_norm, []))
                            # --- FIM NOVO ---
                            
                            if df_r.empty:
                                print("ℹ️ NOTAS RECEBIDAS.xlsx não tem valores (>0). Nada a fazer.")
                            else:

                                totais_por_pn = df_r.groupby('__pn_norm')['__valor'].sum().to_dict()
                                txt_recebimentos = []
                                resumo_receb = []

                                # 5) Pagar notas de RECEITA em ordem do Nº NF, por participante
                                for pn_norm, grupo in df_receitas.groupby('__pn_norm', sort=False):
                                    disponivel = float(totais_por_pn.get(pn_norm, 0.0))
                                    pn_nome = str(grupo.iloc[0][col_pn])
                                    if disponivel <= 0:
                                        resumo_receb.append(f"• {pn_nome}: sem recebimentos. Nenhuma nota paga.")
                                        continue

                                    faltam = 0
                                    pagos = 0

                                    for idx, row in grupo.iterrows():
                                        valor_nf = float(row['valor_receita'])
                                        if saldo_total_pn(pn_norm) + 1e-9 >= valor_nf:
                                            # Consome da fila de NOTAS RECEBIDAS até cobrir o valor da NF
                                            restante = valor_nf
                                            # inicializa variáveis que serão preenchidas ao consumir a fila
                                            conta_cod_usada = ""
                                            dt_receb_usada = pd.NaT

                                            while restante > 1e-9 and filas_por_pn[pn_norm]:
                                                topo = filas_por_pn[pn_norm][0]
                                                usar = min(topo["valor"], restante)
                                                topo["valor"] -= usar
                                                restante -= usar

                                                if not conta_cod_usada:
                                                    conta_cod_usada = topo.get("conta_cod","")
                                                if pd.isna(dt_receb_usada):
                                                    dt_receb_usada = topo.get("data", pd.NaT)
                                                # Atualiza o valor da célula de "Valor" na NOTAS RECEBIDAS (zerando parcial/total)
                                                if OPENPYXL_AVAILABLE and ws_nr:
                                                    ws_nr.cell(row=topo["row"], column=VALOR_COL_XL).value = round(topo["valor"], 2)
                                                rows_receb_atualizados.add(topo["row"])

                                                # Se zerou esse recebimento, pinta B..I de VERDE e remove da fila
                                                if topo["valor"] <= 1e-9:
                                                    filas_por_pn[pn_norm].popleft()
                                                    rows_receb_zerados.add(topo["row"])
                                                    if OPENPYXL_AVAILABLE and ws_nr and green_fill:
                                                        for col in range(2, 10):  # B..I
                                                            ws_nr.cell(row=topo["row"], column=col).fill = green_fill

                                            # Marca a NF como paga no RELATORIO (mantém como já estava)
                                            pagos += 1
                                            linhas_receitas_pagas_idx.append(idx)

                                            # ====== GERAR LINHA DO TXT (RECEBIMENTOS) ======
                                            
                                            # Data e conta do(s) recebimento(s) usados: pega do PRIMEIRO item realmente utilizado
                                            dt_receb_usada = pd.NaT
                                            conta_cod_usada, conta_nome_usado = "", ""
                                            restante_tmp = valor_nf
                                            for item in filas_por_pn[pn_norm]:
                                                if restante_tmp <= 1e-9:
                                                    break
                                                usar = min(item["valor"], restante_tmp)
                                                if usar > 1e-9 and pd.isna(dt_receb_usada):
                                                    dt_receb_usada = item.get("data", pd.NaT)
                                                    conta_cod_usada = item.get("conta_cod", "") or ""
                                                    conta_nome_usado = item.get("conta_nome", "") or ""
                                                restante_tmp -= usar
                                            
                                            if pd.isna(dt_receb_usada):
                                                dt_receb_usada = pd.Timestamp.today()
                                            data_fmt = dt_receb_usada.strftime('%d-%m-%Y')
                                            
                                            # Fazenda → como já estava
                                            cod_faz = "000"
                                            if col_faz and pd.notna(row.get(col_faz, None)):
                                                cod = str(MAP_FAZENDAS.get(str(row[col_faz]).strip(), "0"))
                                                cod_faz = cod.zfill(3)
                                            
                                            # CNPJ → PN (do NOTAS RECEBIDAS) com fallback na RELATORIO (como você já fez)
                                            cnpj = cnpj_por_pn.get(pn_norm, "")
                                            if (not cnpj or cnpj == "0"*14) and col_cnpj:
                                                cnpj = "".join(ch for ch in str(row[col_cnpj]) if ch.isdigit()).zfill(14)
                                            
                                            # NF e descrição (inclui a CONTA mapeada no histórico para conferência)
                                            num_nf = str(row[col_nf]).strip()
                                            conta_info = f" [CONTA: {conta_nome_usado} — {conta_cod_usada}]" if conta_nome_usado else ""
                                            descricao = f"RECEBIMENTO NF {num_nf} {pn_nome}{conta_info}".upper()
                                            
                                            # valor em centavos com Decimal (evita cair R$ 1,00)
                                            from decimal import Decimal, ROUND_HALF_UP
                                            valor_cent = str(
                                                int(
                                                    (Decimal(str(valor_nf)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) * 100)
                                                    .to_integral_value(rounding=ROUND_HALF_UP)
                                                )
                                            )
                                            linha = [
                                                data_fmt,                 # 1  (data da NOTAS RECEBIDAS)
                                                cod_faz,                  # 2
                                                (conta_cod_usada or MAP_CONTAS.get("Não Mapeado","0000")),
                                                num_nf,                   # 4
                                                "1",                      # 5
                                                descricao,                # 6
                                                cnpj,                     # 7
                                                "1",                      # 8 (Receita)
                                                valor_cent,               # 9 (Entrada)
                                                "000",                    # 10
                                                valor_cent,               # 11 (Entrada)
                                                "N"                       # 12
                                            ]
                                            txt_recebimentos.append("|".join(linha))
                                            # ====== FIM GERAR LINHA ======

                                            # (deixe o restante do bloco igual: data_fmt, cod_faz, cnpj, descricao, valor_cent, txt_recebimentos.append...)
                                        else:
                                            faltam += 1

                                    restante_pn = saldo_total_pn(pn_norm)
                                    if restante_pn > 1e-6:
                                        resumo_receb.append(f"• {pn_nome}: pagas {pagos} nota(s), sobrou R$ {restante_pn:,.2f}.")
                                    else:
                                        if faltam > 0:
                                            resumo_receb.append(f"• {pn_nome}: pagas {pagos} nota(s), faltam {faltam} nota(s).")
                                        else:
                                            resumo_receb.append(f"• {pn_nome}: todas as notas pagas.")

                                # garantir que só escrevemos linhas se realmente houve marcação de pago no RELATORIO
                                if txt_recebimentos and not linhas_receitas_pagas_idx:
                                    txt_recebimentos = []

                                # 6) Gerar RECEBIMENTOS.txt
                                if txt_recebimentos:
                                    with open("RECEBIMENTOS.txt", "w", encoding="utf-8") as f:
                                        f.write("\n".join(txt_recebimentos))
                                    print(f"✅ Arquivo TXT gerado com {len(txt_recebimentos)} recebimento(s)")
                                else:
                                    print("ℹ️ Nenhum recebimento gerado (valores insuficientes).")

                                # 6.1) NOVO — Persistir alterações na planilha NOTAS RECEBIDAS
                                if OPENPYXL_AVAILABLE and wb_nr and (rows_receb_atualizados or rows_receb_zerados):
                                    try:
                                        wb_nr.save(notas_recebidas_path)
                                        print(f"✅ NOTAS RECEBIDAS atualizada: {len(rows_receb_atualizados)} linha(s) alterada(s); "
                                              f"{len(rows_receb_zerados)} zerada(s) e destacada(s).")
                                    except Exception as e:
                                        print(f"⚠️ Falha ao salvar NOTAS RECEBIDAS atualizada: {e}")

                                # 7) Marcar em VERDE (C–Q) as linhas de RECEITA pagas (sem mexer nas DESPESAS)
                                if OPENPYXL_AVAILABLE and linhas_receitas_pagas_idx:
                                    try:
                                        wb = load_workbook(testes_path)
                                        ws = wb['RELATORIO']
                                        green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
                                        deslocamento = 7            # mesmo offset do resto do script
                                        col_inicio, col_fim = 3, 17 # C..Q
                                        for idx in linhas_receitas_pagas_idx:
                                            row_excel = deslocamento + idx
                                            for col in range(col_inicio, col_fim + 1):
                                                ws.cell(row=row_excel, column=col).fill = green_fill
                                        wb.save(testes_path)
                                        print(f"✅ {len(linhas_receitas_pagas_idx)} linha(s) de RECEITA marcadas em verde.")
                                    except Exception as e:
                                        print(f"⚠️ Falha ao marcar RECEITAS em verde: {e}")

                                # 8) Resumo por participante
                                if resumo_receb:
                                    print("\nResumo RECEBIMENTOS por participante:")
                                    for msg in resumo_receb:
                                        print(msg)

    except Exception as e:
        print(f"❌ Erro no bloco de RECEBIMENTOS: {str(e)}")

    # =====================================================================
    # MARCAR LINHAS PAGAS NA PLANILHA ORIGINAL (APENAS COLUNAS C-Q)
    # =====================================================================
    if OPENPYXL_AVAILABLE and linhas_pagas_idx:
        try:
            print("\nMarcando notas pagas na planilha original (colunas C-Q)...")
            
            # Carregar workbook original
            wb_original = load_workbook(testes_path)
            ws_original = wb_original['RELATORIO']
            
            # Definir cor de fundo verde
            green_fill = PatternFill(start_color="C6EFCE", 
                                    end_color="C6EFCE", 
                                    fill_type="solid")
            
            # Calcular deslocamento (cabeçalho começa na linha 5)
            deslocamento = 7  # Dados começam na linha 6
            
            # Definir intervalo de colunas (C=3, Q=17)
            col_inicio = 3
            col_fim = 17
            
            # Marcar apenas colunas C-Q para as linhas pagas
            for idx in linhas_pagas_idx:
                row_idx = deslocamento + idx
                for col in range(col_inicio, col_fim + 1):
                    ws_original.cell(row=row_idx, column=col).fill = green_fill
            
            # Salvar alterações na planilha original (sem criar nova)
            wb_original.save(testes_path)
            print(f"✅ Planilha original atualizada com notas pagas destacadas: {testes_path}")
            print(f"   - {len(linhas_pagas_idx)} notas marcadas como pagas (colunas C-Q)")
            
        except Exception as e:
            print(f"⚠️ Erro ao marcar notas pagas: {str(e)}")
    elif not OPENPYXL_AVAILABLE:
        print("⚠️ Openpyxl não disponível - não foi possível destacar notas pagas")
    elif not linhas_pagas_idx:
        print("⚠️ Nenhuma nota paga para destacar")

    # =====================================================================
    # MARCAR PRODUTOS ESPECIAIS NA PLANILHA ORIGINAL (COLUNAS C-Q)
    # =====================================================================
    # Logo depois de "linhas_pagas_idx", adicione:
    
    # 1) Recarregar workbook e aba
    wb_original = load_workbook(testes_path)
    ws_original = wb_original['RELATORIO']
    
    # 2) Definir preenchimento verde
    green_fill = PatternFill(start_color="C6EFCE",
                             end_color="C6EFCE",
                             fill_type="solid")
    
    # 3) Parâmetros de deslocamento de linha/coluna
    deslocamento = 7   # dados começam na linha 6 (porque header=5)
    col_inicio, col_fim = 3, 17  # colunas C (3) até Q (17)
    
    # 4) Índices a destacar
    #   a) Notas pagas
    idx_pagas = set(linhas_pagas_idx)
    
    #   b) Todos os produtos especiais (qualquer substring em PRODUTO)
    pattern = '|'.join(PRODUTOS_ESPECIAIS)

    idx_especiais = set(df_despesas.loc[df_despesas['produto_especial']].index)
    
    #   c) União de ambos
    idx_para_destacar = idx_pagas.union(idx_especiais)
    
    # 5) Aplicar cor em cada célula (C–Q) de cada linha
    for idx in idx_para_destacar:
        row_excel = deslocamento + idx
        for col in range(col_inicio, col_fim + 1):
            ws_original.cell(row=row_excel, column=col).fill = green_fill
    
    # 6) Salvar alterações
    wb_original.save(testes_path)
    print(f"✅ Destacadas {len(idx_para_destacar)} linhas (pagas + especiais)")
    print("\n✅ Processo concluído com sucesso!")

except Exception as e:
    print(f"❌ Erro ao salvar resultados: {str(e)}")