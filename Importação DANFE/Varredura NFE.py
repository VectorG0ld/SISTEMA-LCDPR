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
    "Caixa Geral": "1255",
    "Cheques a Compensar": "1255",
    "Fundo Fixo - Gildevan": "1255",
    "Fundo Fixo - Cleidson Alves": "1255",
    "Fundo Fixo - Rodrigo": "1255",
    "Fundo Fixo - Wandres": "1255",
    "Fundo Fixo - Cezar Dias": "1255",
    "Fundo Fixo - Geraldo": "1255",
    "Fundo Fixo - Daniel": "1255",
    "Fundo Fixo - Hadlaim": "1255",
    "Fundo Fixo - Lourival": "1255",
    "Fundo Fixo - Rogeris": "1255",
    "Fundo Fixo - Joaquim": "1255",
    "Caixa Dobrado": "1255",
    "Fundo Fxo - Douglas": "1255",
    "Fundo Fixo - Samuel": "1255",
    "Fundo Fixo - Adarildo": "1255",
    "Fundo Fixo - Fabricio": "1255",
    "Fundo Fixo - Fernando": "1255",
    "Fundo Fixo - Orivan": "1255",
    "Fundo Fixo - Saimon": "1255",
    "Fundo Fxo - Eduardo": "1255",
    "Fundo Fixo - Melquiades": "1255",
    "Fundo Fixo - Anivaldo": "1255",
    "Fundo Fixo - Cida": "1255",
    "Caixa Dobrado - Cobrança": "1255",
    "Fundo Fixo - Neto": "1255",
    "Conta Rotative Gilson": "1255",
    "Fundo Fixo - Osvaldo": "1255",
    "Fundo Fixo - Cleto Zanatta": "1255",
    "Fundo Fixo - Edison": "1255",
    "Fundo Fixo - Phelipe": "1255",
    "Caixa Deposito": "1255",
    "Fundo Fixo - Valdivino": "1255",
    "Fundo Fixo - Jose Domingos": "1255",
    "Fudo Fixo - Stenyo": "1255",
    "Fundo Fixo - Marcos": "1255",
    "Fundo Fixo - ONR": "1255",
    "Fundo Fixo - Marcelo Dutra": "1255",
    "Fundo Fixo - Gustavo": "1255",
    "Fundo Fixo - Delimar": "1255",
    "Caixa Contábil": "1255",
    "Banco Sicoob_Frutacc_597": "1255",
    "Banco Bradesco_Frutacc_28.751": "1255",
    "Banco do Brasil_Gilson_21252": "1255",
    "Banco do Brasil_Cleuber_24585": "1260",
    "Banco da Amazonia_Cleuber_34472": "1255",
    "Caixa Economica_Cleuber_20573": "1255",
    "Caixa Economica_Adriana_20590": "1255",
    "Banco Bradesco_Cleuber_22102": "1257",
    "Banco Bradesco_Gilson_27014": "1255",
    "Banco Bradesco_Adriana_29260": "1255",
    "Banco Bradesco_Lucas 29620": "1255",
    "Banco Itau_Gilson_26059": "1255",
    "Banco Sicoob_Cleuber_052": "1256",
    "Banco Sicoob_Gilson_781": "1255",
    "Caixa Economica_Cleuber_25766": "1255",
    "Banco Santander_Cleuber_1008472": "1255",
    "Banco Sicredi_Cleuber_36120": "1255",
    "Banco Sicredi_Gilson_39644": "1255",
    "Banco Itau_Cleuber_63206": "1255",
    "Banco Sicoob_Cleuber_81934": "1256",
    "Caixa Economica_Cleuber_20177": "1255",
    "Banco Itau_Frutacc_16900": "1255",
    "Banco Sicredi_Anne_27012": "1255",
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
PRODUTOS_ESPECIAIS = ["GASOLINA", "OLEO DIESEL", "DIESEL", "ETANOL", "MOBILGREASE"]

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

print("\nAssociando pagamentos usando datas de vencimento...")
for i, nota in df_to_process.iterrows():
    # Criar cópia da linha original
    result_row = nota.to_dict()
    
    # === [SUBSTITUIR TODO O BLOCO A PARTIR DAQUI] ===
    # CAMADA 1: NF + CNPJ + não cancelado + não associada
    data_nota = nota['data_nota']
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
    # respeitando a regra do N° Primário se a coluna existir.
    if cands.empty:
        grupo_nf = df_base.loc[
            (df_base['num_nf'] == nota['num_nf_busca']) &
            (~df_base['associada'])
        ].copy()
    
        if 'num_primario' in grupo_nf.columns:
            primarios_cancelados = set(
                grupo_nf.loc[grupo_nf['pagamento_cancelado'] == 'SIM', 'num_primario']
                        .dropna().astype(str).unique().tolist()
            )
            cands = grupo_nf.loc[
                (grupo_nf['pagamento_cancelado'] != 'SIM') &
                (~grupo_nf['num_primario'].astype(str).isin(primarios_cancelados))
            ].copy()
        else:
            cands = grupo_nf.loc[(grupo_nf['pagamento_cancelado'] != 'SIM')].copy()
    
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
        # Preferir data de vencimento igual à data da nota
        if not pd.isna(data_nota):
            cands['score'] += (cands['data_vencimento'].dt.date == data_nota.date()).astype(float) * 1.5
        # Aproximação por valor
        cands['diff_val'] = (cands['valor'] - float(nota['valor_busca'])).abs()
        cands['score'] += (np.isclose(cands['valor'], nota['valor_busca'], atol=0.01)).astype(float) * 1.0
        cands['score'] -= (cands['diff_val'] > 5.0).astype(float) * 0.5
        # Se veio da camada 3, mantenha a similaridade influenciando
        if 'sim_nome' in cands.columns:
            cands['score'] += cands['sim_nome']

        # Preferir quem tem DATA DE PAGAMENTO preenchida
        cands['score'] += (~cands['data_pagamento'].isna()).astype(float) * 1.0

        # Preferir quem tem CONTA (banco) preenchida
        cands['score'] += (cands['banco'].astype(str).str.strip() != '').astype(float) * 1.0

        # (opcional) leve preferência se a data do pagamento = data da nota
        if not pd.isna(data_nota):
            cands['score'] += (cands['data_pagamento'].dt.date == data_nota.date()).astype(float) * 0.5


        cands = cands.sort_values(['score', 'diff_val', 'data_pagamento'],
                          ascending=[False, True, False])

        idx_sel = cands.index[0]
        parcela_encontrada = cands.loc[idx_sel]
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
            cod_banco = MAP_CONTAS.get(banco_nome, MAP_CONTAS["Não Mapeado"])
            
            # >>> NOVO: origem da associação (mais informativa)
            origem = (
                "Associada por CNPJ"
                if str(parcela_encontrada.get('cnpj', '')) == str(nota['cnpj_busca'])
                else "Associada por NF"
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
                
                # descrição padronizada
                descricao = f"PAGAMENTO NF {num_nf} {fornecedor}".upper()
                
                # parcela (ajuste se quiser numeração real)
                parcela_txt = "1"
                
                txt_line = [
                    data_fmt,       # 01-01-2025
                    cod_fazenda3,   # 006
                    "001",          # fixo
                    num_nf,         # 14209
                    parcela_txt,    # 1
                    descricao,      # PAGAMENTO NF 14209 ...
                    cnpj,           # 01608488001250
                    "2",            # fixo
                    "000",          # fixo
                    valor_cent,     # 573500
                    valor_cent,     # 573500
                    "N"             # fixo
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
            'Banco': "1255",  # Banco fixo para produtos especiais
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
        valor_cent = str(int(round(float(nota['valor_busca']) * 100)))
        descricao = f"PAGAMENTO NF {num_nf} {fornecedor}".upper()
        parcela_txt = "1"
        
        txt_line = [
            data_fmt,
            cod_fazenda3,
            "001",
            num_nf,
            parcela_txt,
            descricao,
            cnpj,
            "2",
            "000",
            valor_cent,
            valor_cent,
            "N"
        ]
        txt_lines.append("|".join(txt_line))
        pagamentos_associados += 1
        
        # Registrar como linha paga
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
    cod_banco = MAP_CONTAS.get(banco_nome, MAP_CONTAS["Não Mapeado"])
    
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
    descricao = f"PAGAMENTO NF {num_nf} {fornecedor}".upper()
    parcela_txt = "1"

    txt_line = [
        data_fmt, cod_fazenda3, "001", num_nf, parcela_txt,
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
    
    # Salvar arquivo TXT
    if txt_lines:
        with open("PAGAMENTOS.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(txt_lines))
        print(f"✅ Arquivo TXT gerado com {len(txt_lines)} pagamentos válidos")
    else:
        print("⚠️  Nenhum pagamento válido para gerar TXT")
    
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