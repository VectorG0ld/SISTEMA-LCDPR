# -*- coding: utf-8 -*-
"""
AUTOMAÇÃO FOLHA – ESTILO IMPORTADOR (mesmo do código anexado)
--------------------------------------------------------------
• UI: mesmo tema (linhas azuis só onde necessário), cabeçalho com ❓ Ajuda, ⚙️ Configurar e ✖ Fechar,
  cartões de Controles/Status, Log com layout idêntico e botão ⛔ Cancelar.
• Período compacto: Início (MM/AAAA) → Fim (MM/AAAA).
• Lê planilha (C1 = MM/AAAA; A5.. = info do imóvel/fazenda; B5.. CPF; C5.. Nome; D5.. Líquido; F3/G3/H3 = INSS/IRRF/FGTS).
• Gera 1 TXT único (layout 12 colunas). Conta = "001" fixa. IMÓVEL = nome extraído da coluna A (ex.: "FAZENDA ALIANÇA").
• Funcionários: data do 5º dia útil. Tributos (INSS/IRRF/FGTS): data do 20º dia útil.
• Evita duplicidade por (CPF + Data).
• Cancelável via QThread.

Dependências:
- PySide6
- xlwings (opcional; recalcula fórmulas). Sem xlwings, usa openpyxl (lê valores cacheados).
"""

from __future__ import annotations

import os, re, json, time, calendar, traceback
from pathlib import Path
from datetime import datetime, date, timedelta
from collections import Counter

from PySide6.QtCore import Qt, QThread, Signal, QRegularExpression
from PySide6.QtGui import QFont, QTextCursor, QPixmap, QColor, QTextOption, QRegularExpressionValidator, QIntValidator
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFrame, QLabel, QPushButton, QTextEdit,
    QFileDialog, QMessageBox, QComboBox, QToolButton, QTabWidget, QDialog,
    QDialogButtonBox, QFormLayout, QLineEdit, QSizePolicy, QGroupBox, QListView
)
import unicodedata

# ============================
# Estilo (igual ao anexo)
# ============================
ICON_PATH = Path(__file__).parent / "image" / "logo.png"

STYLE_SHEET = """
QMainWindow, QWidget { background-color: #1B1D1E; font-family: 'Segoe UI', Arial, sans-serif; color: #E0E0E0; }
QLineEdit, QDateEdit, QComboBox, QTextEdit { color: #E0E0E0; background-color: #2B2F31; border: 1px solid #1e5a9c; border-radius: 6px; padding: 6px; }
QLineEdit::placeholder { color: #5A5A5A; }
QPushButton { background-color: #1e5a9c; color: #FFFFFF; border: none; border-radius: 6px; padding: 8px 16px; font-weight: bold; }
QPushButton:hover, QPushButton:pressed { background-color: #002a54; }
QPushButton#danger { background-color: #C0392B; }
QPushButton#danger:hover { background-color: #E74C3C; }
QPushButton#success { background-color: #27AE60; }
QPushButton#success:hover { background-color: #2ECC71; }
QGroupBox { border: 1px solid #11398a; border-radius: 6px; margin-top: 10px; font-weight: bold; background-color: #0d1b3d; }
QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px; color: #ffffff; }
QTableWidget { background-color: #222426; color: #E0E0E0; border: 1px solid #1e5a9c; border-radius: 4px; gridline-color: #3A3C3D; alternate-background-color: #2A2C2D; }
QHeaderView::section { background-color: #1e5a9c; color: #FFFFFF; padding: 6px; border: none; }
QTabWidget::pane { border: 1px solid #1e5a9c; border-radius: 4px; background: #212425; margin-top: 5px; }
QTabBar::tab { background: #2A2C2D; color: #E0E0E0; padding: 8px 16px; border: 1px solid #1e5a9c; border-top-left-radius: 4px; border-top-right-radius: 4px; margin-right: 2px; }
QTabBar::tab:selected { background: #1e5a9c; color: #FFFFFF; border-bottom: 2px solid #002a54; }
QStatusBar { background-color: #212425; color: #7F7F7F; border-top: 1px solid #1e5a9c; }
/* ===== Diálogo de Configurações ===== */
QWidget#tab_config QGroupBox {
    background: transparent;
    border: 1px solid #11398a;
    border-radius: 6px;
    margin-top: 14px;
}
QWidget#tab_config QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 0 6px;
    background-color: #1B1D1E;
    color: #ffffff;
}
QWidget#tab_config QLabel { border: none; background: transparent; }
QWidget#tab_config QFrame,
QWidget#tab_config QFrame#card,
QWidget#tab_config QFrame.card {
    background: transparent;
    border: 1px solid #11398a;
    border-radius: 6px;
}
/* ===== Somente no diálogo de Configurar (objectName=tab_config) ===== */
QWidget#tab_config QGroupBox {
    background: transparent;          /* sem fundo azul */
    border: 1px solid #11398a;        /* só a linha azul */
    border-radius: 6px;
    margin-top: 14px;                 /* espaço para o título do groupbox */
}

QWidget#tab_config QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 0 6px;
    background-color: #1B1D1E;        /* mesma cor do fundo do diálogo */
    color: #ffffff;
}

/* (no dialog Configurar) */
QWidget#tab_config QFrame {            /* todos os frames: sem borda */
    background: transparent;
    border: none;
}
QWidget#tab_config QFrame#card,        /* apenas o card principal com a borda azul */
QWidget#tab_config QFrame.card {
    background: transparent;
    border: 1px solid #11398a;
    border-radius: 6px;
}

/* Labels do Configurar sem borda/fundo */
QWidget#tab_config QLabel {
    border: none;
    background: transparent;
}
/* Evita que textos forcem largura mínima absurda */
QLabel, QTextEdit { min-width: 0; }

/* Log mais dócil: nada de barra horizontal */
#logCard QTextEdit {
    /* sem barra horizontal */
    qproperty-horizontalScrollBarPolicy: 1; /* Qt::ScrollBarAlwaysOff */
}
/* ===== Combos: popup consistente e sem “explodir” tamanho ===== */
QComboBox { combobox-popup: 0; }  /* usa popup rolável */
QComboBox QAbstractItemView {
    background-color: #2B2F31;
    color: #E0E0E0;
    border: 1px solid #1e5a9c;
    outline: none;
}
QComboBox QAbstractItemView::item {
    padding: 6px 10px;
}
QComboBox QAbstractItemView::item:selected {
    background: #1e5a9c;
    color: #FFFFFF;
}

"""

# ===== Mapeamentos de IMÓVEL por TITULAR (coluna A5..) =====
IMOVEL_MAP = {
    "CLEUBER": {
        "FAZENDA FRUTACC": "001",
        "FAZENDA UNIÃO": "002",
        "FAZENDA L3": "003",
        "FAZENDA PRIMAVERA": "004",
        "FAZENDA ALIANÇA": "005",
        "ARMAZEM PRIMAVERA": "006",
        "FAZENDA BARRAGEM GRANDE": "007",
        "FAZENDA ESTRELA": "008",
        "FAZENDA GUARA": "009",
    },
    "ADRIANA": {
        "FAZENDA POUSO DA ANTA": "001",
    },
    "GILSON": {
        "FAZENDA FORMIGA": "001",
    },
    "LUCAS": {
        "FAZENDA ALIANÇA 2": "001",
    },
}

# ===== CNPJs dos TRIBUTOS (usar apenas dígitos) =====
TRIBUTOS_CNPJ = {
    "INSS": re.sub(r"\D", "", "00.394.460/0058-87"),  # 00394460005887
    "IRRF": re.sub(r"\D", "", "29.979.036/0001-40"),  # 29979036000140
    "FGTS": re.sub(r"\D", "", "37.115.367/0001-60"),  # 37115367000160
}

# ============================
# Helpers
# ============================
def _digits(s) -> str:
    return re.sub(r"\D", "", str(s or ""))

def _to_cent(valor) -> str:
    """Converte para centavos sem pontuação. Aceita 123,45 | 123.45 | 'R$ 123,45' | float | int."""
    try:
        s = str(valor).strip().replace("R$", "").replace(" ", "")
        if not s:
            return "0"
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s and "." not in s:
            s = s.replace(",", ".")
        return str(int(round(float(s) * 100)))
    except Exception:
        return "0"

def _fmt_dd_mm_yyyy(d: date) -> str:
    return d.strftime("%d-%m-%Y")

def _next_business_day(d: date) -> date:
    # sábado → segunda, domingo → segunda
    if d.weekday() == 5:  # sat
        return d + timedelta(days=2)
    if d.weekday() == 6:  # sun
        return d + timedelta(days=1)
    return d

def _dia_ajustado(ano: int, mes: int, dia: int) -> date:
    last_day = calendar.monthrange(ano, mes)[1]
    dia = min(dia, last_day)
    return _next_business_day(date(ano, mes, dia))

def _iter_mes_ano(inicio_mm_aaaa: str, fim_mm_aaaa: str):
    im, ia = [int(x) for x in inicio_mm_aaaa.split("/")]
    fm, fa = [int(x) for x in fim_mm_aaaa.split("/")]
    y, m = ia, im
    while (y < fa) or (y == fa and m <= fm):
        yield (m, y)
        m += 1
        if m > 12:
            m = 1
            y += 1

def _make_line(data_br, imovel, conta, num_doc, tipo_doc, historico, cpf, tipo_lanc, cent_ent, cent_sai, cent_saldo, nat) -> str:
    return "|".join([
        data_br, imovel, conta, num_doc, tipo_doc,
        historico, cpf, tipo_lanc, cent_ent, cent_sai, cent_saldo, nat
    ])

def _extract_imovel_name(texto_a: str) -> str:
    """
    A célula A pode vir como "4 - CLEUBER MARCOS DE OLIVEIRA FAZ ALIANÇA" ou conter
    'FAZENDA <NOME>', 'FAZ <NOME>', 'SÍTIO/SITIO <NOME>', 'ARMAZÉM/ARMAZEM <NOME>'.
    Retorna um nome de imóvel padronizado, preferindo o prefixo 'FAZENDA '.
    """
    if not texto_a:
        return ""
    t = str(texto_a).upper()

    # Padrões "FAZENDA XYZ"
    m = re.search(r"(FAZENDA\s+[A-Z0-9ÇÃÕÁÉÍÓÚÂÊÔ ]+)", t)
    if m:
        return m.group(1).strip()

    # Padrões "FAZ XYZ" -> normaliza pra "FAZENDA XYZ"
    m = re.search(r"\bFAZ[\.\s]+([A-Z0-9ÇÃÕÁÉÍÓÚÂÊÔ ]+)", t)
    if m:
        return f"FAZENDA {m.group(1).strip()}"

    # SÍTIO / CHÁCARA / ARMAZÉM
    for prefix in ("SÍTIO", "SITIO", "CHÁCARA", "CHACARA", "ARMAZÉM", "ARMAZEM"):
        m = re.search(rf"({prefix}\s+[A-Z0-9ÇÃÕÁÉÍÓÚÂÊÔ ]+)", t)
        if m:
            return m.group(1).strip()

    # Fallback: tenta pegar o trecho após o hífen
    m = re.search(r"\-\s*([A-Z0-9ÇÃÕÁÉÍÓÚÂÊÔ ]+)", t)
    if m:
        # Se achar "FAZ ALGO", normaliza…
        val = m.group(1).strip()
        if val.startswith("FAZ "):
            return "FAZENDA " + val[4:].strip()
        return val
    return "FAZENDA"

def _mode_or_default(items, default="FAZENDA"):
    try:
        c = Counter([x for x in items if (x and x.strip())])
        if not c:
            return default
        return c.most_common(1)[0][0]
    except Exception:
        return default

def _norm(s: str) -> str:
    """Remove acentos e força UPPER para comparação robusta."""
    s = str(s or "")
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ASCII", "ignore").decode("ASCII")
    return s.upper().strip()

def _owner_from_text(a_txt: str) -> str | None:
    """Detecta o titular (CLEUBER/ADRIANA/GILSON/LUCAS) no texto da coluna A."""
    TITS = ("CLEUBER", "ADRIANA", "GILSON", "LUCAS")
    t = _norm(a_txt)
    for tt in TITS:
        if tt in t:
            return tt
    return None

def _cod_imovel_from_colA(a_txt: str) -> str:
    """
    Lê a coluna A (A5..), identifica o TITULAR e procura um dos nomes de fazenda
    do respectivo mapeamento; retorna o CÓDIGO (ex.: '005').
    Se não encontrar nada, retorna '001'.
    """
    t = _norm(a_txt)
    owner = _owner_from_text(t) or ""  # pode ser vazio
    # 1) tenta no mapa do titular detectado
    if owner and owner in IMOVEL_MAP:
        for faz, code in IMOVEL_MAP[owner].items():
            if _norm(faz) in t:
                return code
    # 2) fallback: procura em todos os mapas (caso A não traga o nome do titular)
    for mp in IMOVEL_MAP.values():
        for faz, code in mp.items():
            if _norm(faz) in t:
                return code
    # 3) fallback final
    return "001"

# ============================
# Leitura da planilha (xlwings → openpyxl)
# ============================
def _read_sheet_with_xlwings(filepath: str, mes: int, ano: int):
    try:
        import xlwings as xw
    except Exception as e:
        raise RuntimeError("xlwings não disponível. Instale xlwings para recálculo automático.") from e

    app = None
    wb = None

    try:
        app = xw.App(visible=False, add_book=False)
        wb = app.books.open(filepath, update_links=False, read_only=False)
        sh = wb.sheets[0]

        # Seleciona o mês na C1 (MM/AAAA)
        sh.range("C1").value = f"{mes:02d}/{ano}"
        app.api.Calculation = -4105  # Automatic
        wb.api.CalculateFullRebuild()
        time.sleep(0.2)

        inss = _to_cent(sh.range("F3").value or 0)
        irrf = _to_cent(sh.range("G3").value or 0)
        fgts = _to_cent(sh.range("H3").value or 0)

        funcionarios, imoveis = [], []
        r = 5
        while True:
            a_txt = str(sh.range(f"A{r}").value or "").strip()
            cpf   = _digits(sh.range(f"B{r}").value or "")
            nome  = (sh.range(f"C{r}").value or "").strip()
            val   = sh.range(f"D{r}").value
            if not (a_txt or cpf or nome or val):
                break

            cod_imovel = _cod_imovel_from_colA(a_txt)
            if cod_imovel:
                imoveis.append(cod_imovel)

            if cpf and nome and (val is not None and str(val).strip() != ""):
                funcionarios.append({
                    "cpf": cpf,
                    "nome": nome,
                    "centavos": _to_cent(val),
                    "imovel": cod_imovel
                })
            r += 1

        imovel_tributos = _mode_or_default(imoveis, default="001")
        return funcionarios, {"INSS": inss, "IRRF": irrf, "FGTS": fgts}, imovel_tributos
    finally:
        try:
            if wb:
                wb.close()
        except Exception:
            pass
        try:
            if app:
                app.quit()
        except Exception:
            pass

def _read_sheet_with_openpyxl(filepath: str, mes: int, ano: int):
    from openpyxl import load_workbook
    wb = load_workbook(filepath, data_only=True)
    ws = wb.active

    # Tenta setar C1; sem recálculo
    try:
        ws["C1"].value = f"{mes:02d}/{ano}"
    except Exception:
        pass

    def cell(addr):
        v = ws[addr].value
        return v if v is not None else 0

    inss = _to_cent(cell("F3"))
    irrf = _to_cent(cell("G3"))
    fgts = _to_cent(cell("H3"))

    funcionarios, imoveis = [], []
    r = 5
    while True:
        a_txt = str(ws[f"A{r}"].value or "").strip()
        cpf   = _digits(ws[f"B{r}"].value or "")
        nome  = str(ws[f"C{r}"].value or "").strip()
        val   = ws[f"D{r}"].value
        if not (a_txt or cpf or nome or val):
            break

        cod_imovel = _cod_imovel_from_colA(a_txt)
        if cod_imovel:
            imoveis.append(cod_imovel)

        funcionarios.append({
            "cpf": cpf, "nome": nome,
            "centavos": _to_cent(val),
            "imovel": cod_imovel  # <- usa o CÓDIGO
        })
        r += 1

    imovel_tributos = _mode_or_default(imoveis, default="001")  # códigos

    return funcionarios, {"INSS": inss, "IRRF": irrf, "FGTS": fgts}, imovel_tributos

def _read_planilha(filepath: str, mes: int, ano: int):
    try:
        return _read_sheet_with_xlwings(filepath, mes, ano)
    except Exception:
        return _read_sheet_with_openpyxl(filepath, mes, ano)

# ============================
# Worker (QThread) – Geração do TXT com Cancelar
# ============================
class FolhaWorker(QThread):
    log_sig = Signal(str, str)               # (mensagem, tipo)
    stats_sig = Signal(int, int, int)        # total, ok, err
    finished_sig = Signal(str, str)          # status, caminho_txt (ou "")

    def __init__(self, planilha: str, inicio: str, fim: str, parent=None):
        super().__init__(parent)
        self.planilha = planilha
        self.inicio = inicio
        self.fim = fim
        self._cancel = False
        self.total = 0
        self.ok = 0
        self.err = 0

        # Constantes do layout
        self.COD_CONTA = "001"
        self._vistos = set()  # (cpf, data_br)
        self._linhas = []

    def cancel(self):
        self._cancel = True

    def _emit(self, msg: str, kind: str = "info"):
        self.log_sig.emit(msg, kind)

    def _emit_stats(self):
        self.stats_sig.emit(self.total, self.ok, self.err)

    def run(self):
        try:
            # Vamos contar total estimado como "meses * (func + 3 tributos)"
            # Atualiza dinamicamente por mês.
            linhas = []
            self._emit("Iniciando leitura da planilha…", "title")

            for m, y in _iter_mes_ano(self.inicio, self.fim):
                if self._cancel: 
                    self._emit("Processo cancelado.", "warning")
                    self.finished_sig.emit("Cancelado", "")
                    return

                # Lê mês/ano
                try:
                    funcs, trib, imovel_trib = _read_planilha(self.planilha, m, y)
                    self._emit(f"Leitura {m:02d}/{y}: {len(funcs)} funcionários – INSS={trib['INSS']} IRRF={trib['IRRF']} FGTS={trib['FGTS']}", "info")
                except Exception as e:
                    self.err += 1
                    self._emit_stats()
                    self._emit(f"Erro ao ler {m:02d}/{y}: {e}", "error")
                    continue


                data_func = _dia_ajustado(y, m, 5)
                data_trib = _dia_ajustado(y, m, 20)
                data_func_br = _fmt_dd_mm_yyyy(data_func)
                data_trib_br = _fmt_dd_mm_yyyy(data_trib)

                # Funcionários
                for f in funcs:
                    if self._cancel:
                        self._emit("Cancelado pelo usuário.", "warning")
                        self.finished_sig.emit("Cancelado", "")
                        return
                    cpf = _digits(f.get("cpf")); nome = (f.get("nome") or "").strip()
                    cents = str(f.get("centavos") or "0")
                    imovel = (f.get("imovel") or "001").strip()  # código do imóvel

                    if not cpf or not nome or cents in ("", "0"):
                        continue

                    key = (cpf, data_func_br)
                    if key in self._vistos:
                        self._emit(f"↩️ DUP ignorado: {cpf} {data_func_br}", "warning")
                        continue
                    self._vistos.add(key)

                    historico = f"FOLHA DE PAGAMENTO REF. {m:02d}/{y} ({nome})"
                    linha = _make_line(
                        data_func_br, imovel, self.COD_CONTA,
                        "N", "1", historico, cpf, "2", "000", cents, cents, "N"
                    )
                    linhas.append(linha)
                    self.ok += 1; self._emit_stats()

                # Tributos únicos do mês
                for rotulo, cents in (("INSS", trib.get("INSS","0")), ("IRRF", trib.get("IRRF","0")), ("FGTS", trib.get("FGTS","0"))):
                    if self._cancel:
                        self._emit("Cancelado pelo usuário.", "warning")
                        self.finished_sig.emit("Cancelado", "")
                        return
                    if not cents or str(cents) == "0":
                        continue
                    historico = f"FOLHA DE PAGAMENTO REF. {m:02d}/{y} {rotulo}"
                    cnpj = TRIBUTOS_CNPJ.get(rotulo, "")
                    linha = _make_line(
                        data_trib_br, imovel_trib or "001", self.COD_CONTA,
                        "N", "1", historico, cnpj, "2", "000", str(cents), str(cents), "N"
                    )

                    linhas.append(linha)
                    self.ok += 1; self._emit_stats()

            if not linhas:
                self._emit("Nenhuma linha para salvar.", "warning")
                self.finished_sig.emit("Vazio", "")
                return

            out_dir = Path(self.planilha).parent
            fname = out_dir / f"folha_{self.inicio.replace('/','-')}_a_{self.fim.replace('/','-')}.txt"
            with open(fname, "w", encoding="utf-8") as f:
                f.write("\n".join(linhas))

            self._emit(f"TXT gerado: {fname}", "success")
            self.finished_sig.emit("Concluído", str(fname))
        except Exception:
            self.err += 1
            self._emit_stats()
            self._emit(f"Erro inesperado:\n{traceback.format_exc()}", "error")
            self.finished_sig.emit("Erro", "")

# ============================
# Diálogo de Configurar
# ============================
class ConfigDialog(QDialog):
    def __init__(self, cfg: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("⚙️ Configurações — Automação Folha")
        self.setModal(True)
        self.setStyleSheet(STYLE_SHEET)
        self.setFixedWidth(640)
        self.setObjectName("tab_config")  # aplica o CSS acima somente neste diálogo


        self._cfg = dict(cfg or {})
        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(10)

        # Card com borda azul (linhas só onde necessário), igual Talão de Energia
        card = QFrame(self)
        card.setObjectName("card")
        card.setStyleSheet("#card{border:1px solid #1e5a9c; border-radius:12px;}")
        card_lay = QVBoxLayout(card)
        card_lay.setContentsMargins(12, 10, 12, 12)
        card_lay.setSpacing(8)

        grp = QGroupBox("Caminhos e Opções", card)
        grp_lay = QFormLayout(grp)
        grp_lay.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        grp_lay.setLabelAlignment(Qt.AlignLeft)
        grp_lay.setFormAlignment(Qt.AlignLeft | Qt.AlignTop)
        grp_lay.setContentsMargins(10, 12, 10, 10)
        grp_lay.setHorizontalSpacing(10)
        grp_lay.setVerticalSpacing(10)

        # Campo: Planilha de Folha
        self.ed_planilha = QLineEdit(self._cfg.get("folha_xlsx", ""))
        btn_browse = QPushButton("Procurar…")
        def _choose():
            fn, _ = QFileDialog.getOpenFileName(self, "Selecionar Planilha de Folha", "", "Planilhas (*.xlsx *.xlsm)")
            if fn:
                self.ed_planilha.setText(fn)
        btn_browse.clicked.connect(_choose)

        row = QHBoxLayout()
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(8)
        row.addWidget(self.ed_planilha, 1)
        row.addWidget(btn_browse, 0)

        row_w = QFrame(grp)
        row_w.setLayout(row)
        grp_lay.addRow("Planilha de Folha:", row_w)

        card_lay.addWidget(grp)
        root.addWidget(card)

        # Botões (Save/Cancel) alinhados à direita — mesmo padrão
        btns = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel, Qt.Horizontal, self)
        btns.button(QDialogButtonBox.Save).setText("Salvar")
        btns.button(QDialogButtonBox.Cancel).setText("Cancelar")

        def _save():
            self._cfg["folha_xlsx"] = self.ed_planilha.text().strip()
            self.accept()

        btns.accepted.connect(_save)
        btns.rejected.connect(self.reject)
        root.addWidget(btns)

    def get_config(self) -> dict:
        return dict(self._cfg)


# ============================
# UI Principal (estilo do anexo)
# ============================
class AutomacaoFolhaUI(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("tab_automacao_folha")
        self.setStyleSheet(STYLE_SHEET)

        self.cfg = self._load_config()
        self.worker: FolhaWorker | None = None
        self.stat_total = 0
        self.stat_ok = 0
        self.stat_err = 0
        self._last_txt = ""

        root = QVBoxLayout(self)
        root.setContentsMargins(14,14,14,14)
        root.setSpacing(12)

        root.addWidget(self._build_header())
        top = QHBoxLayout(); top.setSpacing(12)
        top.addWidget(self._build_controls_card(), 3)
        top.addWidget(self._build_stats_card(), 2)
        root.addLayout(top)
        root.addWidget(self._build_log_card())

        footer = QLabel("🧾 Automação Folha — v1.0")
        footer.setAlignment(Qt.AlignCenter)
        footer.setStyleSheet("font-size:11px; color:#7F7F7F; padding-top:4px;")
        root.addWidget(footer)

    # ---------- Header ----------
    def _build_header(self) -> QFrame:
        header = QFrame()
        header.setStyleSheet("QFrame{border:1px solid #1e5a9c; border-radius:16px;}")
        lay = QHBoxLayout(header)
        lay.setContentsMargins(18,16,18,16)
        lay.setSpacing(14)

        icon = QLabel()
        if ICON_PATH.exists():
            pix = QPixmap(str(ICON_PATH)).scaled(44,44, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            icon.setPixmap(pix)
        else:
            icon.setText("🧾"); icon.setStyleSheet("font-size:34px; border:none;")
        lay.addWidget(icon, 0, Qt.AlignVCenter)

        title = QLabel("AUTOMAÇÃO FOLHA – TXT (12 colunas)")
        f = QFont(); f.setPointSize(20); f.setBold(True); title.setFont(f)
        subtitle = QLabel("Gere e importe TXT da folha com período, log e cancelamento.")

        title.setStyleSheet("border:none;"); subtitle.setStyleSheet("border:none;")
        title.setWordWrap(True)
        subtitle.setWordWrap(True)
        title.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        subtitle.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        box = QVBoxLayout(); box.addWidget(title); box.addWidget(subtitle)
        lay.addLayout(box, 1)

        btn_help = QToolButton(); btn_help.setText("❓ Ajuda")
        btn_help.clicked.connect(lambda: QMessageBox.information(
            self, "Ajuda",
            "• Defina a planilha em ⚙️ Configurar.\n"
            "• Escolha o período (MM/AAAA → MM/AAAA).\n"
            "• A: imóvel/fazenda; B: CPF; C: Funcionário; D: Líquido; F3/G3/H3: INSS/IRRF/FGTS.\n"
            "• Funcionários no 5º dia útil; tributos no 20º dia útil; valores sem pontuação.\n"
            "• Você pode CANCELAR durante a geração."
        ))
        btn_cfg = QToolButton(); btn_cfg.setText("⚙️ Configurar")
        btn_cfg.clicked.connect(self._open_config)

        btn_close = QToolButton(); btn_close.setText("✖ Fechar")
        btn_close.clicked.connect(self._close_self_tab)

        right = QHBoxLayout()
        right.setSpacing(8)
        right.addWidget(btn_help); right.addWidget(btn_cfg); right.addWidget(btn_close)
        lay.addLayout(right, 0)
        return header

    def _close_self_tab(self):
        parent = self.parent()
        while parent and not isinstance(parent, QTabWidget):
            parent = parent.parent()
        if parent:
            idx = parent.indexOf(self)
            if idx != -1:
                parent.removeTab(idx)
        else:
            self.close()

    # ---------- Controls Card ----------
    def _build_controls_card(self) -> QFrame:
        card = QFrame()
        card.setStyleSheet("QFrame{border:1px solid #1e5a9c; border-radius:12px;}")
        lay = QVBoxLayout(card); lay.setContentsMargins(14,12,14,12); lay.setSpacing(10)

        # Período compacto (combos editáveis + popup consistente)
        period = QHBoxLayout()
        period.setSpacing(8)
        period.addWidget(QLabel("Período:"))

        self.cmb_mes_ini = QComboBox()
        self.cmb_mes_fim = QComboBox()
        for m in range(1, 13):
            self.cmb_mes_ini.addItem(f"{m:02d}", m)
            self.cmb_mes_fim.addItem(f"{m:02d}", m)

        self.cmb_ano_ini = QComboBox()
        self.cmb_ano_fim = QComboBox()
        ano_atual = datetime.now().year
        # anos MUITO amplos (ex.: ~160 anos de janela)
        for y in range(ano_atual - 80, ano_atual + 81):
            self.cmb_ano_ini.addItem(str(y), y)
            self.cmb_ano_fim.addItem(str(y), y)

        # tornar editáveis para permitir digitação
        for cmb in (self.cmb_mes_ini, self.cmb_mes_fim, self.cmb_ano_ini, self.cmb_ano_fim):
            cmb.setEditable(True)
            cmb.setInsertPolicy(QComboBox.NoInsert)  # não “insere” novos itens
            cmb.lineEdit().setAlignment(Qt.AlignCenter)

        # validadores: MM (1..12) e AAAA (1900..9999)
        re_mes = QRegularExpression(r"^(0?[1-9]|1[0-2])$")
        val_mes = QRegularExpressionValidator(re_mes)
        val_ano = QIntValidator(1900, 9999)

        self.cmb_mes_ini.lineEdit().setPlaceholderText("MM")
        self.cmb_mes_fim.lineEdit().setPlaceholderText("MM")
        self.cmb_ano_ini.lineEdit().setPlaceholderText("AAAA")
        self.cmb_ano_fim.lineEdit().setPlaceholderText("AAAA")

        self.cmb_mes_ini.lineEdit().setValidator(val_mes)
        self.cmb_mes_fim.lineEdit().setValidator(val_mes)
        self.cmb_ano_ini.lineEdit().setValidator(val_ano)
        self.cmb_ano_fim.lineEdit().setValidator(val_ano)

        # largura e popup “normal”
        for cmb in (self.cmb_mes_ini, self.cmb_mes_fim, self.cmb_ano_ini, self.cmb_ano_fim):
            cmb.setSizeAdjustPolicy(QComboBox.AdjustToContentsOnFirstShow)
            cmb.setMinimumContentsLength(4)
            view = QListView()
            view.setUniformItemSizes(True)
            view.setStyleSheet(
                "QListView { background:#2B2F31; color:#E0E0E0; border:1px solid #1e5a9c; }"
                "QListView::item { padding:6px 10px; }"
                "QListView::item:selected { background:#1e5a9c; color:#FFFFFF; }"
            )
            cmb.setView(view)
            cmb.setMaxVisibleItems(12)

        # larguras equilibradas
        self.cmb_mes_ini.setFixedWidth(72)
        self.cmb_mes_fim.setFixedWidth(72)
        self.cmb_ano_ini.setFixedWidth(92)
        self.cmb_ano_fim.setFixedWidth(92)

        # valores padrão = mês/ano atuais
        self.cmb_mes_ini.setCurrentIndex(datetime.now().month - 1)
        self.cmb_mes_fim.setCurrentIndex(datetime.now().month - 1)
        idx_ini = self.cmb_ano_ini.findData(ano_atual)
        idx_fim = self.cmb_ano_fim.findData(ano_atual)
        self.cmb_ano_ini.setCurrentIndex(idx_ini if idx_ini >= 0 else self.cmb_ano_ini.count() - 1)
        self.cmb_ano_fim.setCurrentIndex(idx_fim if idx_fim >= 0 else self.cmb_ano_fim.count() - 1)

        period.addWidget(self.cmb_mes_ini)
        period.addWidget(self.cmb_ano_ini)
        arrow = QLabel("→")
        arrow.setStyleSheet("padding:0 6px;")
        period.addWidget(arrow)
        period.addWidget(self.cmb_mes_fim)
        period.addWidget(self.cmb_ano_fim)
        period.addStretch()
        lay.addLayout(period)

        # Ações (cores ajustadas)
        actions = QHBoxLayout(); actions.setSpacing(10)
        self.btn_gerar = QPushButton("📤 Ler Planilha → Gerar TXT")
        self.btn_gerar.setObjectName("success")
        self.btn_gerar.clicked.connect(self._start_worker)
        actions.addWidget(self.btn_gerar)

        self.btn_import = QPushButton("📥 Importar Lançamentos (TXT)")
        self.btn_import.clicked.connect(self._importar_txt)
        actions.addWidget(self.btn_import)

        self.btn_cancel = QPushButton("⛔ Cancelar")
        self.btn_cancel.setObjectName("danger")
        self.btn_cancel.setEnabled(False)
        self.btn_cancel.clicked.connect(self._cancel_worker)
        actions.addWidget(self.btn_cancel)

        actions.addStretch()

        self.btn_log_clear = QToolButton(); self.btn_log_clear.setText("🧹 Limpar Log")
        self.btn_log_clear.setStyleSheet("QToolButton{background:#0d1b3d; border:1px solid #1e5a9c; border-radius:8px; padding:6px 10px;} QToolButton:hover{background:#123764;}")
        self.btn_log_clear.clicked.connect(self._log_clear)
        actions.addWidget(self.btn_log_clear)

        self.btn_log_save = QToolButton(); self.btn_log_save.setText("💾 Salvar Log")
        self.btn_log_save.setStyleSheet("QToolButton{background:#0d1b3d; border:1px solid #1e5a9c; border-radius:8px; padding:6px 10px;} QToolButton:hover{background:#123764;}")
        self.btn_log_save.clicked.connect(self._log_save)
        actions.addWidget(self.btn_log_save)

        lay.addLayout(actions)
        return card

    # ---------- Stats Card ----------
    def _build_stats_card(self) -> QFrame:
        card = QFrame(); card.setObjectName("statsCard")
        card.setStyleSheet("#statsCard{border:1px solid #1e5a9c; border-radius:14px;} #statsCard *{border:none; background:transparent;}")
        lay = QVBoxLayout(card); lay.setContentsMargins(14,12,14,12); lay.setSpacing(6)

        title = QLabel("📊 Último Status da Sessão")
        f = QFont(); f.setPointSize(12); f.setBold(True); title.setFont(f)
        lay.addWidget(title)

        self.lbl_last_status = QLabel("Pronto")
        self.lbl_last_status_time = QLabel(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        self.lbl_last_status_time.setAlignment(Qt.AlignRight)

        row = QHBoxLayout(); row.addWidget(self.lbl_last_status); row.addStretch(); row.addWidget(self.lbl_last_status_time)
        lay.addLayout(row)

        chips = QHBoxLayout(); chips.setSpacing(10)
        self.lbl_stat_total = self._make_chip("Total", "#2B2F31", "#E0E0E0")
        self.lbl_stat_ok    = self._make_chip("Sucesso", "#183d2a", "#A7F3D0")
        self.lbl_stat_err   = self._make_chip("Erros", "#3b1f1f", "#FF6B6B")
        chips.addWidget(self.lbl_stat_total); chips.addWidget(self.lbl_stat_ok); chips.addWidget(self.lbl_stat_err); chips.addStretch()
        lay.addLayout(chips)
        return card

    def _make_chip(self, label: str, bg: str, fg: str) -> QLabel:
        w = QLabel(f"{label}: 0")
        w.setAlignment(Qt.AlignCenter)
        w.setStyleSheet(f"QLabel {{ background:{bg}; color:{fg}; border-radius:10px; padding:8px 12px; font-weight:600; }}")
        return w

    # ---------- Log Card (layout idêntico) ----------
    def _build_log_card(self) -> QFrame:
        card = QFrame(); card.setObjectName("logCard")
        card.setStyleSheet("#logCard{background:#212425; border:1px solid #1e5a9c; border-radius:10px;} "
                           "#logCard QLabel{border:none; background:transparent; color:#E0E0E0;}")
        lay = QVBoxLayout(card); lay.setContentsMargins(12,10,12,12); lay.setSpacing(8)
    
        title = QLabel("📝 Histórico")
        f = QFont(); f.setBold(True); f.setPointSize(12)
        title.setFont(f); title.setStyleSheet("padding:2px 6px;")
        lay.addWidget(title, alignment=Qt.AlignLeft)
    
        body = QFrame(); body.setObjectName("logBody")
        body.setStyleSheet("#logBody{background:#2B2F31; border:none; border-radius:8px;}")
        body_lay = QVBoxLayout(body); body_lay.setContentsMargins(12,12,12,12); body_lay.setSpacing(0)
    
        self.log = QTextEdit(readOnly=True)
        self.log.setMinimumHeight(280)
        self.log.setMaximumHeight(320)  # não deixa “empurrar” a janela
        self.log.setFrameStyle(QFrame.NoFrame)
        self.log.setStyleSheet("QTextEdit{background:transparent; border:none;} "
                               "QTextEdit::viewport{background:transparent; border:none;}")
    
        # >>> PONTOS-CHAVE contra “esticamento”
        self.log.setLineWrapMode(QTextEdit.WidgetWidth)
        self.log.setWordWrapMode(QTextOption.WrapAnywhere)           # quebra até “palavras” longas
        self.log.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff) # nunca usa barra horizontal
        self.log.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
    
        body_lay.addWidget(self.log)
        lay.addWidget(body)
        return card
    
    # ---------- Log helpers (mesma paleta/estilo) ----------
    def log_msg(self, message: str, msg_type: str = "info", update_status: bool = True):
        now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        palette = {
            "info":   {"emoji":"ℹ️","text":"#FFFFFF","accent":"#3A3C3D","weight":"500"},
            "success":{"emoji":"✅","text":"#A7F3D0","accent":"#2F7D5D","weight":"700"},
            "warning":{"emoji":"⚠️","text":"#FFFFFF","accent":"#8A6D3B","weight":"600"},
            "error":  {"emoji":"❌","text":"#FF6B6B","accent":"#7A2E2E","weight":"800"},
            "title":  {"emoji":"📌","text":"#FFFFFF","accent":"#1e5a9c","weight":"800"},
            "divider":{"emoji":"","text":"","accent":"#3A3C3D","weight":"400"},
        }
        if msg_type == "divider":
            self.log.append('<div style="border-top:1px solid #3A3C3D; margin:10px 0;"></div>')
            return
        p = palette.get(msg_type, palette["info"])
        html = (
            f'<div style="border-left:3px solid {p["accent"]}; padding:6px 10px; margin:2px 0;'
            f'word-break: break-word; overflow-wrap: anywhere; white-space: normal;">'
            f'<span style="opacity:.7; font-family:monospace;">[{now}]</span>'
            f' <span style="margin:0 6px 0 8px;">{p["emoji"]}</span>'
            f'<span style="color:{p["text"]}; font-weight:{p["weight"]};">{message}</span>'
            f'</div>'
        )
        self.log.append(html)
    
        if update_status:
            # status curto (sem paths enormes): tira caminhos e limita tamanho
            def _shorten_for_status(text: str, maxlen: int = 140) -> str:
                # remove/encurta qualquer coisa que pareça path absoluto
                text = re.sub(r'([A-Za-z]:\\\\[^\\s]+|/[^\\s]+)', lambda m: os.path.basename(m.group(0)), text)
                return (text[:maxlen-3] + '...') if len(text) > maxlen else text
    
            short = _shorten_for_status(message)
            self.lbl_last_status.setText(short)
            self.lbl_last_status_time.setText(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    

        self.lbl_last_status.setWordWrap(True)
        self.lbl_last_status.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        self.lbl_last_status_time.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Preferred)
        self.lbl_last_status_time.setMinimumWidth(120)

    def _log_clear(self):
        self.log.clear()
        self.log_msg("Log limpo.", "info")

    def _log_save(self):
        try:
            out_dir = Path(__file__).parent / "logs"
            out_dir.mkdir(exist_ok=True, parents=True)
            fname = out_dir / f"folha_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(fname, "w", encoding="utf-8") as f:
                f.write(self.log.toPlainText())
            # caminho só no LOG; status curto
            self.log_msg(f"Log salvo em: {fname}", "success", update_status=False)
            self.lbl_last_status.setText("Log salvo.")
            self.lbl_last_status_time.setText(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        except Exception as e:
            self.log_msg(f"Falha ao salvar log: {e}", "error")

    # ---------- Config persistente ----------
    def _cfg_path(self) -> Path:
        p = Path(__file__).parent / "json"
        p.mkdir(parents=True, exist_ok=True)
        return p / "config_folha.json"

    def _load_config(self) -> dict:
        cfg_file = self._cfg_path()
        if cfg_file.exists():
            try:
                return json.loads(cfg_file.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save_config(self):
        try:
            self._cfg_path().write_text(json.dumps(self.cfg, indent=4, ensure_ascii=False), encoding="utf-8")
            self.log_msg("Configurações salvas.", "success")
        except Exception as e:
            self.log_msg(f"Erro ao salvar config: {e}", "error")

    def _open_config(self):
        dlg = ConfigDialog(self.cfg, self)
        if dlg.exec() == QDialog.Accepted:
            self.cfg.update(dlg.get_config())
            try:
                p = Path(__file__).parent / "json"
                p.mkdir(parents=True, exist_ok=True)
                cfg_file = p / "config_folha.json"
                cfg_file.write_text(json.dumps(self.cfg, indent=4, ensure_ascii=False), encoding="utf-8")
                # caminho só no LOG; status curto
                self.log_msg(f"Configurações salvas em: {cfg_file}", "success", update_status=False)
                self.lbl_last_status.setText("Configurações salvas.")
                self.lbl_last_status_time.setText(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
            except Exception as e:
                self.log_msg(f"Erro ao salvar configurações: {e}", "error")
        
        

    # ---------- Fluxo Geração/Importação ----------
    def _start_worker(self):
        planilha = (self.cfg or {}).get("folha_xlsx", "").strip()
        if not planilha or not Path(planilha).exists():
            QMessageBox.warning(self, "Planilha", "Defina a planilha em ⚙️ Configurar.")
            return

        # lê mês/ano permitindo texto digitado
        def _parse_mes(cmb: QComboBox) -> int:
            t = (cmb.currentText() or "").strip()
            m = re.match(r"^(\d{1,2})$", t)
            if m:
                mi = int(m.group(1))
                if 1 <= mi <= 12:
                    return mi
            d = cmb.currentData()
            if isinstance(d, int):
                return d
            return datetime.now().month
        
        def _parse_ano(cmb: QComboBox) -> int:
            t = (cmb.currentText() or "").strip()
            m = re.match(r"^(\d{4})$", t)
            if m:
                yi = int(m.group(1))
                if 1900 <= yi <= 9999:
                    return yi
            d = cmb.currentData()
            if isinstance(d, int):
                return d
            return datetime.now().year
        
        mes_ini = _parse_mes(self.cmb_mes_ini)
        ano_ini = _parse_ano(self.cmb_ano_ini)
        mes_fim = _parse_mes(self.cmb_mes_fim)
        ano_fim = _parse_ano(self.cmb_ano_fim)
        
        ini = f"{mes_ini:02d}/{ano_ini}"
        fim = f"{mes_fim:02d}/{ano_fim}"

        self._update_stats(0,0,0)
        self.btn_cancel.setEnabled(True)
        self.log_msg(f"Geração do TXT iniciada ({ini} → {fim})…", "title")

        self.worker = FolhaWorker(planilha, ini, fim, parent=self)
        self.worker.log_sig.connect(self._on_worker_log)
        self.worker.stats_sig.connect(self._update_stats)
        self.worker.finished_sig.connect(self._on_worker_finished)
        self.worker.start()

    def _on_worker_log(self, msg: str, level: str):
        # Se a mensagem envolve salvar/gerar/importar com caminho, joga só no LOG
        if ("TXT gerado:" in msg) or ("Log salvo em:" in msg) or ("Importado no sistema:" in msg):
            self.log_msg(msg, level, update_status=False)
            # status curto e sem path
            if "TXT gerado:" in msg:
                self.lbl_last_status.setText("TXT gerado.")
            elif "Log salvo em:" in msg:
                self.lbl_last_status.setText("Log salvo.")
            elif "Importado no sistema:" in msg:
                self.lbl_last_status.setText("Importado no sistema.")
            self.lbl_last_status_time.setText(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
            return

        # Demais mensagens: seguem padrão (com proteção interna para não “esticar”)
        self.log_msg(msg, level, update_status=True)

    def _cancel_worker(self):
        if self.worker and self.worker.isRunning():
            self.log_msg("Solicitando cancelamento…", "warning")
            self.worker.cancel()
        else:
            self.log_msg("Nenhum processo em execução para cancelar.", "info")

    def _on_worker_finished(self, status: str, path_txt: str):
        self.btn_cancel.setEnabled(False)
        if status == "Concluído":
            self._last_txt = path_txt
            # caminho completo só no LOG; status curto
            if path_txt:
                self.log_msg(f"TXT gerado: {path_txt}", "success", update_status=False)
            else:
                self.log_msg("TXT gerado.", "success", update_status=False)

            self.lbl_last_status.setText("TXT gerado.")
            self.lbl_last_status_time.setText(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

            # perguntar se deseja importar agora
            resp = QMessageBox.question(
                self, "Importar agora?",
                "Deseja importar o TXT gerado no sistema?",
                QMessageBox.Yes | QMessageBox.No
            )
            if resp == QMessageBox.Yes:
                self._importar_txt()

        elif status == "Vazio":
            QMessageBox.information(self, "TXT", "Nenhuma linha foi gerada para o período.")
        elif status == "Cancelado":
            QMessageBox.information(self, "TXT", "Processo cancelado.")
        else:
            QMessageBox.warning(self, "TXT", f"Finalizado com status: {status}")


    def _importar_txt(self):
        path = self._last_txt
        if not path or not os.path.exists(path):
            # permitir seleção manual
            path, _ = QFileDialog.getOpenFileName(self, "Selecione o TXT", "", "TXT (*.txt)")
            if not path:
                return
        try:
            mw = self.window()
            if not hasattr(mw, "_import_lancamentos_txt"):
                raise RuntimeError("A janela principal não expõe _import_lancamentos_txt.")
            mw._import_lancamentos_txt(path)
            if hasattr(mw, "carregar_lancamentos"): mw.carregar_lancamentos()
            if hasattr(mw, "dashboard"):
                try: mw.dashboard.load_data()
                except Exception: pass
            self.log_msg(f"Importado no sistema: {os.path.basename(path)}", "success", update_status=False)
            self.lbl_last_status.setText("Importado no sistema.")
            self.lbl_last_status_time.setText(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

        except Exception as e:
            self.log_msg(f"Erro ao importar: {e}", "error")

    # ---------- Stats ----------
    def _update_stats(self, total: int, ok: int, err: int):
        self.stat_total, self.stat_ok, self.stat_err = total, ok, err
        self.lbl_stat_total.setText(f"Total: {total}")
        self.lbl_stat_ok.setText(f"Sucesso: {ok}")
        self.lbl_stat_err.setText(f"Erros: {err}")

# ============================
# Instalação em MainWindow (opcional)
# ============================
def install_in_mainwindow(main_win):
    # evita duplicidade...
    ui = AutomacaoFolhaUI(main_win)   # <- passe o main_win como parent
    ui.setObjectName('tab_automacao_folha')
    main_win.tabs.addTab(ui, "Automação Folha")
    main_win.tabs.setCurrentWidget(ui)
