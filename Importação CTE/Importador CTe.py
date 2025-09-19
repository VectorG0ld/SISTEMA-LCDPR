# -*- coding: utf-8 -*-
# Importador CTe.py — Importador de CT-e no mesmo fluxo do Importador DANFE,
# porém sem "associar pagamentos". Lê XMLs de CT-e, gera TXT no layout 12 colunas
# e oferece importar esses lançamentos diretamente no sistema.

import os
import re
import json
from pathlib import Path
from datetime import datetime
import xml.etree.ElementTree as ET

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFont, QTextCursor, QPixmap
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFrame, QLabel, QPushButton, QLineEdit,
    QFileDialog, QMessageBox, QTextEdit
)

# ===========================
# Aparência / recursos
# ===========================
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
"""

from PySide6.QtGui import QColor, QIcon, QPixmap, QTextCursor
from PySide6.QtWidgets import QToolButton, QTabWidget, QGraphicsDropShadowEffect

def _apply_global_styles(self):
    self.setStyleSheet(STYLE_SHEET)

def _add_shadow(self, widget: QWidget, radius=16, blur=24, color=QColor(0,0,0,50), y_offset=5):
    eff = QGraphicsDropShadowEffect(self)
    eff.setBlurRadius(blur)
    eff.setColor(color)
    eff.setOffset(0, y_offset)
    widget.setGraphicsEffect(eff)
    widget.setStyleSheet(widget.styleSheet() + f"; border-radius:{radius}px;")

# ===========================
# Helpers
# ===========================
def _digits(s: str) -> str:
    return re.sub(r"\D", "", str(s or ""))

def _to_cent(valor_str: str) -> str:
    try:
        return str(int(round(float(str(valor_str).replace(",", ".").strip()) * 100)))
    except Exception:
        return "0"

def _fmt_ddmmYYYY_from_iso(iso_or_br: str) -> str:
    """
    Converte 'YYYY-MM-DD' ou 'YYYY-MM-DDTHH:MM:SS-03:00' ou 'DD/MM/YYYY' em 'DD-MM-YYYY'
    """
    s = (iso_or_br or "").strip()
    if not s:
        return datetime.today().strftime("%d-%m-%Y")
    try:
        if "T" in s:  # ISO datetime
            s2 = re.split(r"[+-]\d{2}:\d{2}$", s.replace("Z", ""))[0]
            dt = datetime.fromisoformat(s2)
            return dt.strftime("%d-%m-%Y")
        if re.match(r"\d{4}-\d{2}-\d{2}$", s):  # ISO date
            y, m, d = s.split("-")
            return f"{d}-{m}-{y}"
        if re.match(r"\d{2}/\d{2}/\d{4}$", s):  # BR com /
            d, m, y = s.split("/")
            return f"{d}-{m}-{y}"
        if re.match(r"\d{2}-\d{2}-\d{4}$", s):  # BR com -
            return s
    except Exception:
        pass
    return datetime.today().strftime("%d-%m-%Y")

def _cte_tomador_info(xml_path: str) -> tuple[str, str]:
    """
    Retorna (cpf_cnpj_tomador, nome_tomador) já normalizados.
    Considera tanto ide/toma4 (tomador 'outros') quanto ide/toma3 (0..3).
    """
    text = Path(xml_path).read_text(encoding="utf-8", errors="ignore")
    root = ET.fromstring(text)
    ns = "{http://www.portalfiscal.inf.br/cte}"
    infcte = root.find(f".//{ns}infCte") or root.find(".//infCte")
    if infcte is None:
        return "", ""

    def fx(path: str) -> str:
        return (infcte.findtext(path) or "").strip()

    # 1) Toma4 com identificação direta do tomador
    t_cnpj = fx(f".//{ns}ide/{ns}toma4/{ns}CNPJ")
    t_cpf  = fx(f".//{ns}ide/{ns}toma4/{ns}CPF")
    t_nome = fx(f".//{ns}ide/{ns}toma4/{ns}xNome")
    if t_cnpj or t_cpf:
        return (_digits(t_cnpj or t_cpf), t_nome)

    # 2) Toma3 -> mapear o papel para a seção correspondente
    toma = fx(f".//{ns}ide/{ns}toma3/{ns}toma")
    papel = {"0": "rem", "1": "exped", "2": "receb", "3": "dest"}.get(toma)
    if papel:
        p_cnpj = fx(f".//{ns}{papel}/{ns}CNPJ")
        p_cpf  = fx(f".//{ns}{papel}/{ns}CPF")
        p_nome = fx(f".//{ns}{papel}/{ns}xNome")
        if p_cnpj or p_cpf:
            return (_digits(p_cnpj or p_cpf), p_nome)

    return "", ""

# --- Tomadores admitidos e seus mapeamentos de imóvel/conta ---
TOMADOR_MAP = {
    # CLEUBER – mantém mapeamento dinâmico já existente (cidade/IE)
    "42276950153": {"perfil": "Cleuber Marcos", "modo": "cleuber"},

    # GILSON
    "54860253191": {
        "perfil": "Gilson Oliveira", "modo": "fixo",
        "cod_imovel": "112725503",
        "nome_imovel": "RIALMA - FAZENDA FORMIGA",
    },

    # LUCAS
    "03886681130": {
        "perfil": "Lucas Laignier", "modo": "fixo",
        "cod_imovel": "115008810",
        "nome_imovel": "TROMBAS - FAZENDA PRIMAVERA RETIRO",
    },

    # ADRIANA
    "47943246187": {
        "perfil": "Adriana Lucia", "modo": "fixo",
        "cod_imovel": "113348037",
        "nome_imovel": "MONTIVIDIU DO NORTE - FAZENDA POUSO DA ANTA",
    },
}

def _cte_tomador_endereco(xml_path: str) -> dict:
    """
    Extrai o endereço do tomador (toma4 ou papel de toma3) para
    auto-cadastro do imóvel: xLgr, nro, xCpl, xBairro, cMun, xMun, CEP, UF e IE.
    """
    text = Path(xml_path).read_text(encoding="utf-8", errors="ignore")
    root = ET.fromstring(text)
    ns = "{http://www.portalfiscal.inf.br/cte}"
    infcte = root.find(f".//{ns}infCte") or root.find(".//infCte")
    if infcte is None:
        return {}

    def fx(p): return (infcte.findtext(p) or "").strip()

    # tenta toma4
    base = f".//{ns}ide/{ns}toma4"
    if infcte.find(base) is not None:
        end = {
            "xLgr": fx(f"{base}/{ns}enderToma/{ns}xLgr"),
            "nro": fx(f"{base}/{ns}enderToma/{ns}nro"),
            "xCpl": fx(f"{base}/{ns}enderToma/{ns}xCpl"),
            "xBairro": fx(f"{base}/{ns}enderToma/{ns}xBairro"),
            "cMun": fx(f"{base}/{ns}enderToma/{ns}cMun"),
            "xMun": fx(f"{base}/{ns}enderToma/{ns}xMun"),
            "CEP": fx(f"{base}/{ns}enderToma/{ns}CEP"),
            "UF": fx(f"{base}/{ns}enderToma/{ns}UF"),
            "IE": fx(f"{base}/{ns}IE"),
        }
        return end

    # mapeia toma3 → seção de rem/exped/receb/dest
    papel = {"0": "rem", "1": "exped", "2": "receb", "3": "dest"}.get(fx(f".//{ns}ide/{ns}toma3/{ns}toma"))
    if papel:
        tag_by_papel = {"rem":"enderReme","exped":"enderExped","receb":"enderReceb","dest":"enderDest"}
        ender_tag = tag_by_papel.get(papel)
        if ender_tag:
            end = {
                "xLgr": fx(f".//{ns}{papel}/{ns}{ender_tag}/{ns}xLgr"),
                "nro":  fx(f".//{ns}{papel}/{ns}{ender_tag}/{ns}nro"),
                "xCpl": fx(f".//{ns}{papel}/{ns}{ender_tag}/{ns}xCpl"),
                "xBairro": fx(f".//{ns}{papel}/{ns}{ender_tag}/{ns}xBairro"),
                "cMun": fx(f".//{ns}{papel}/{ns}{ender_tag}/{ns}cMun"),
                "xMun": fx(f".//{ns}{papel}/{ns}{ender_tag}/{ns}xMun"),
                "CEP":  fx(f".//{ns}{papel}/{ns}{ender_tag}/{ns}CEP"),
                "UF":   fx(f".//{ns}{papel}/{ns}{ender_tag}/{ns}UF"),
                "IE":   fx(f".//{ns}{papel}/{ns}IE"),
            }
            return end

    return {}

# Perfis padrão da sua UI (exatamente como aparecem no sistema.py)
PERFIS_VALIDOS = ["Cleuber Marcos", "Gilson Oliveira", "Adriana Lucia", "Lucas Laignier"]
TIPO_FORNECEDOR = 2  # CT-e gera saída → contra-parte tratada como fornecedor

def _detectar_perfil(emit_nome: str, dest_nome: str, cpf_cnpj_contraparte: str) -> str:
    """
    Heurística simples por nome (emitente/destinatário) + CPF do Cleuber.
    Caso não encontre, assume 'Cleuber Marcos' (você disse ser o operador).
    """
    nome_busca = f"{emit_nome or ''} {dest_nome or ''}".upper()
    if "CLEUBER" in nome_busca or cpf_cnpj_contraparte == "42276950153":
        return "Cleuber Marcos"
    if "GILSON" in nome_busca:
        return "Gilson Oliveira"
    if "ADRIANA" in nome_busca:
        return "Adriana Lucia"
    if "LUCAS" in nome_busca:
        return "Lucas Laignier"
    return "Cleuber Marcos"  # padrão

def _read_cte_fields(xml_path: str) -> dict:
    """
    Extrai campos da CT-e para montar a linha do TXT (12 colunas) e adiciona cidade/IE para mapeamento do imóvel.
    """
    text = Path(xml_path).read_text(encoding="utf-8", errors="ignore")
    root = ET.fromstring(text)

    ns = "{http://www.portalfiscal.inf.br/cte}"
    infcte = root.find(f".//{ns}infCte") or root.find(".//infCte")
    if infcte is None:
        raise ValueError("infCte não encontrado")

    def fx(path):
        return infcte.findtext(path)

    # Datas / número
    nCT   = fx(f".//{ns}ide/{ns}nCT") or ""
    dhEmi = fx(f".//{ns}ide/{ns}dhEmi")
    dEmi  = fx(f".//{ns}ide/{ns}dEmi")
    data  = _fmt_ddmmYYYY_from_iso(dhEmi or dEmi)

    # Cidades possíveis (ordem de preferência)
    xMunFim  = fx(f".//{ns}ide/{ns}xMunFim") or ""
    xMunDest = fx(f".//{ns}dest/{ns}enderDest/{ns}xMun") or ""
    xMunRecb = fx(f".//{ns}receb/{ns}enderReceb/{ns}xMun") or ""
    cidade   = next((c for c in (xMunFim, xMunDest, xMunRecb) if c), "")

    # IE possíveis
    ie_dest = fx(f".//{ns}dest/{ns}IE") or ""
    ie_recb = fx(f".//{ns}receb/{ns}IE") or ""
    ie_toma = fx(f".//{ns}ide/{ns}toma4/{ns}IE") or ""
    ie_any  = re.sub(r"\D", "", (ie_dest or ie_recb or ie_toma or ""))

    # Emitente e destinatário
    emit_cnpj = fx(f".//{ns}emit/{ns}CNPJ") or ""
    emit_cpf  = fx(f".//{ns}emit/{ns}CPF") or ""
    emit_nome = fx(f".//{ns}emit/{ns}xNome") or ""

    dest_cnpj = fx(f".//{ns}dest/{ns}CNPJ") or ""
    dest_cpf  = fx(f".//{ns}dest/{ns}CPF") or ""
    dest_nome = fx(f".//{ns}dest/{ns}xNome") or ""

    # Total do CT-e
    vTPrest = fx(f".//{ns}vPrest/{ns}vTPrest") or fx(f".//{ns}vTPrest") or fx(f".//{ns}vPrest/{ns}vRec") or "0"
    centavos = _to_cent(vTPrest)

    historico = f"PAGAMENTO CTE {nCT} {emit_nome}".strip()
    cpf_cnpj = re.sub(r"\D","", emit_cnpj or emit_cpf or dest_cnpj or dest_cpf or "")

    perfil = _detectar_perfil(emit_nome, dest_nome, cpf_cnpj)
    return {
        "data_br": data, "cod_imovel": "001", "cod_conta": "001",
        "num_doc": nCT or "", "tipo_doc": "1",
        "historico": historico, "cpf_cnpj": cpf_cnpj,
        "tipo_lanc": "2", "cent_ent": "000",
        "cent_sai": centavos, "cent_saldo": centavos, "nat": "N",
        "perfil": perfil,
        "emitente": emit_nome, "destinatario": dest_nome,
        "arquivo": os.path.basename(xml_path),
        "cidade": cidade, "ie": ie_any
    }

def _make_line(d: dict) -> str:
    return "|".join([
        d["data_br"], d["cod_imovel"], d["cod_conta"], d["num_doc"], d["tipo_doc"],
        d["historico"], d["cpf_cnpj"], d["tipo_lanc"], d["cent_ent"],
        d["cent_sai"], d["cent_saldo"], d["nat"]
    ])

# ===========================
# Widget principal
# ===========================
class ImportadorCTe(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("tab_import_cte")
        _apply_global_styles(self)

        # Estado / Config
        self.config = self.load_config()
        self.stat_total = 0
        self.stat_ok = 0
        self.stat_err = 0
        self._cancel = False

        # UI
        self._build_ui()

    # ---------- UI ----------
    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(12)

        header = self._header_card()
        root.addWidget(header)

        controls_card = self._controls_card()
        stats_card = self._build_stats_card()

        top_row = QHBoxLayout()
        top_row.setSpacing(12)
        top_row.addWidget(controls_card, 3)
        top_row.addWidget(stats_card, 2)
        root.addLayout(top_row)

        log_card = self._log_card()
        root.addWidget(log_card)

        footer = QLabel("🚚 Importador CT-e — v.1.0")
        footer.setAlignment(Qt.AlignCenter)
        footer.setStyleSheet("font-size:11px; color:#7F7F7F; padding-top:4px;")
        root.addWidget(footer)

    def _build_stats_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName("statsCard")
        card.setStyleSheet("#statsCard{border:1px solid #1e5a9c; border-radius:14px;} #statsCard *{border:none; background:transparent;}")
        lay = QVBoxLayout(card); lay.setContentsMargins(14,12,14,12); lay.setSpacing(6)

        title = QLabel("📊 Último Status da Sessão")
        f = QFont(); f.setPointSize(12); f.setBold(True)
        title.setFont(f)
        lay.addWidget(title)

        self.lbl_last_status = QLabel("Pronto")
        self.lbl_last_status.setStyleSheet("font-weight:600;")
        self.lbl_last_status_time = QLabel(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        self.lbl_last_status_time.setAlignment(Qt.AlignRight)
        status_row = QHBoxLayout(); status_row.setSpacing(10)
        status_row.addWidget(self.lbl_last_status); status_row.addStretch(); status_row.addWidget(self.lbl_last_status_time)
        lay.addLayout(status_row)

        # Reaproveita chips já criados em _controls_card
        chips = QHBoxLayout(); chips.setSpacing(10)
        chips.addWidget(self.lbl_stat_total)
        chips.addWidget(self.lbl_stat_ok)
        chips.addWidget(self.lbl_stat_err)
        chips.addStretch()
        lay.addLayout(chips)

        _add_shadow(self, card, radius=14, blur=20, color=QColor(0,0,0,45), y_offset=4)
        return card

    def _header_card(self) -> QFrame:
        card = QFrame()
        card.setStyleSheet("QFrame{border:1px solid #1e5a9c; border-radius:16px;}")
        lay = QHBoxLayout(card); lay.setContentsMargins(18,16,18,16); lay.setSpacing(14)

        icon = QLabel()
        if ICON_PATH.exists():
            pix = QPixmap(str(ICON_PATH)).scaled(44, 44, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            icon.setPixmap(pix); icon.setStyleSheet("border:none;")
        else:
            icon.setText("🚚"); icon.setStyleSheet("font-size:34px; border:none;")
        lay.addWidget(icon, 0, Qt.AlignVCenter)

        title = QLabel("IMPORTADOR CT-e")
        f = QFont(); f.setPointSize(20); f.setBold(True)
        title.setFont(f); title.setStyleSheet("border:none;")

        subtitle = QLabel("Lê XMLs, gera TXT (12 colunas) e importa no sistema.")
        subtitle.setStyleSheet("border:none;")

        title_box = QVBoxLayout()
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        lay.addLayout(title_box, 1)

        btn_help = QToolButton(); btn_help.setText("❓ Ajuda")
        btn_help.clicked.connect(lambda: QMessageBox.information(
            self, "Ajuda",
            "1) Defina a pasta dos XMLs de CT-e.\n"
            "2) Use “📤 Importar XMLs (CT-e) → Gerar TXT”.\n"
            "3) Se quiser, “📥 Importar Lançamentos” para subir o TXT.\n"
            "4) Acompanhe os logs e salve o histórico."
        ))

        btn_close = QToolButton(); btn_close.setText("✖ Fechar")
        def _close_self_tab():
            parent = self.parent()
            while parent and not isinstance(parent, QTabWidget):
                parent = parent.parent()
            if parent:
                idx = parent.indexOf(self)
                if idx != -1:
                    parent.removeTab(idx)
            else:
                self.close()
        btn_close.clicked.connect(_close_self_tab)

        row = QHBoxLayout(); row.setSpacing(8)
        row.addWidget(btn_help); row.addWidget(btn_close)
        lay.addLayout(row, 0)

        _add_shadow(self, card, radius=16, blur=24, color=QColor(0,0,0,50), y_offset=5)
        return card

    def _controls_card(self) -> QFrame:
        card = QFrame()
        card.setStyleSheet("QFrame{border:1px solid #1e5a9c; border-radius:12px;}")
        lay = QVBoxLayout(card); lay.setContentsMargins(14,12,14,12); lay.setSpacing(10)
    
        # Linha 1 — Pasta CT-e
        l1 = QHBoxLayout()
        self.ed_dir = QLineEdit(self.config.get("cte_dir", ""))
        self.ed_dir.setPlaceholderText("Pasta onde estão os XMLs de CT-e…")
        btn_browse = QPushButton("Selecionar Pasta"); btn_browse.clicked.connect(self._choose_dir)
        btn_save   = QPushButton("Salvar Pasta");     btn_save.clicked.connect(self._save_dir)
        l1.addWidget(QLabel("📂 Pasta CT-e:"))
        l1.addWidget(self.ed_dir, 1)
        l1.addWidget(btn_browse)
        l1.addWidget(btn_save)
    
        # Linha 2 — Ações principais
        actions = QHBoxLayout(); actions.setSpacing(10)
        self.btn_xmls = QPushButton("📤 Importar XMLs (CT-e) → Gerar TXT")
        self.btn_xmls.setObjectName("success")
        self.btn_xmls.clicked.connect(self.importar_xmls_cte)
        actions.addWidget(self.btn_xmls)
    
        self.btn_import_txt = QPushButton("📥 Importar Lançamentos")
        self.btn_import_txt.clicked.connect(self.importar_lancamentos_txt)
        actions.addWidget(self.btn_import_txt)
    
        self.btn_cancel = QPushButton("⛔ Cancelar"); self.btn_cancel.setEnabled(False)
        self.btn_cancel.setObjectName("danger")
        self.btn_cancel.clicked.connect(self.cancelar_processos)
        actions.addWidget(self.btn_cancel)
    
        actions.addStretch()
    
        # Botões utilitários do Log (QToolButton + estilo)
        self.btn_log_clear = QToolButton()
        self.btn_log_clear.setText("🧹 Limpar Log")
        self.btn_log_clear.setStyleSheet("QToolButton{background:#0d1b3d; border:1px solid #1e5a9c; border-radius:8px; padding:6px 10px;} QToolButton:hover{background:#123764;}")
        self.btn_log_clear.clicked.connect(self._log_clear)
        actions.addWidget(self.btn_log_clear)
    
        self.btn_log_save = QToolButton()
        self.btn_log_save.setText("💾 Salvar Log")
        self.btn_log_save.setStyleSheet("QToolButton{background:#0d1b3d; border:1px solid #1e5a9c; border-radius:8px; padding:6px 10px;} QToolButton:hover{background:#123764;}")
        self.btn_log_save.clicked.connect(self._log_save)
        actions.addWidget(self.btn_log_save)
    
        # Linha 3 — Chips de status
        chips_row = QHBoxLayout(); chips_row.setSpacing(10)
        def _make_chip(label: str, bg: str, fg: str) -> QLabel:
            w = QLabel(f"{label}: 0"); w.setAlignment(Qt.AlignCenter)
            w.setStyleSheet(f"QLabel {{ background:{bg}; color:{fg}; border-radius:10px; padding:8px 12px; font-weight:600; }}")
            return w
        self.lbl_stat_total = _make_chip("Total",   "#2B2F31", "#E0E0E0")
        self.lbl_stat_ok    = _make_chip("Sucesso", "#183d2a", "#A7F3D0")
        self.lbl_stat_err   = _make_chip("Erros",   "#3b1f1f", "#FF6B6B")
        chips_row.addWidget(self.lbl_stat_total)
        chips_row.addWidget(self.lbl_stat_ok)
        chips_row.addWidget(self.lbl_stat_err)
        chips_row.addStretch()
    
        lay.addLayout(l1)
        lay.addLayout(actions)
        lay.addLayout(chips_row)
    
        _add_shadow(self, card, radius=14, blur=20, color=QColor(0,0,0,45), y_offset=4)
        return card
    
    def cancelar_processos(self):
        # cancela o processamento atual de XMLs CT-e
        self._cancel = True
        self.btn_cancel.setEnabled(False)
        self.log_msg("Processo(s) cancelado(s).", "success")
        self.lbl_last_status.setText("PROCESSO CANCELADO PELO USUARIO")
        self.lbl_last_status_time.setText(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

    def _log_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName("logCard")
        card.setStyleSheet("#logCard{background:#212425; border:1px solid #1e5a9c; border-radius:10px;} #logCard QLabel{border:none; background:transparent; color:#E0E0E0;}")
        lay = QVBoxLayout(card); lay.setContentsMargins(12,10,12,12); lay.setSpacing(8)

        title = QLabel("📝 Histórico")
        f = QFont(); f.setBold(True); f.setPointSize(12)
        title.setFont(f); title.setStyleSheet("padding:2px 6px;")
        lay.addWidget(title, alignment=Qt.AlignLeft)

        body = QFrame(); body.setObjectName("logBody")
        body.setStyleSheet("#logBody{background:#2B2F31; border:none; border-radius:8px;}")
        body_lay = QVBoxLayout(body); body_lay.setContentsMargins(12,12,12,12); body_lay.setSpacing(0)

        self.log = QTextEdit(readOnly=True)
        self.log.setMinimumHeight(260)
        self.log.setFrameStyle(QFrame.NoFrame)
        self.log.setStyleSheet("QTextEdit{background:transparent; border:none;} QTextEdit::viewport{background:transparent; border:none;}")

        body_lay.addWidget(self.log)
        lay.addWidget(body)
        return card


    # ---------- Ações ----------
    def _choose_dir(self):
        start = self.ed_dir.text().strip() or os.getcwd()
        d = QFileDialog.getExistingDirectory(self, "Selecione a pasta de CT-e", start)
        if d: self.ed_dir.setText(d)

    def _save_dir(self):
        self.config["cte_dir"] = self.ed_dir.text().strip()
        self.save_config()
        self.log_msg(f"Pasta salva: {self.config.get('cte_dir','')}", "success")

    # ===== NOVO: garante cadastro de participante (CNPJ/CPF) =====
    def _ensure_participante(self, cpf_cnpj: str, nome: str, tipo: int = TIPO_FORNECEDOR) -> int | None:
        """
        Retorna o ID do participante. Se não existir, cadastra e atualiza as listas da UI.
        - tipo: usar 2 para fornecedor (saída), compatível com seu schema.
        """
        cpf_cnpj = re.sub(r"\D", "", cpf_cnpj or "")
        nome = (nome or "").strip()
        if not cpf_cnpj or not nome:
            return None

        mw = self.window()
        if not mw or not hasattr(mw, "db"):
            # sem DB disponível na janela principal, não há o que fazer
            return None

        # Existe?
        row = mw.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj=?", (cpf_cnpj,))
        if row and row[0]:
            return int(row[0])

        # Cadastra
        mw.db.execute_query(
            "INSERT INTO participante (cpf_cnpj, nome, tipo_contraparte) VALUES (?,?,?)",
            (cpf_cnpj, nome, int(tipo))
        )
        pid = mw.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj=?", (cpf_cnpj,))
        pid = int(pid[0]) if pid and pid[0] else None

        # Atualiza listas/tabelas imediatamente (tenta vários nomes possíveis)
        for fn in (
            "atualizar_participantes", "carregar_participantes",
            "reload_participants", "refresh_participants", "preencher_participantes"
        ):
            try:
                if hasattr(mw, fn):
                    getattr(mw, fn)()
            except Exception:
                pass

        # Log amigável
        self.log_msg(f"Participante cadastrado: {nome} [{cpf_cnpj}] (id={pid})", "success")
        return pid
    
    # ===== NOVO: garante cadastro do imóvel =====
    def _ensure_imovel(self, cod_imovel_sugerido: str, nome_imovel: str, addr: dict) -> str:
        """
        Garante que o imóvel exista. Se não existir, cria com CÓDIGO SEQUENCIAL (001, 002, ...),
        ignorando IE/códigos grandes. Retorna o código efetivamente usado/existente.
        """
        mw = self.window()
        if not mw or not hasattr(mw, "db"):
            # Sem DB, devolve algo seguro
            return (cod_imovel_sugerido or "001")

        # 1) Já existe pelo NOME? (preferência por nome exato da fazenda)
        row = mw.db.fetch_one(
            "SELECT cod_imovel FROM imovel_rural WHERE nome_imovel=? LIMIT 1",
            (nome_imovel,)
        )
        if row and row[0]:
            # Se já existir, retorna o código existente (com zero-pad se for numérico de até 3 dígitos)
            code = str(row[0])
            return code.zfill(3) if re.fullmatch(r"\d{1,3}", code) else code

        # 2) Não existe: calcular PRÓXIMO CÓDIGO SEQUENCIAL de 3 dígitos (001..999)
        row = mw.db.fetch_one(
            "SELECT COALESCE(MAX(CAST(cod_imovel AS INTEGER)), 0) "
            "FROM imovel_rural "
            "WHERE cod_imovel GLOB '[0-9]*' AND LENGTH(cod_imovel) <= 3"
        )
        max3 = int(row[0] or 0)
        new_code = f"{max3 + 1:03d}"

        # 3) Sanitização dos campos de endereço
        def nz(v, alt=""):
            v = (v or "").strip()
            return v if v else alt

        cep = _digits(addr.get("CEP", ""))[:8] or "00000000"
        uf  = nz(addr.get("UF"), "GO")
        cod_mun = nz(_digits(addr.get("cMun", "")) or "", "0")
        endereco = nz(addr.get("xLgr"), ".")
        bairro   = nz(addr.get("xBairro"), ".")
        numero   = nz(addr.get("nro"), None)
        compl    = nz(addr.get("xCpl"), None)
        ie       = _digits(addr.get("IE", "")) or None

        # 4) INSERT com 18 campos (modelo do seu sistema)
        mw.db.execute_query(
            """
            INSERT OR REPLACE INTO imovel_rural (
              cod_imovel, pais, moeda, cad_itr, caepf, insc_estadual,
              nome_imovel, endereco, num, compl, bairro, uf,
              cod_mun, cep, tipo_exploracao, participacao,
              area_total, area_utilizada
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
              new_code, "BR", "BRL", None, None, ie,
              nome_imovel, endereco, numero, compl, bairro, uf,
              cod_mun, cep, 1, 100.0, 0.0, 0.0
            ],
            autocommit=True
        )

        self.log_msg(f"🏡 Imóvel cadastrado: {nome_imovel} [cod {new_code}]", "success")
        return new_code


    def importar_xmls_cte(self):
        base_dir = self.ed_dir.text().strip()
        if not base_dir or not os.path.isdir(base_dir):
            QMessageBox.warning(self, "Pasta inválida", "Defina uma pasta válida dos XMLs de CT-e.")
            return

        xmls = [str(p) for p in Path(base_dir).glob("*.xml")]
        if not xmls:
            QMessageBox.information(self, "Sem XML", "Nenhum .xml encontrado na pasta.")
            return

        self.stat_total = len(xmls); self.stat_ok = 0; self.stat_err = 0; self._upd_stats()
        self._cancel = False; self.btn_cancel.setEnabled(True)
        self.log.clear()
        self.log_msg(f"Iniciando processamento de {len(xmls)} arquivo(s)…", "info")

        QTimer.singleShot(0, lambda: self._process_xmls(xmls))

    def _process_xmls(self, files: list):
            try:
                grupos = {}    # perfil -> [registros]
                resumo = {}    # (perfil, cod_imovel) -> total_centavos

                # Salvar na MESMA pasta dos XMLs
                base_dir = self.ed_dir.text().strip() or os.getcwd()

                # --- mapeamentos do Cleuber (cidade/IE -> apelido de fazenda) ---
                CODIGOS_CIDADES = {
                    "Lagoa da Confusao": "Frutacc",
                    "Montividiu do Norte": "Barragem",
                    "Rialma": "Aliança",
                    "TROMBAS": "Primavera",
                    "DUERE": "L3", "DUERÉ": "L3", "DUERE TO": "L3", "Duere": "L3",
                    "Ceres": "Aliança", "Rianapolis": "Aliança", "NOVA GLORIA": "Aliança",
                    "MONTIVIDIU": "Barragem", "MONTIVIDIU DO NORTE - GO": "Barragem",
                    "Nova Glória": "Aliança", "Nova Gloria": "Aliança",
                    "Lagoa da Confusão": "Frutacc", "MONTIVIDIU DO NORTE": "Barragem",
                    "LAGOA DA CONFUSAO": "Frutacc", "LAGOA DA CONFUSÃO": "Frutacc",
                    "LAGOA CONFUSAO": "Frutacc", "LAGOA DA CONFUSAO - TO": "Frutacc",
                    "RIALMA": "Aliança", "Trombas": "Primavera", "CERES": "Aliança",
                    "Formoso do Araguaia": "União", "FORMOSO DO ARAGUAIA": "União",
                    "APARECIDA DO RIO NEGRO": "Primavera",
                    "Tasso Fragoso": "Guara", "BALSAS": "Guara", "Balsas": "Guara",
                    "Montividiu": "Barragem",
                }
                FARM_MAPPING = {
                    "115149210": "Arm. Primavera",
                    "111739837": "Aliança",
                    "114436720": "B. Grande",
                    "115449965": "Estrela",
                    "294186832": "Frutacc",
                    "294907068": "Frutacc III",
                    "295057386": "L3",
                    "112877672": "Primavera",
                    "113135521": "Primavera Retiro",
                    "294904093": "União",
                    "295359790": "Frutacc V",
                    "295325704": "Siganna"
                }

                def _norm(s: str) -> str:
                    import unicodedata, re
                    s = (s or "").strip()
                    if not s: return ""
                    s = unicodedata.normalize("NFKD", s)
                    s = "".join(ch for ch in s if not unicodedata.combining(ch))
                    s = re.sub(r"\s+", " ", s.upper())
                    return s

                CIDADES_NORM = { _norm(k): v for k, v in CODIGOS_CIDADES.items() }
                IE_TO_ALIAS  = { re.sub(r"\D", "", k): v for k, v in FARM_MAPPING.items() }

                # helpers de consulta no banco
                def _query_cod_by_alias(alias: str):
                    mw = self.window()
                    if not alias or not mw or not hasattr(mw, "db"): return None
                    alias_to_cod = self.config.get("alias_to_cod", {})
                    cod = alias_to_cod.get(alias) or alias_to_cod.get(_norm(alias))
                    if cod: return str(cod)
                    row = mw.db.fetch_one("SELECT cod_imovel FROM imovel_rural WHERE UPPER(nome_imovel)=?", (alias.upper(),))
                    if row and row[0]: return str(row[0])
                    row = mw.db.fetch_one("SELECT cod_imovel FROM imovel_rural WHERE UPPER(nome_imovel) LIKE ?", (f"%{alias.upper()}%",))
                    if row and row[0]: return str(row[0])
                    return None

                def _query_cod_by_ie(ie: str):
                    mw = self.window()
                    if not ie or not mw or not hasattr(mw, "db"): return None
                    row = mw.db.fetch_one("SELECT cod_imovel FROM imovel_rural WHERE insc_estadual=?", (ie,))
                    if row and row[0]: return str(row[0])
                    return None

                for path in files:
                    if self._cancel:
                        self.log_msg("Processo cancelado pelo usuário.", "warning"); break
                    try:
                        # filtro por tomador (aceita Cleuber, Gilson, Lucas, Adriana)
                        tomador_cpf, tomador_nome = _cte_tomador_info(path)
                        if tomador_cpf not in TOMADOR_MAP:
                            self.log_msg(
                                f"Pulado: tomador '{tomador_nome}' [{tomador_cpf or '---'}] não é Cleuber/Gilson/Lucas/Adriana.",
                                "warning"
                            )
                            continue

                        rec = _read_cte_fields(path)

                        # --- restringe aos tomadores permitidos e seta perfil/conta ---
                        tom = TOMADOR_MAP[tomador_cpf]
                        rec["perfil"] = tom["perfil"]
                        rec["cod_conta"] = "001"           # conta 001 do perfil do tomador
                        rec["_tomador_cpf"] = tomador_cpf

                        # ------- resolver cod_imovel (tomador-específico) -------
                        cod_imovel = "001"; origem = "default"
                        tom_cfg = TOMADOR_MAP.get(tomador_cpf, {})

                        if tom_cfg.get("modo") == "cleuber":
                            # mantém lógica dinâmica por cidade/IE (Cleuber)
                            cidade = rec.get("cidade") or ""
                            ie     = rec.get("ie") or ""

                            alias = CIDADES_NORM.get(_norm(cidade))
                            if alias:
                                cod = _query_cod_by_alias(alias)
                                if cod:
                                    cod_imovel, origem = cod, f"cidade:{alias}"

                            if origem == "default" and ie:
                                cod = _query_cod_by_ie(ie)
                                if cod:
                                    cod_imovel, origem = cod, f"IE:{ie}"
                                else:
                                    alias2 = IE_TO_ALIAS.get(re.sub(r"\D","", ie))
                                    if alias2:
                                        cod = _query_cod_by_alias(alias2)
                                        if cod:
                                            cod_imovel, origem = cod, f"IE→alias:{alias2}"
                        else:
                            # GILSON / LUCAS / ADRIANA — código fixo + prepara auto-cadastro
                            cod_imovel = tom_cfg["cod_imovel"]
                            origem = "tomador:fixo"
                            addr = _cte_tomador_endereco(path)  # endereço do tomador no XML
                            rec["_auto_imovel"] = True
                            rec["_imovel_payload"] = {
                                "cod_imovel": cod_imovel,
                                "nome_imovel": tom_cfg["nome_imovel"],
                                "addr": addr,
                            }

                        rec["cod_imovel"] = cod_imovel

                        grupos.setdefault(rec["perfil"], []).append(rec)

                        key = (rec["perfil"], cod_imovel)
                        resumo[key] = resumo.get(key, 0) + int(rec["cent_sai"])

                        self.stat_ok += 1
                        self.log_line(rec, origem)
                    except Exception as e:
                        self.stat_err += 1
                        self.log_msg(f"[ERRO] {os.path.basename(path)} → {e}", "error")
                    finally:
                        self._upd_stats()

                # Salvar TXT(s) na MESMA pasta dos XMLs
                out_dir = Path(base_dir)

                # [AJUSTE] Autocadastro + ATUALIZAÇÃO do CÓDIGO SEQUENCIAL **NO PERFIL CORRETO**
                main_win = self.window()
                for perfil, lst in grupos.items():
                    if main_win and hasattr(main_win, "switch_profile"):
                        main_win.switch_profile(perfil)  # << cadastra o imóvel no perfil do tomador
                    for r in lst:
                        if r.get("_auto_imovel") and r.get("_imovel_payload"):
                            p = r["_imovel_payload"]
                            cod_usado = self._ensure_imovel(p["cod_imovel"], p["nome_imovel"], p["addr"])
                            r["cod_imovel"] = cod_usado  # 001, 002, ...

                # (re)calcular o RESUMO com o cod_imovel atualizado
                resumo = {}
                for _perfil, _lst in grupos.items():
                    for r in _lst:
                        key = (r["perfil"], r["cod_imovel"])
                        resumo[key] = resumo.get(key, 0) + int(r.get("cent_sai", 0))

                # Agora SIM gerar os TXT já com o código certo (sem recriar imóvel aqui)
                all_txt = out_dir / "CTE_TODOS.txt"
                with open(all_txt, "w", encoding="utf-8") as f:
                    for lst in grupos.values():
                        for r in lst:
                            f.write(_make_line(r) + "\n")

                por_perfil = {}
                for perfil, lst in grupos.items():
                    fname = out_dir / f"CTE_{re.sub(r'[^A-Za-z0-9_-]+','_', perfil)}.txt"
                    with open(fname, "w", encoding="utf-8") as f:
                        for r in lst:
                            f.write(_make_line(r) + "\n")
                    por_perfil[perfil] = str(fname)

                # Blocos de log por perfil (fora do loop acima, para não duplicar)
                EMOJI_PERFIL = {
                    "Cleuber Marcos": "🧑‍🌾",
                    "Gilson Oliveira": "🧔",
                    "Lucas Laignier": "🧑",
                    "Adriana Lucia": "👩"
                }
                for perfil, lst in grupos.items():
                    if not lst:
                        continue
                    self._log_section(perfil.split()[0], EMOJI_PERFIL.get(perfil, "🚚"))
                    self._log_header()
                    for r in lst:
                        self._log_row_table(r)
                    self.log.append("<div style='text-align:center;color:#2e3d56;font-family:monospace;'>======================</div>")

                # Resumo organizado
                if resumo:
                    self.log_msg("\n—— Resumo por Perfil/Imóvel ——", "info")
                    for (perfil, imv), cents in sorted(resumo.items(), key=lambda x:(x[0][0], x[0][1])):
                        reais = cents/100.0
                        self.log_msg(f"  • {perfil:>16s} | Imóvel {imv:<6s} | Total R$ {reais:,.2f}".replace(",", "X").replace(".", ",").replace("X","."), "success")

                self.log_msg(f"\nTXT(s) gerados na pasta dos XMLs:\n  • TODOS: {all_txt}\n" + "\n".join([f"  • {p}: {fp}" for p, fp in por_perfil.items()]), "success")
                self.lbl_last_status.setText("TXT(s) gerados ✅")
                self.lbl_last_status_time.setText(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                self.btn_cancel.setEnabled(False)

                # Perguntar se deseja importar por perfil
                main_win = self.window()
                if main_win and hasattr(main_win, "_import_lancamentos_txt") and hasattr(main_win, "switch_profile"):
                    if QMessageBox.question(self, "Importar agora?", "Importar os lançamentos gerados para cada perfil?",
                                            QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
                        # 🔁 Preparar registro de duplicados para log ao final
                        dups_log = []

                        for perfil, fp in por_perfil.items():
                            try:
                                if perfil not in PERFIS_VALIDOS:
                                    self.log_msg(f"Aviso: perfil '{perfil}' não está na lista de perfis válidos; pulado.", "warning")
                                    continue
                                
                                # Troca para o banco do perfil ANTES de checar duplicidade
                                main_win.switch_profile(perfil)

                                # (se houver) auto-cadastro/ajuste do código do imóvel antes da importação
                                lst = grupos.get(perfil, [])
                                for r in lst:
                                    if r.get("_auto_imovel"):
                                        payload = r["_imovel_payload"]
                                        cod_usado = self._ensure_imovel(
                                            payload["cod_imovel"], payload["nome_imovel"], payload["addr"] or {}
                                        )
                                        r["cod_imovel"] = cod_usado  # garante 001, 002, ...

                                # 🔎 Filtra DUPLICADOS (mesmo participante + mesmo nº doc) consultando o banco
                                mw = self.window()
                                sem_dup = []
                                for r in lst:
                                    num_doc = (r.get("num_doc") or "").replace(" ", "")
                                    cpf = re.sub(r"\D", "", r.get("cpf_cnpj") or "")
                                    if not num_doc or not cpf:
                                        # sem dados para checar => não bloqueia
                                        sem_dup.append(r)
                                        continue
                                    
                                    row = mw.db.fetch_one("""
                                        SELECT 1
                                          FROM lancamento l
                                          JOIN participante p ON p.id = l.id_participante
                                         WHERE REPLACE(COALESCE(l.num_doc,''),' ','') = ?
                                           AND REPLACE(COALESCE(p.cpf_cnpj,''),' ','') = ?
                                         LIMIT 1
                                    """, (num_doc, cpf))

                                    if row:
                                        dups_log.append((
                                            perfil,
                                            num_doc,
                                            cpf,
                                            (r.get("emitente") or r.get("historico") or ""),
                                            r.get("arquivo")
                                        ))
                                    else:
                                        sem_dup.append(r)

                                if not sem_dup:
                                    self.log_msg(f"⚠️ Nenhum lançamento novo para {perfil} (todos já existem).", "warning")
                                    continue
                                
                                # ✍️ Gera um TXT filtrado (SEM duplicados) e importa esse arquivo
                                fname_filtrado = Path(fp).with_name(Path(fp).stem + "_SEM_DUP.txt")
                                with open(fname_filtrado, "w", encoding="utf-8") as f:
                                    for r in sem_dup:
                                        f.write(_make_line(r) + "\n")

                                main_win._import_lancamentos_txt(str(fname_filtrado))
                                if hasattr(main_win, "carregar_lancamentos"): main_win.carregar_lancamentos()
                                if hasattr(main_win, "dashboard"):
                                    try: main_win.dashboard.load_data()
                                    except Exception: pass
                                self.log_msg(f"Importado em: {perfil} ({Path(fname_filtrado).name})", "success")

                            except Exception as e:
                                self.log_msg(f"Falha ao importar em {perfil}: {e}", "error")

                        # 📣 Bloco final — CT-e DUPLICADOS (destacado e tabelado)
                        if dups_log:
                            # título centralizado
                            self._log_section("DUPLICADOS", "🔁")
                        
                            # subtítulo
                            self.log.append("<div style='font-family:monospace; color:#ffd166; text-align:center; margin:2px 0 6px 0;'>MESMO PARTICIPANTE + MESMO Nº DA NOTA</div>")
                        
                            # cabeçalho da tabela
                            hdr = (
                                "PERFIL".ljust(16) + " │ " +
                                "DOC".ljust(10) + " │ " +
                                "CPF/CNPJ".ljust(14) + " │ " +
                                "EMITENTE".ljust(24) + " │ " +
                                "ARQUIVO"
                            )
                            self.log.append("<div style='font-family:monospace;'><b style='color:#ffd166;'>" + hdr + "</b></div>")
                            self.log.append("<div style='font-family:monospace; color:#554a08;'>"
                                            "────────────────┼──────────┼──────────────┼──────────────────────────┼────────────────</div>")
                        
                            # linhas
                            for perfil, num_doc, cpf, emit, arq in dups_log:
                                perf = f"{(perfil or '')[:16]:<16}"
                                doc  = f"{(num_doc or '')[:10]:<10}"
                                cpf2 = f"{(cpf or '')[:14]:<14}"
                                emi  = f"{(emit or '')[:24]:<24}"
                                arq2 = (arq or "")[:16]
                                line = f"{perf} │ {doc} │ {cpf2} │ {emi} │ {arq2}"
                                self.log.append(f"<span style='font-family:monospace; color:#ffd166;'>{line}</span>")
                        
                            # rodapé
                            self.log.append("<div style='text-align:center;color:#2e3d56;font-family:monospace;'>======================</div>")
                        else:
                            self.log_msg("✅ Nenhum CT-e duplicado detectado.", "success")
                        

                        QMessageBox.information(self, "Concluído", "Lançamentos importados.")

            finally:
                self._upd_stats()
                self.btn_cancel.setEnabled(False)

    def importar_lancamentos_txt(self):
        path, _ = QFileDialog.getOpenFileName(self, "Importar Lançamentos (TXT)", "", "Textos (*.txt *.TXT);;Todos (*.*)")
        if not path:
            return
        try:
            main_win = self.window()
            if not main_win or not hasattr(main_win, "_import_lancamentos_txt"):
                QMessageBox.warning(self, "Aviso", "Janela principal não disponível.")
                return
            main_win._import_lancamentos_txt(path)
            if hasattr(main_win, "carregar_lancamentos"): main_win.carregar_lancamentos()
            if hasattr(main_win, "dashboard"):
                try: main_win.dashboard.load_data()
                except Exception: pass
            self.log_msg(f"Lançamentos importados de {os.path.basename(path)}", "success")
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"{e}")

    def _cancelar(self):
        self._cancel = True
        self.btn_cancel.setEnabled(False)
        self.log_msg("Cancelado pelo usuário.", "warning")

# ---------- Log / Stats ----------
    def _upd_stats(self):
        self.lbl_stat_total.setText(f"Total: {self.stat_total}")
        self.lbl_stat_ok.setText(f"Sucesso: {self.stat_ok}")
        self.lbl_stat_err.setText(f"Erros: {self.stat_err}")
        
    def _log_header(self):
        self.log.append("<b style='color:#a9c7ff;'>ARQ".ljust(6) + " │ DATA │ PERFIL          │ DOC │ CIDADE → IMÓVEL │ VALOR │ EMITENTE</b>")
        self.log.append("<span style='color:#2e3d56;'>──────┼──────┼────────────────┼─────┼────────────────┼──────┼────────────────────────────────────</span>")
    
    def log_line(self, rec: dict, origem: str):
        arq = rec['arquivo'][:12]
        data = rec['data_br']
        perf = rec['perfil']
        doc  = rec.get('num_doc') or "-"
        cid  = rec.get('cidade') or "-"
        imv  = rec['cod_imovel']
        val  = f"R$ {int(rec['cent_sai'])/100:,.2f}".replace(",", "X").replace(".", ",").replace("X",".")
        emi  = (rec.get('emitente') or "")
        texto = f"{arq} • {data} • {perf} • Doc {doc} • {cid}→{imv} • {val} • {emi}  ({origem})"
        self.log_msg(texto, "info" if origem == "default" else "success")

    
    def log_msg(self, message: str, msg_type: str = "info"):
        now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        palette = {
            "info":   {"emoji": "ℹ️", "text": "#FFFFFF", "accent": "#3A3C3D", "weight": "500"},
            "success":{"emoji": "✅", "text": "#A7F3D0", "accent": "#2F7D5D", "weight": "700"},
            "warning":{"emoji": "⚠️", "text": "#FFFFFF", "accent": "#8A6D3B", "weight": "600"},
            "error":  {"emoji": "❌", "text": "#FF6B6B", "accent": "#7A2E2E", "weight": "800"},
            "title":  {"emoji": "📌", "text": "#FFFFFF", "accent": "#1e5a9c", "weight": "800"},
            "divider":{"emoji": "",   "text": "",       "accent": "#3A3C3D", "weight": "400"},
        }
        if msg_type == "divider":
            self.log.append('<div style="border-top:1px solid #3A3C3D; margin:10px 0;"></div>')
            return
        p = palette.get(msg_type, palette["info"])
        html = (
            f'<div style="border-left:3px solid {p["accent"]}; padding:6px 10px; margin:2px 0;">'
            f'<span style="opacity:.7; font-family:monospace;">[{now}]</span>'
            f' <span style="margin:0 6px 0 8px;">{p["emoji"]}</span>'
            f'<span style="color:{p["text"]}; font-weight:{p["weight"]};">{message}</span>'
            f'</div>'
        )
        self.log.append(html)

    def _log_section(self, titulo: str, emoji: str = "🚚"):
        self.log.append(
            f"<div style='text-align:center;margin:8px 0 4px 0;'>"
            f"<span style='font-family:monospace;color:#a9c7ff;font-weight:800;'>"
            f"========== {emoji} CT-e {titulo.upper()} =========="
            f"</span></div>"
        )

    def _log_header(self):
        self.log.append(
            "<div style='font-family:monospace;'>"
            "<b style='color:#a9c7ff;'>"
            + "ARQ".ljust(6)
            + " │ DATA │ PERFIL          │ DOC  │ CIDADE → IMÓVEL     │ VALOR       │ EMITENTE"
            + "</b></div>"
        )
        self.log.append(
            "<div style='font-family:monospace;color:#2e3d56;'>"
            "──────┼──────┼────────────────┼──────┼────────────────────┼────────────┼────────────────────────────────────"
            "</div>"
        )

    def _log_row_table(self, rec: dict):
        def cut(s, n): return (str(s or "")[:n])
        def money(cents):
            try:
                v = int(rec.get("cent_sai", 0)) / 100.0
            except Exception:
                v = 0.0
            return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

        arq  = cut(rec.get("arquivo",""), 6).ljust(6)
        data = cut(rec.get("data_br",""), 10).ljust(6)[:6]  # dd-mm
        perf = cut(rec.get("perfil",""), 16).ljust(16)
        doc  = cut(rec.get("num_doc","-"), 6).ljust(6)
        cid  = cut(rec.get("cidade",""), 12)
        imv  = cut(rec.get("cod_imovel",""), 8)
        cid_imv = (f"{cid} → {imv}").ljust(20)
        val  = money(rec).rjust(12)
        emi  = cut(rec.get("emitente",""), 36)

        line = f"{arq} │ {data} │ {perf} │ {doc} │ {cid_imv} │ {val} │ {emi}"
        self.log.append(f"<span style='font-family:monospace;'>{line}</span>")

    def _append_html(self, html: str):
        if not html: return
        self.log.moveCursor(QTextCursor.End)
        self.log.insertHtml(html + "<br/>")
        self.log.moveCursor(QTextCursor.End)
        self.log.ensureCursorVisible()
    
    def _log_clear(self):
        self.log.clear()
        self.log_msg("Log limpo.", "info")

    
    def _log_save(self):
        try:
            out_dir = Path(__file__).parent / "logs"
            out_dir.mkdir(parents=True, exist_ok=True)
            fname = out_dir / f"importador_cte_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(fname, "w", encoding="utf-8") as f:
                f.write(self.log.toPlainText())
            self.log_msg(f"Log salvo em: {fname}", "success")
        except Exception as e:
            self.log_msg(f"Erro ao salvar log: {e}", "error")
    
    # ---------- Config ----------
    def load_config(self) -> dict:
        cfg_dir = Path(__file__).parent / "json"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        cfg = cfg_dir / "config_cte.json"
        if cfg.exists():
            try:
                return json.load(open(cfg, "r", encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def save_config(self):
        cfg_dir = Path(__file__).parent / "json"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        cfg = cfg_dir / "config_cte.json"
        json.dump(self.config, open(cfg, "w", encoding="utf-8"), indent=4, ensure_ascii=False)
