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
    QFileDialog, QMessageBox, QTextEdit, QSizePolicy, QSpacerItem
)

# ===========================
# Aparência / recursos
# ===========================
ICON_PATH = Path(__file__).parent / "image" / "logo.png"

STYLE = """
#card{ background:#1c1f22; border:1px solid #1e5a9c; border-radius:12px; }
#soft{ background:#22262a; border:none; border-radius:10px; }
QLabel, QPushButton, QLineEdit, QTextEdit { color:#E6EAF0; }
QPushButton{ background:#1e5a9c; border:none; border-radius:8px; padding:10px 16px; font-weight:600; }
QPushButton:hover{ background:#164771; }
QPushButton#danger{ background:#C5483D; }
QPushButton#danger:hover{ background:#E4574C; }
QLineEdit{ background:#1b1e21; border:1px solid #294d7a; border-radius:8px; padding:8px; color:#E6EAF0; }
QTextEdit{ background:#0f1113; border:1px solid #2c3340; border-radius:8px; padding:10px; font-family: 'Cascadia Mono','Consolas','Courier New',monospace; }
.stat-chip{ background:#222a33; border:1px solid #355b92; border-radius:999px; padding:6px 10px; font-weight:700;}
"""

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
        self.setStyleSheet(STYLE)

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
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        root.addWidget(self._header_card())
        root.addWidget(self._controls_card())
        root.addWidget(self._log_card())
        root.addStretch()

    def _header_card(self) -> QFrame:
        card = QFrame(); card.setObjectName("card")
        lay = QHBoxLayout(card); lay.setContentsMargins(12, 10, 12, 10); lay.setSpacing(10)

        icon = QLabel()
        if ICON_PATH.exists():
            pix = QPixmap(str(ICON_PATH)).scaled(44, 44, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            icon.setPixmap(pix)
        else:
            icon.setText("🚚")
            icon.setStyleSheet("font-size:34px;")
        lay.addWidget(icon, 0, Qt.AlignVCenter)

        title = QLabel("IMPORTADOR CT-e")
        f = QFont(); f.setPointSize(18); f.setBold(True)
        title.setFont(f)

        self.lbl_last_status = QLabel("Pronto")
        self.lbl_last_status.setStyleSheet("color:#9cc2ff;")
        self.lbl_last_status_time = QLabel(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        self.lbl_last_status_time.setStyleSheet("color:#ccc;")

        lay.addWidget(title, 0, Qt.AlignVCenter)
        lay.addStretch()
        lay.addWidget(self.lbl_last_status)
        lay.addWidget(self.lbl_last_status_time)
        return card

    def _controls_card(self) -> QFrame:
        card = QFrame(); card.setObjectName("card")
        lay = QVBoxLayout(card); lay.setContentsMargins(12, 10, 12, 10); lay.setSpacing(8)

        # Linha 1 — Pasta CT-e (persistente)
        l1 = QHBoxLayout()
        self.ed_dir = QLineEdit(self.config.get("cte_dir", ""))
        self.ed_dir.setPlaceholderText("Pasta onde estão os XMLs de CT-e…")
        btn_browse = QPushButton("Selecionar Pasta"); btn_browse.clicked.connect(self._choose_dir)
        btn_save   = QPushButton("Salvar Pasta");     btn_save.clicked.connect(self._save_dir)
        l1.addWidget(QLabel("📂 Pasta CT-e:")); l1.addWidget(self.ed_dir, 1); l1.addWidget(btn_browse); l1.addWidget(btn_save)

        # Linha 2 — Ações
        l2 = QHBoxLayout()
        self.btn_xmls = QPushButton("Importar XMLs (CT-e) → Gerar TXT")
        self.btn_xmls.clicked.connect(self.importar_xmls_cte)

        self.btn_import_txt = QPushButton("Importar CTe (TXT)")
        self.btn_import_txt.clicked.connect(self.importar_lancamentos_txt)

        self.btn_cancel = QPushButton("Cancelar"); self.btn_cancel.setEnabled(False); self.btn_cancel.setObjectName("danger")
        self.btn_cancel.clicked.connect(self._cancelar)

        l2.addWidget(self.btn_xmls)
        l2.addWidget(self.btn_import_txt)
        l2.addStretch()
        l2.addWidget(self.btn_cancel)

        # Linha 3 — Stats
        l3 = QHBoxLayout()
        self.lbl_stat_total = QLabel("Total: 0"); self.lbl_stat_total.setProperty("class","stat-chip")
        self.lbl_stat_ok    = QLabel("Sucesso: 0"); self.lbl_stat_ok.setProperty("class","stat-chip"); self.lbl_stat_ok.setStyleSheet("QLabel{color:#9be27b;}")
        self.lbl_stat_err   = QLabel("Erros: 0");   self.lbl_stat_err.setProperty("class","stat-chip"); self.lbl_stat_err.setStyleSheet("QLabel{color:#ff8a8a;}")

        l3.addWidget(self.lbl_stat_total); l3.addWidget(self.lbl_stat_ok); l3.addWidget(self.lbl_stat_err); l3.addStretch()

        lay.addLayout(l1); lay.addLayout(l2); lay.addLayout(l3)
        return card

    def _log_card(self) -> QFrame:
        card = QFrame(); card.setObjectName("card")
        lay = QVBoxLayout(card); lay.setContentsMargins(12, 10, 12, 10); lay.setSpacing(8)

        header = QHBoxLayout()
        title = QLabel("📝 Histórico"); f = QFont(); f.setPointSize(12); f.setBold(True); title.setFont(f)
        header.addWidget(title); header.addStretch()
        btn_clear = QPushButton("Limpar Log"); btn_clear.clicked.connect(self._log_clear)
        btn_save  = QPushButton("Salvar Log"); btn_save.clicked.connect(self._log_save)
        header.addWidget(btn_clear); header.addWidget(btn_save)

        box = QFrame(); box.setObjectName("soft")
        box_lay = QVBoxLayout(box); box_lay.setContentsMargins(10, 10, 10, 10)
        self.log = QTextEdit(readOnly=True); self.log.setMinimumHeight(240)
        box_lay.addWidget(self.log)

        lay.addLayout(header); lay.addWidget(box)
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
        self._log_header()
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
                        rec = _read_cte_fields(path)

                        # ===== [NOVO] Garantir participante (usa CNPJ/CPF do CT-e) =====
                        # Preferimos o nome do emitente (transportadora); se vazio, tenta destinatário.
                        _nome_part = (rec.get("emitente") or rec.get("destinatario") or "").strip()
                        if rec.get("cpf_cnpj"):
                            self._ensure_participante(rec["cpf_cnpj"], _nome_part, TIPO_FORNECEDOR)

                        # ------- resolver cod_imovel (apenas Cleuber tem mapeamento) -------
                        cod_imovel = "001"; origem = "default"
                        if rec.get("perfil") == "Cleuber Marcos":
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
                        for perfil, fp in por_perfil.items():
                            try:
                                if perfil not in PERFIS_VALIDOS:
                                    self.log_msg(f"Aviso: perfil '{perfil}' não está na lista de perfis válidos; pulado.", "warning")
                                    continue
                                main_win.switch_profile(perfil)
                                main_win._import_lancamentos_txt(fp)
                                if hasattr(main_win, "carregar_lancamentos"): main_win.carregar_lancamentos()
                                if hasattr(main_win, "dashboard"):
                                    try: main_win.dashboard.load_data()
                                    except Exception: pass
                                self.log_msg(f"Importado em: {perfil} ({Path(fp).name})", "success")
                            except Exception as e:
                                self.log_msg(f"Falha ao importar em {perfil}: {e}", "error")
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
        arq = rec['arquivo'][:6].ljust(6)
        data = rec['data_br'][:5]                 # dd-mm
        perf = rec['perfil'][:16].ljust(16)
        doc  = (rec['num_doc'] or "")[:5].ljust(5)
        cid  = (rec.get('cidade') or "-")[:12].ljust(12)
        imv  = rec['cod_imovel'][:6].ljust(6)
        val  = f"R$ {int(rec['cent_sai'])/100:,.2f}".replace(",", "X").replace(".", ",").replace("X",".")
        emi  = (rec.get('emitente') or "")[:30]
        linha = f"{arq} │ {data} │ {perf} │ {doc} │ {cid}→{imv} │ {val:>10s} │ {emi}"
        cor = "#8fce00" if origem != "default" else "#bcd"
        self._append_html(f"<span style='color:{cor};'>{linha}</span>  <span style='color:#6982b3;'>( {origem} )</span>")
    
    def log_msg(self, text: str, level: str = "info"):
        color = {"info": "#a9c7ff", "success": "#9be27b", "warning": "#ffd166", "error": "#ff8a8a"}.get(level, "#bcd")
        self._append_html(f"<span style='color:{color}; white-space:pre-wrap;'>{text}</span>")
    
    def _append_html(self, html: str):
        if not html: return
        self.log.moveCursor(QTextCursor.End)
        self.log.insertHtml(html + "<br/>")
        self.log.moveCursor(QTextCursor.End)
        self.log.ensureCursorVisible()
    
    def _log_clear(self):
        self.log.clear()
        self._log_header()
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
