# -*- coding: utf-8 -*-
"""
Automação Energia – UI (estilo Importador XML)
- Seleciona pasta base de PDFs
- Separa PDFs por titular (CLEUBER/ADRIANA/LUCAS/GILSON)
- Gera TXT por titular via IA (com OCR fallback)
- Log estilizado + Cancelar/Limpar/Salvar
- Definir/Salvar API Key (sem hardcode)

Dep.: PySide6, pdfplumber, pytesseract, pillow, openai
"""

import os
import sys
import json
import re
import traceback
from pathlib import Path
from datetime import datetime
import tempfile

import pdfplumber
import pytesseract
from PIL import Image

from PySide6.QtCore import (Qt, QThread, Signal, QTimer, QCoreApplication)
from PySide6.QtGui import (QIcon, QFont, QColor, QTextCursor, QPixmap, QCloseEvent, QTextOption)
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QFrame, QLabel, QToolButton,
    QPushButton, QTextEdit, QFileDialog, QMessageBox, QCheckBox, QDialog, QLineEdit,
    QDialogButtonBox, QFormLayout, QGroupBox, QSplitter, QGraphicsDropShadowEffect, QTabWidget,
    QSizePolicy
)
from sistema import _RestTable

# ============================
# Estilo (copiado/adaptado do Importador XML)
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
/* ===== Apenas tela de Configurações (objectName=tab_config) ===== */
QWidget#tab_config QGroupBox {
    background: transparent;              /* sem fundo azul */
    border: 1px solid #11398a;            /* só a linha azul envolta */
    border-radius: 6px;
    margin-top: 14px;                     /* espaço p/ título */
}

QWidget#tab_config QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;        /* título no topo/esq */
    padding: 0 6px;
    /* PONTO-CHAVE: pinta atrás do texto para "apagar" a linha que cruza o título */
    background-color: #1B1D1E;            /* use a MESMA cor do fundo do diálogo */
    color: #ffffff;
}

/* Garante que labels não tenham borda “acidental” */
QWidget#tab_config QLabel {
    border: none;
    background: transparent;
}

/* Se tiver QFrame usado como “card” nessa tela, mantenha só a borda azul */
QWidget#tab_config QFrame,
QWidget#tab_config QFrame#card,
QWidget#tab_config QFrame.card {
    background: transparent;
    border: 1px solid #11398a;
    border-radius: 6px;
}
/* Apenas Configurações */
QWidget#tab_config QLabel { border: none; background: transparent; }
"""

# ============================
# Regras / Mapeamentos (iguais ao seu script)
# ============================
NAMES = ["Cleuber", "Gilson", "Adriana", "Lucas"]

CREDIT_ACCOUNT = "001"

PARTICIPANT_CODES = {"Equatorial":"01543032000104","Companhia Hidroelétrica":"01377555000110","Energisa":"25086034000171"}

FARM_CODES = {
    "Fazenda Frutacc":"001","Fazenda Frutacc II":"001","Fazenda Frutacc III":"001",
    "Fazenda L3":"003","Armazem L3":"003","Fazenda Rio Formoso":"002",
    "Fazenda Siganna":"001","Armazem Frutacc":"006","Lagoa da Confusão":"001",
    "Fazenda Primavera":"004","Fazenda Primaveira":"004","Fazenda Estrela":"008",
    "Fazenda Ilhéus":"004","Sitio Boa Esperança":"007","Fazenda Retiro":"004",
    "Fazenda Barragem Grande":"007","Fazenda Ilha do Formoso":"001","Fazenda Pouso da Anta":"001",
    # opcional: se quiser já mapear explicitamente Formiga:
    "Fazenda Formiga":"001"
}

# Mapeamentos por TITULAR (mesma lógica do Cleuber: casa por NOME em texto/arquivo)
FARM_MAP_GILSON = {
    "FAZENDA RIO FORMOSO": "002",
}

FARM_MAP_ADRIANA = {
    "FAZENDA DUERE": "002",
    "Duere": "002",
    "FAZENDA BARRAGEM GRANDE": "001",
    "MONTIVIDIU DO NORTE": "001",
}

FARM_MAP_LUCAS = {
    "FAZENDA RIO FORMOSO": "002",
}

def _farm_code_by_owner(owner: str, upper_text: str):
    """
    Retorna o código do imóvel conforme OWNER, procurando por chaves (fazenda OU cidade)
    no texto. Match robusto com normalização e borda de palavra.
    Prioridade: match exato por fazenda/cidade > fallback anterior.
    """
    import unicodedata

    def _norm(s: str) -> str:
        s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
        s = re.sub(r"\s+", " ", s).strip().upper()
        return s

    def _word_hit(hay: str, needle: str) -> bool:
        # borda de palavra para evitar falsos positivos (ex.: 'DUERE' não pega 'DUERES')
        pattern = r"(?<!\w)" + re.escape(needle) + r"(?!\w)"
        return re.search(pattern, hay) is not None

    # normaliza todo o texto de busca
    textN = _norm(upper_text)

    # seleciona o mapa do titular
    if owner == "Cleuber":
        mapping = FARM_CODES
    elif owner == "Gilson":
        mapping = FARM_MAP_GILSON
    elif owner == "Adriana":
        mapping = FARM_MAP_ADRIANA
    elif owner == "Lucas":
        mapping = FARM_MAP_LUCAS
    else:
        mapping = {}

    # 1) Tenta hits por chave (fazenda/cidade) normalizada, com borda de palavra
    best_code = None
    for key, code in mapping.items():
        kN = _norm(key)
        if not kN:
            continue
        if _word_hit(textN, kN):
            best_code = code
            break

    if best_code:
        return best_code

    # 2) Fallback: comportamento antigo (substring simples, já normalizada)
    for key, code in mapping.items():
        kN = _norm(key)
        if kN and kN in textN:
            return code

    return None

def _prompt_base_for(owner: str) -> str:
    """
    Gera o prompt com MAPEAMENTO DE FAZENDAS específico do titular atual.
    - Cleuber: usa FARM_CODES (mapeamento global existente)
    - Gilson/Adriana/Lucas: usam FARM_MAP_[OWNER] (mapeamentos separados)
    A IA deve usar EXCLUSIVAMENTE o mapa daquele titular para 'codigo_imovel'.
    """
    if owner == "Cleuber":
        farms_map = FARM_CODES
    elif owner == "Gilson":
        farms_map = FARM_MAP_GILSON
    elif owner == "Adriana":
        farms_map = FARM_MAP_ADRIANA
    elif owner == "Lucas":
        farms_map = FARM_MAP_LUCAS
    else:
        farms_map = {}  # fallback

    return (
        "Você é um leitor especializado em talões de energia.\n"
        "Extraia e retorne apenas um JSON com os campos:\n"
        "  - data_vencimento (DDMMYYYY) — data de vencimento, não emissão.\n"
        "    Priorize o nome do arquivo; se não tiver, busque no texto.\n"
        "  - valor (0,00) — total a pagar, precedido de \"R$\".\n"
        "  - numero_nf — número da nota fiscal (NF, Nota Fiscal Nº etc.).\n"
        "    Retire pontos e traços do número.\n"
        "  - nome_fornecedor — texto após NF ou label \"Fornecedor:\".\n"
        "  - codigo_participante — mapeie:\n"
        "      Equatorial->01543032000104, Companhia Hidroelétrica->01377555000110, Energisa->25086034000171\n"
        "  - codigo_imovel — CONSIDERE o titular atual: " + owner.upper() + ".\n"
        "      Use EXCLUSIVAMENTE os mapeamentos abaixo (não use outros; se não encontrar, deixe vazio):\n"
        + json.dumps(farms_map, indent=4, ensure_ascii=False) + "\n"
        "Formato: JSON puro, sem markup."
    )

# ============================
# Helpers de OpenAI (SDK nova/antiga)
# ============================
# ============================
# Helpers de OpenAI (SDK nova/antiga) — VERSÃO DINÂMICA (lê a chave a cada chamada)
# ============================
def _get_run_chat():
    """
    Retorna função run_chat(model, messages, temperature=0) que
    instancia o cliente em CADA chamada, lendo a OPENAI_API_KEY atual.
    Evita 401 quando a chave é definida depois que o módulo já foi importado.
    """
    try:
        # SDK nova
        from openai import OpenAI

        def run_chat(model, messages, temperature=0):
            key = os.environ.get("OPENAI_API_KEY") or ""
            if not key:
                raise RuntimeError("OPENAI_API_KEY não definida")
            # cria o cliente AGORA, já com a chave correta
            client = OpenAI(api_key=key)
            resp = client.chat.completions.create(
                model=model, messages=messages, temperature=temperature
            )
            return resp.choices[0].message.content

        return run_chat

    except Exception:
        # SDK antiga
        import openai as old_openai

        def run_chat(model, messages, temperature=0):
            key = os.environ.get("OPENAI_API_KEY") or ""
            if not key:
                raise RuntimeError("OPENAI_API_KEY não definida")
            old_openai.api_key = key
            resp = old_openai.ChatCompletion.create(
                model=model, messages=messages, temperature=temperature
            )
            msg = resp.choices[0].message
            return msg["content"] if isinstance(msg, dict) else msg.content

        return run_chat

RUN_CHAT = _get_run_chat()

# ============================
# Dialogs (Config/Key)
# ============================
class ApiKeyDialog(QDialog):
    def __init__(self, api_key_default="", parent=None):
        super().__init__(parent)
        self.setWindowTitle("🔑 Definir API da OpenAI")
        self.setModal(True)
        self.setFixedSize(560, 180)

        lay = QVBoxLayout(self)
        form = QFormLayout()
        self.key_edit = QLineEdit(api_key_default)
        self.key_edit.setPlaceholderText("sk-...")
        self.key_edit.setEchoMode(QLineEdit.Password)
        form.addRow("API Key:", self.key_edit)
        lay.addLayout(form)

        btns = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        lay.addWidget(btns)

    def get_key(self) -> str:
        return self.key_edit.text().strip()


class ConfigDialog(QDialog):
    def __init__(self, cfg: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("⚙️ Configurações")
        self.setObjectName("tab_config")  # aplica os estilos só neste diálogo
        self.setModal(True)
        self.setFixedSize(640, 260)
        self.cfg = cfg or {}
        self.setObjectName("tab_config")

        lay = QVBoxLayout(self)
        grp = QGroupBox("Caminhos e Opções")
        form = QFormLayout(grp)
        
        # empurra o conteúdo do groupbox para baixo (L, T, R, B)
        form.setContentsMargins(12, 18, 12, 12)
        form.setVerticalSpacing(10)  # opcional: distância entre linhas

        self.base_dir_edit = QLineEdit(self.cfg.get("base_dir", ""))
        btn_base = QPushButton("Procurar")
        btn_base.clicked.connect(self._browse_base)
        row = QHBoxLayout(); row.addWidget(self.base_dir_edit); row.addWidget(btn_base)
        lbl_base = QLabel("Pasta Base (ENERGIA):")
        lbl_base.setObjectName("lblBaseEnergia")
        lbl_base.setFrameShape(QFrame.NoFrame)                 # remove qualquer frame nativo
        lbl_base.setStyleSheet("border:none; background:transparent; padding:0;")
        lbl_base.setBuddy(self.base_dir_edit)                  # acessibilidade (Alt+letra no Win)
        form.addRow(lbl_base, row)

        self.chk_ocr = QCheckBox("Usar OCR quando não houver texto extraível")
        self.chk_ocr.setChecked(bool(self.cfg.get("use_ocr", True)))
        form.addRow("", self.chk_ocr)

        lay.addWidget(grp)

        btns = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept); btns.rejected.connect(self.reject)
        lay.addWidget(btns)

    def _browse_base(self):
        folder = QFileDialog.getExistingDirectory(self, "Selecione a pasta base (ENERGIA)")
        if folder:
            self.base_dir_edit.setText(folder)

    def get_config(self) -> dict:
        return {
            "base_dir": self.base_dir_edit.text().strip(),
            "use_ocr": self.chk_ocr.isChecked()
        }

# ============================
# Global Progress (busy / cancelar)
# ============================
class GlobalProgress:
    _parent = None
    _busy = False

    @classmethod
    def begin(cls, text: str, parent=None):
        cls._parent = parent
        cls._busy = True

    @classmethod
    def end(cls):
        cls._busy = False

    @classmethod
    def is_busy(cls) -> bool:
        return cls._busy

# ============================
# Workers (QThread)
# ============================
class WorkerSignals:
    log       = Signal(str, str)   # (mensagem, tipo)
    stats     = Signal(int, int, int)  # total, ok, err
    finished  = Signal(str)        # status final
    step      = Signal(int, int)   # current, total


class BaseWorker(QThread):
    log_sig = Signal(str, str)
    stats_sig = Signal(int, int, int)
    finished_sig = Signal(str)
    step_sig = Signal(int, int)

    def __init__(self, base_dir: str, use_ocr: bool, parent=None):
        super().__init__(parent)
        self.base_dir = base_dir
        self.use_ocr = use_ocr
        self._cancel = False
        self.total = 0
        self.ok = 0
        self.err = 0

    def cancel(self):
        self._cancel = True

    # helpers
    def _emit_log(self, msg: str, kind: str = "info"):
        self.log_sig.emit(msg, kind)

    def _emit_stats(self):
        self.stats_sig.emit(self.total, self.ok, self.err)

    def _emit_step(self, i: int, n: int):
        self.step_sig.emit(i, n)

    # OCR helper
    def _ocr_pdf(self, path: str) -> str:
        text = ''
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                img = page.to_image(resolution=300).original
                text += pytesseract.image_to_string(img, lang='por') + '\n'
        return text


class SeparadorWorker(BaseWorker):
    """
    Implementa o 'Separador de Titular' dentro da UI.
    Caminha por base_dir (recursivo), lê PDFs, classifica com IA, move para pastas NAMES.
    """
    def run(self):
        try:
            run_chat = RUN_CHAT
            model = "gpt-4o-mini"
            found = []

            # Levanta lista de PDFs/IMAGENS (sem IA)
            alvos = []
            EXTS = {".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".webp"}
            for root, _, files in os.walk(self.base_dir):
                for f in files:
                    if Path(f).suffix.lower() in EXTS:
                        alvos.append(Path(root) / f)
            
            self.total = len(alvos)
            self._emit_stats()
            if self.total == 0:
                self._emit_log("Nenhum arquivo PDF/Imagem encontrado na pasta base.", "warning")
                self.finished_sig.emit("Nenhum arquivo para processar")
                return
            
            self._emit_log(f"Iniciando separação por titular ({self.total} arquivos)...", "title")

            owners_seen = set()
            
            for i, any_path in enumerate(alvos, start=1):
                if self._cancel:
                    self._emit_log("Processo cancelado pelo usuário.", "warning")
                    self.finished_sig.emit("Cancelado")
                    return
            
                self._emit_step(i, self.total)
                self._emit_log(f"Processando: {any_path.name}", "info")
            

                # Extrai texto (PDF ou Imagem) — sem usar IA
                try:
                    ext = any_path.suffix.lower()
                    text = ""
                    if ext == ".pdf":
                        with pdfplumber.open(any_path) as pdf:
                            text = "\n".join((p.extract_text() or "") for p in pdf.pages)
                        if not text.strip() and self.use_ocr:
                            self._emit_log("Sem texto extraível. Usando OCR...", "warning")
                            text = self._ocr_pdf(str(any_path))
                    else:
                        # imagens: usa OCR direto
                        img = Image.open(any_path)
                        text = pytesseract.image_to_string(img, lang="por")
                except Exception as e:
                    self._emit_log(f"Erro ao ler arquivo: {e}", "error")
                    self.err += 1; self._emit_stats()
                    continue
                
                if not text.strip():
                    self._emit_log("Sem texto extraível e OCR desativado. Pulando.", "warning")
                    self.err += 1; self._emit_stats()
                    continue
                
                # Detecta titular por ocorrência simples no nome do arquivo OU no texto
                txt_lower   = text.lower()
                fname_lower = any_path.name.lower()
                final = "UNKNOWN"
                
                for n in NAMES:
                    nlow = n.lower()
                    if nlow in fname_lower or nlow in txt_lower:
                        final = n
                        break
                    
                self._emit_log(f"Identificado: {final}", "info")

                # Cabeçalho por perfil (uma vez por titular)
                if final != "UNKNOWN" and final not in owners_seen:
                    owners_seen.add(final)
                    self._emit_log("", "divider")      # divisor visual
                    self._emit_log(final, "bigtitle")
                
                if final != "UNKNOWN":
                    dest = Path(self.base_dir) / final
                    try:
                        dest.mkdir(exist_ok=True, parents=True)
                        new_path = dest / any_path.name
                        any_path.replace(new_path)
                        self._emit_log(f"Movido para: {new_path}", "success")
                        self.ok += 1; self._emit_stats()
                    except Exception as e:
                        self._emit_log(f"Erro ao mover arquivo: {e}", "error")
                        self.err += 1; self._emit_stats()
                else:
                    self._emit_log("Nome não reconhecido; arquivo permanece onde está.", "warning")
                    self.err += 1; self._emit_stats()
                

            self._emit_log("Separação concluída.", "success")
            self.finished_sig.emit("Concluído")
        except Exception:
            self._emit_log(f"Falha geral no separador:\n{traceback.format_exc()}", "error")
            self.finished_sig.emit("Erro")


class GerarTXTWorker(BaseWorker):
    """
    Implementa o 'Automacao Energia txt' dentro da UI:
    - Para cada subpasta (Cleuber/Gilson/Adriana/Lucas)
    - Lê PDFs, extrai texto (ou OCR), chama IA para JSON, gera linhas e grava NOME.txt
    - Gera log detalhado por pasta (como no script original)
    """
    def run(self):
        try:
            run_chat = RUN_CHAT
            model = "gpt-4o-mini"
            base_dir = Path(self.base_dir)

            # Subpastas alvo
            subfolders = [base_dir / n for n in NAMES if (base_dir / n).is_dir()]
            self.total = sum(len([f for f in os.listdir(sf) if f.lower().endswith(".pdf")]) for sf in subfolders)
            self._emit_stats()

            if self.total == 0:
                self._emit_log("Nenhum PDF encontrado nas subpastas (Cleuber/Gilson/Adriana/Lucas).", "warning")
                self.finished_sig.emit("Nenhum PDF")
                return

            # ===== Helpers locais =====
            def _fmt_money_br(v: float) -> str:
                s = f"{v:,.2f}"
                return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")

            def _to_dd_mm_aaaa(s: str) -> str:
                s = (s or "").strip()
                m = re.match(r"^(\d{2})[/-](\d{2})[/-](\d{4})$", s)
                if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
                m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
                if m: return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
                m = re.match(r"^(\d{2})(\d{2})(\d{4})$", s)
                if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
                return s

            def _fmt_hms(dt: datetime) -> str:
                return dt.strftime("%H:%M:%S")

            def _secs(delta) -> int:
                return int(delta.total_seconds())

            # ===== Cabeçalho da sessão (será reimpresso no resumo final com total/tempo) =====
            session_start = datetime.now()
            n_sub = len(subfolders)
            self._emit_log(f"🧾 <b>Geração de TXT — {session_start.strftime('%d/%m/%Y')}</b>", "raw")
            self._emit_log(f"⏳ <b>Início:</b> {_fmt_hms(session_start)} • <b>{n_sub} subpastas</b> • <b>{self.total} PDFs</b>", "raw")
            self._emit_log("", "divider")

            processed = 0
            grand_total_val = 0.0
            generated_txts = []

            # ===== Processa por titular (subpasta) — bloco inteiro é BUFFERIZADO e impresso ao final =====
            for sf in subfolders:
                if self._cancel:
                    self._emit_log("Processo cancelado pelo usuário.", "warning")
                    self.finished_sig.emit("Cancelado")
                    return

                name = sf.name
                out_txt  = base_dir / f"{name}.txt"
                log_path = sf / f"{name}_leitura_detalhada.txt"
                lines = []
                pdfs = sorted([f for f in os.listdir(sf) if f.lower().endswith(".pdf")])

                # Buffer do bloco para imprimir “de uma vez”, já com tempos calculados
                blk = []
                prof_start = datetime.now()
                talao_idx = 0
                subtotal_val = 0.0

                with open(log_path, "w", encoding="utf-8") as log_f:
                    for fname in pdfs:
                        if self._cancel:
                            self._emit_log("Cancelado pelo usuário.", "warning")
                            self.finished_sig.emit("Cancelado")
                            return

                        processed += 1
                        self._emit_step(processed, self.total)

                        talao_idx += 1
                        t_start = datetime.now()

                        path = sf / fname
                        # Extrai texto
                        try:
                            with pdfplumber.open(path) as pdf:
                                full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
                        except Exception as e:
                            self.err += 1; self._emit_stats()
                            blk.append(f"<div>❌ Erro ao abrir PDF: {fname} — {e}</div>")
                            continue

                        if not full_text.strip():
                            if self.use_ocr:
                                try:
                                    full_text = self._ocr_pdf(str(path))
                                except Exception as e:
                                    self.err += 1; self._emit_stats()
                                    blk.append(f"<div>❌ OCR falhou: {fname} — {e}</div>")
                                    continue
                            else:
                                self.err += 1; self._emit_stats()
                                blk.append(f"<div>⚠️ Sem texto extraível e OCR desativado: {fname}</div>")
                                continue

                        # Log técnico para auditoria
                        sep = "="*60
                        prompt_owner = _prompt_base_for(name)
                        log_f.write(f"\n{sep}\nPROMPT ENVIADO PARA IA ({name}):\n{prompt_owner}\n\nNome do arquivo: {fname}\nConteúdo extraído:\n{full_text}\n")

                        # Chamada à IA
                        try:
                            json_text = run_chat(model, [
                                {"role":"system","content":prompt_owner},
                                {"role":"user","content":f"Nome do arquivo: {fname}\nConteúdo extraído:\n{full_text}"}
                            ], temperature=0)
                            data = json.loads(json_text)
                            if 'numero_nf' in data and isinstance(data['numero_nf'], str):
                                data['numero_nf'] = re.sub(r"[.\-]", "", data['numero_nf'])
                            log_f.write("\nRESPOSTA DA IA (JSON):\n" + json.dumps(data, indent=4, ensure_ascii=False) + "\n")
                        except Exception as e:
                            self.err += 1; self._emit_stats()
                            blk.append(f"<div>❌ Erro IA em {fname}: {e}</div>")
                            continue

                        # Normaliza campos
                        df = str(data.get("data_vencimento", "") or "")
                        vf_raw = str(data.get("valor", "") or "")

                        # valor numérico
                        vf_clean = re.sub(r"R\$\s*", "", vf_raw).replace(".", "")
                        try:
                            num = float(vf_clean.replace(',', '.'))
                            vf = f"{num:.2f}".replace('.', ',')
                            num_val = float(vf.replace(",", "."))
                        except Exception:
                            vf = vf_clean
                            try:
                                num_val = float(vf_clean.replace(",", "."))
                            except Exception:
                                num_val = 0.0

                        nf = str(data.get("numero_nf", "") or "")
                        pc = str(data.get("codigo_participante", "") or "")
                        ic = str(data.get("codigo_imovel", "") or "")
                        txt_upper = (full_text + " " + fname).upper()
                        code_match = _farm_code_by_owner(name, txt_upper)
                        if code_match:
                            ic = code_match
                        else:
                            ic = ic or "001"

                        if vf in ("0", "0,00", "") or num_val == 0.0:
                            blk.append(f"<div>⚠️ Valor zero detectado em <i>{fname}</i>; pulando.</div>")
                            continue

                        # Data para LCDPR + exibição
                        data_br = _to_dd_mm_aaaa(df)
                        data_show = (data_br or "").replace("-", "/")

                        # Monta linha do TXT
                        if len(df) == 8:
                            mes, ano = df[2:4], df[4:8]
                            history = f"TALAO DE ENERGIA {mes}/{ano}"
                        else:
                            history = "TALAO DE ENERGIA"

                        tipo_doc = "4"
                        tipo_lanc = "2"
                        valor_entrada = "000"
                        valor_saida   = f"{num_val:.2f}".replace(".", ",")
                        saldo_dummy   = "000"
                        natureza      = "N"

                        cols = [
                            data_br,         # 1 data (DD-MM-AAAA)
                            ic,              # 2 cod_imovel
                            CREDIT_ACCOUNT,  # 3 cod_conta
                            nf,              # 4 num_doc
                            tipo_doc,        # 5 tipo_doc
                            history,         # 6 historico
                            pc,              # 7 cpf_cnpj/participante
                            tipo_lanc,       # 8 tipo_lanc
                            "000",           # 9 valor_entrada
                            valor_saida,     # 10 valor_saida
                            "000",           # 11 saldo
                            "N"              # 12 natureza
                        ]
                        lines.append("|".join(str(x or "") for x in cols))

                        # Stats
                        self.ok += 1; self._emit_stats()
                        subtotal_val += num_val
                        grand_total_val += num_val

                        # Bloco de exibição do talão (CLEAN)
                        elapsed_now = _fmt_hms(datetime.now())
                        blk.append(
                            "<div style='border:1px solid #3A3C3D; border-radius:8px; padding:10px 12px; margin:10px 0;'>"
                            f"<div style='font-weight:700; margin-bottom:6px;'>📄 Talão {talao_idx} "
                            f"<span style='opacity:.75; font-weight:500;'>— {fname}</span></div>"
                            f"<div>🗓️ Venc.: <b>{data_show}</b> &nbsp;•&nbsp; 💰 <b>{_fmt_money_br(num_val)}</b> &nbsp;•&nbsp; 🧾 NF: <b>{nf}</b></div>"
                            f"<div>🏢 Fornecedor: <b>{str(data.get('nome_fornecedor','') or '').strip()}</b></div>"
                            f"<div>🧩 Participante: <b>{pc}</b> &nbsp;•&nbsp; 🏠 Imóvel: <b>{ic}</b></div>"
                            f"<div style='margin-top:6px; opacity:.85;'>✅ OK <b>{elapsed_now}</b> &nbsp;•&nbsp; 📌 Talão processado</div>"
                            "</div>"
                        )
                        # Espaçador extra entre talões
                        blk.append("<div style='height:14px;'></div>")

                    # Salva TXT por titular
                    try:
                        with open(out_txt, "w", encoding="utf-8") as f:
                            f.write("\n".join(lines))
                        generated_txts.append((name, str(out_txt)))
                    except Exception as e:
                        self.err += 1; self._emit_stats()
                        blk.append(f"<div>❌ Falha ao salvar TXT de {name}: {e}</div>")

                # Imprime bloco do titular com cabeçalho e subtotal
                prof_end = datetime.now()
                prof_secs = _secs(prof_end - prof_start)
                head = (
                    "<div style='border-top:1px solid #3A3C3D; margin:14px 0 10px 0;'></div>"
                    f"<div style='font-weight:800; font-size:14px; margin:2px 0 8px 0;'>"
                    f"👤 {name} &nbsp;—&nbsp; ⏱️ <b>{prof_secs}s</b> "
                    f"(<span style='opacity:.8;'>{_fmt_hms(prof_start)} → {_fmt_hms(prof_end)}</span>)"
                    "</div>"
                )

                self._emit_log(head, "raw")
                for piece in blk:
                    self._emit_log(piece, "raw")

                # Rodapé do titular
                txt_tuple = [t for t in generated_txts if t[0] == name]
                txt_line = f"🗂️ TXT gerado: <code>{txt_tuple[-1][1]}</code> <b>({_fmt_hms(prof_end)} | +{_secs(prof_end - session_start)}s)</b>" if txt_tuple else ""
                if txt_line:
                    self._emit_log(txt_line, "raw")
                self._emit_log(f"💵 <b>Subtotal {name}:</b> <b>{_fmt_money_br(subtotal_val)}</b>", "raw")
                self._emit_log("", "divider")

            # ===== Resumo final =====
            session_end = datetime.now()
            total_secs = _secs(session_end - session_start)
            txt_list = ", ".join([n for n, _ in generated_txts]) or "—"
            self._emit_log(
                "<pre style='font-family:monospace; color:#E0E0E0; margin:6px 0;'>"
                "================================================================\n"
                "🏁 <b>Resumo Final</b>\n"
                "</pre>", "raw"
            )
            self._emit_log(f"• 📦 <b>PDFs processados:</b> {self.total}", "raw")
            self._emit_log(f"• 🧾 <b>TXTs gerados:</b> {len(generated_txts)} ({txt_list})", "raw")
            self._emit_log(f"• ⏱️ <b>Tempo total:</b> <b>{total_secs}s</b> ({_fmt_hms(session_start)} → {_fmt_hms(session_end)})", "raw")
            self._emit_log(f"• 💰 <b>Valor total processado:</b> <b>{_fmt_money_br(grand_total_val)}</b>", "raw")
            self._emit_log("✅ <b>Status:</b> Concluído com sucesso.", "raw")

            self.finished_sig.emit("Concluído")
        except Exception:
            self._emit_log(f"Falha geral na geração de TXT:\n{traceback.format_exc()}", "error")
            self.finished_sig.emit("Erro")

# ============================
# Interface Principal (estilo Importador XML)
# ============================
class AutomacaoEnergiaUI(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Automação Talões de Energia")
        if ICON_PATH.exists():
            self.setWindowIcon(QIcon(str(ICON_PATH)))

        self.setStyleSheet(STYLE_SHEET)
        self._last_action = None

        # Estado
        self.cfg = self._load_config()
        self.worker = None
        self.stat_total = 0
        self.stat_ok = 0
        self.stat_err = 0

        # ROOT LAYOUT
        root = QVBoxLayout(self)
        root.setContentsMargins(14,14,14,14)
        root.setSpacing(12)

        # Header
        header = self._build_header()
        root.addWidget(header)

        # Cards Topo
        top_row = QHBoxLayout()
        top_row.setSpacing(12)
        top_row.addWidget(self._build_controls_card(), 3)
        top_row.addWidget(self._build_stats_card(), 2)
        root.addLayout(top_row)

        # Log ocupa o resto da tela (mesmo comportamento do Importar Dump)
        log_card = self._build_log_card()
        root.addWidget(log_card, 1)  # stretch=1

        # Rodapé
        footer = QLabel("⚡ Automação de Talões de Energia — v1.0")
        footer.setAlignment(Qt.AlignCenter)
        footer.setStyleSheet("font-size:11px; color:#7F7F7F; padding-top:4px;")
        root.addWidget(footer)

        # aplica OPENAI_API_KEY se existir em config
        if self.cfg.get("api_key"):
            os.environ["OPENAI_API_KEY"] = self.cfg["api_key"]

    # ---------- UI builders ----------
    def _add_shadow(self, widget: QWidget, radius=16, blur=22, color=QColor(0,0,0,60), y_offset=6):
        eff = QGraphicsDropShadowEffect(self)
        eff.setBlurRadius(blur)
        eff.setColor(color)
        eff.setOffset(0, y_offset)
        widget.setGraphicsEffect(eff)
        widget.setStyleSheet(widget.styleSheet() + f"; border-radius:{radius}px;")

    def _build_header(self) -> QFrame:
        header = QFrame()
        header.setStyleSheet("QFrame{border:1px solid #1e5a9c; border-radius:16px;}")
        lay = QHBoxLayout(header); lay.setContentsMargins(18,16,18,16); lay.setSpacing(14)

        icon = QLabel()
        if ICON_PATH.exists():
            pix = QPixmap(str(ICON_PATH)).scaled(44,44, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            icon.setPixmap(pix)
        else:
            icon.setText("⚡"); icon.setStyleSheet("font-size:34px; border:none;")
        lay.addWidget(icon, 0, Qt.AlignVCenter)

        title = QLabel("AUTOMAÇÃO ENERGIA – TXT & CLASSIFICAÇÃO")
        f = QFont(); f.setPointSize(20); f.setBold(True)
        title.setFont(f)
        
        subtitle = QLabel("Separe por titular, gere TXT e acompanhe tudo em tempo real.")
        
        title.setStyleSheet("border:none;")
        subtitle.setStyleSheet("border:none;")
        
        # ↓ Impede alargamento: deixa o texto quebrar dentro do espaço disponível
        title.setWordWrap(True)
        subtitle.setWordWrap(True)
        title.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        subtitle.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        
        box = QVBoxLayout()
        box.addWidget(title)
        box.addWidget(subtitle)
        lay.addLayout(box, 1)

        # ações rápidas
        btn_cfg = QToolButton(); btn_cfg.setText("⚙️ Configurar")
        btn_cfg.clicked.connect(self._open_config)

        btn_key = QToolButton(); btn_key.setText("🔑 Definir API da OpenAI")
        btn_key.clicked.connect(self._open_key)

        btn_close = QToolButton()
        btn_close.setText("✖ Fechar")
        btn_close.clicked.connect(self._close_self_tab)

        right = QHBoxLayout(); right.setSpacing(8)
        right.addWidget(btn_cfg); right.addWidget(btn_key); right.addWidget(btn_close)
        lay.addLayout(right, 0)

        self._add_shadow(header, radius=16, blur=24, color=QColor(0,0,0,50), y_offset=5)
        return header

    def _close_self_tab(self):
        parent = self.parent()
        while parent and not isinstance(parent, QTabWidget):
            parent = parent.parent()
        if parent:  # dentro de um QTabWidget
            idx = parent.indexOf(self)
            if idx != -1:
                parent.removeTab(idx)
        else:       # janela solta
            self.close()

    def _build_controls_card(self) -> QFrame:
        card = QFrame(); card.setStyleSheet("QFrame{border:1px solid #1e5a9c; border-radius:12px;}")
        lay = QVBoxLayout(card); lay.setContentsMargins(14,12,14,12); lay.setSpacing(10)

        actions = QHBoxLayout(); actions.setSpacing(10)

        self.btn_separar = QPushButton("🧭 Separar por Titular")
        self.btn_separar.setObjectName("success")
        self.btn_separar.clicked.connect(self._start_separador)
        actions.addWidget(self.btn_separar)

        self.btn_gerar = QPushButton("🧾 Gerar TXT dos Talões")
        self.btn_gerar.clicked.connect(self._start_gerar_txt)
        actions.addWidget(self.btn_gerar)
        
        # INSIRA LOGO APÓS self.btn_gerar ... antes do btn_cancel:
        self.btn_importar_txt = QPushButton("📥 Importar TXT do Talão")
        self.btn_importar_txt.clicked.connect(self._importar_txt_manual)
        actions.addWidget(self.btn_importar_txt)

        self.btn_cancel = QPushButton("⛔ Cancelar")
        self.btn_cancel.setObjectName("danger")
        self.btn_cancel.setEnabled(False)
        self.btn_cancel.clicked.connect(self._cancel_worker)
        actions.addWidget(self.btn_cancel)

        actions.addStretch()

        # Log utils
        self.btn_log_clear = QToolButton(); self.btn_log_clear.setText("🧹 Limpar Log")
        self.btn_log_clear.setStyleSheet("QToolButton{background:#0d1b3d; border:1px solid #1e5a9c; border-radius:8px; padding:6px 10px;} QToolButton:hover{background:#123764;}")
        self.btn_log_clear.clicked.connect(self._log_clear)
        actions.addWidget(self.btn_log_clear)

        self.btn_log_save = QToolButton(); self.btn_log_save.setText("💾 Salvar Log")
        self.btn_log_save.setStyleSheet("QToolButton{background:#0d1b3d; border:1px solid #1e5a9c; border-radius:8px; padding:6px 10px;} QToolButton:hover{background:#123764;}")
        self.btn_log_save.clicked.connect(self._log_save)
        actions.addWidget(self.btn_log_save)

        lay.addLayout(actions)

        # options
        opts = QHBoxLayout(); opts.setSpacing(12)
        self.chk_ocr = QCheckBox("Usar OCR quando necessário")
        self.chk_ocr.setChecked(bool(self.cfg.get("use_ocr", True)))
        opts.addWidget(self.chk_ocr)
        opts.addStretch()
        lay.addLayout(opts)

        self._add_shadow(card, radius=14, blur=20, color=QColor(0,0,0,45), y_offset=4)
        return card

    def _importar_txt_manual(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Selecione o TXT do Talão", "", "TXT (*.txt)"
        )
        if not path:
            return
        try:
            main_win = self.window()  # assume que a tela roda dentro do MainWindow
            # o importador detecta 11/12 colunas e delega corretamente
            main_win._import_lancamentos_txt(path)
            main_win.carregar_lancamentos()
            main_win.dashboard.load_data()
            QMessageBox.information(self, "OK", "Lançamentos importados com sucesso.")
            self.log_msg(f"Importado manualmente: {path}", "success")
        except Exception as e:
            QMessageBox.warning(self, "Falha", str(e))
            self.log_msg(f"Falha ao importar: {e}", "error")

    def _build_stats_card(self) -> QFrame:
        card = QFrame(); card.setObjectName("statsCard")
        card.setStyleSheet("#statsCard{border:1px solid #1e5a9c; border-radius:14px;} #statsCard *{border:none; background:transparent;}")
        lay = QVBoxLayout(card); lay.setContentsMargins(14,12,14,12); lay.setSpacing(6)

        title = QLabel("📊 Último Status da Sessão")
        f = QFont(); f.setPointSize(12); f.setBold(True)
        title.setFont(f)
        lay.addWidget(title)

        self.lbl_last_status = QLabel("—")
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

        self._add_shadow(card, radius=14, blur=20, color=QColor(0,0,0,45), y_offset=4)
        return card

    def _make_chip(self, label: str, bg: str, fg: str) -> QLabel:
        w = QLabel(f"{label}: 0")
        w.setAlignment(Qt.AlignCenter)
        w.setStyleSheet(f"QLabel {{ background:{bg}; color:{fg}; border-radius:10px; padding:8px 12px; font-weight:600; }}")
        return w

    def _build_log_card(self) -> QFrame:
        card = QFrame(); card.setObjectName("logCard")
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
        self.log.setFrameStyle(QFrame.NoFrame)
        
        # ocupa todo o espaço, igual ao Importar Dump
        self.log.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.log.setMinimumHeight(0)
        self.log.setMaximumHeight(16777215)
        
        # zera acolchoamentos para a 1ª linha não “nascer” no meio
        self.log.setStyleSheet(
            "QTextEdit{background:transparent; border:none; padding:0; margin:0;}"
            "QTextEdit::viewport{background:transparent; border:none; padding:0; margin:0;}"
        )
        self.log.document().setDocumentMargin(2)
        self.log.setViewportMargins(0, 0, 0, 0)
        self.log.setContentsMargins(0, 0, 0, 0)
        
        # mesmas opções de quebra que você já usa
        self.log.setLineWrapMode(QTextEdit.WidgetWidth)
        self.log.setWordWrapMode(QTextOption.WrapAnywhere)
        self.log.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.log.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        
        body_lay.addWidget(self.log, 1)
        
        # garante que a primeira mensagem apareça colada no topo
        self.log.clear()
        self.log.moveCursor(QTextCursor.Start)
        if self.log.verticalScrollBar():
            self.log.verticalScrollBar().setValue(self.log.verticalScrollBar().minimum())
        
        # Preencher todo o card (igual ao importar-dump)
        card.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        lay.setStretch(0, 0)   # título
        lay.setStretch(1, 1)   # corpo
        body_lay.setStretch(0, 1)  # QTextEdit ocupa o corpo

        lay.addWidget(body)
        return card

    # ---------- Log helpers (mesmo estilo do Importador XML) ----------
    def log_msg(self, message: str, msg_type: str = "info"):
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
        # Mensagem crua (HTML pronto), sem carimbo [hora] e sem moldura
        if msg_type == "raw":
            self.log.append(message)
            sb = self.log.verticalScrollBar()
            if sb: sb.setValue(sb.maximum())
            return
        
        # Título grande com faixas e ícone de perfil
        if msg_type == "bigtitle":
            self.log.append(
                "<pre style='font-family:monospace; color:#E0E0E0; margin:6px 0;'>"
                "================================================================\n"
                f"👤 {message}\n"
                "================================================================"
                "</pre>"
            )
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
        # mantém no topo quando há só 1ª/2ª linha; senão, rola para o fim
        sb = self.log.verticalScrollBar()
        if sb:
            if self.log.document().blockCount() <= 2:
                sb.setValue(0)
            else:
                sb.setValue(sb.maximum())

    def _log_clear(self):
        self.log.clear()                      # sem HTML “fantasma”
        self.log.moveCursor(QTextCursor.Start)
        if self.log.verticalScrollBar():
            self.log.verticalScrollBar().setValue(self.log.verticalScrollBar().minimum())
        self.log_msg("Log limpo.", "info")


    def _log_save(self):
        try:
            out_dir = Path(__file__).parent / "logs"
            out_dir.mkdir(exist_ok=True, parents=True)
            fname = out_dir / f"energia_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(fname, "w", encoding="utf-8") as f:
                f.write(self.log.toPlainText())
            self.log_msg(f"Log salvo em: {fname}", "success")
        except Exception as e:
            self.log_msg(f"Falha ao salvar log: {e}", "error")

    # ---------- Config persistente ----------
    def _cfg_path(self) -> Path:
        p = Path(__file__).parent / "json"
        p.mkdir(parents=True, exist_ok=True)
        return p / "config_energia.json"

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
            self._cfg_path().write_text(json.dumps(self.cfg, indent=4), encoding="utf-8")
            self.log_msg("Configurações salvas.", "success")
        except Exception as e:
            self.log_msg(f"Erro ao salvar config: {e}", "error")

    # ---------- Ações de topo ----------
    def _open_config(self):
        dlg = ConfigDialog(self.cfg, self)
        if dlg.exec() == QDialog.Accepted:
            self.cfg.update(dlg.get_config())
            self._save_config()

    def _open_key(self):
        dlg = ApiKeyDialog(self.cfg.get("api_key", ""), self)
        if dlg.exec() == QDialog.Accepted:
            key = dlg.get_key()
            if not key:
                QMessageBox.warning(self, "API Key", "Informe uma chave válida.")
                return
            self.cfg["api_key"] = key
            os.environ["OPENAI_API_KEY"] = key
            self._save_config()
            self.log_msg("API Key definida com sucesso.", "success")

    def _select_base_dir(self):
        folder = QFileDialog.getExistingDirectory(self, "Selecione a pasta base com PDFs (ENERGIA)")
        if folder:
            self.cfg["base_dir"] = folder
            self._save_config()
            self.log_msg(f"Pasta base definida: {folder}", "success")

    # ---------- Stats ----------
    def _update_stats(self, total: int, ok: int, err: int):
        self.stat_total, self.stat_ok, self.stat_err = total, ok, err
        self.lbl_stat_total.setText(f"Total: {total}")
        self.lbl_stat_ok.setText(f"Sucesso: {ok}")
        self.lbl_stat_err.setText(f"Erros: {err}")

    # ---------- Worker lifecycle ----------
    def _start_separador(self):
        base = self.cfg.get("base_dir", "")
        if not base or not Path(base).exists():
            QMessageBox.warning(self, "Pasta base", "Defina a Pasta Base de PDFs.")
            return
        if not os.environ.get("OPENAI_API_KEY"):
            QMessageBox.warning(self, "OpenAI", "Defina a API Key da OpenAI primeiro.")
            return
        
        self._last_action = "separar"
        self._reset_stats_ui()
        self.btn_cancel.setEnabled(True)
        self.lbl_last_status.setText("Separador em execução…")
        self.lbl_last_status_time.setText(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        GlobalProgress.begin("Separando por titular…", self)

        self.worker = SeparadorWorker(base_dir=base, use_ocr=self.chk_ocr.isChecked())
        self._connect_worker_signals()
        self.worker.start()

    def _start_gerar_txt(self):
        base = self.cfg.get("base_dir", "")
        if not base or not Path(base).exists():
            QMessageBox.warning(self, "Pasta base", "Defina a Pasta Base de PDFs.")
            return
        if not os.environ.get("OPENAI_API_KEY"):
            QMessageBox.warning(self, "OpenAI", "Defina a API Key da OpenAI primeiro.")
            return

        self._last_action = "gerar"

        self._reset_stats_ui()
        self.btn_cancel.setEnabled(True)
        self.lbl_last_status.setText("Geração de TXT em execução…")
        self.lbl_last_status_time.setText(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        GlobalProgress.begin("Gerando TXT…", self)

        # já existe o check com QMessageBox. Se quiser reforçar:
        os.environ["OPENAI_API_KEY"] = self.cfg.get("api_key", "") or os.environ.get("OPENAI_API_KEY", "")

        self.worker = GerarTXTWorker(base_dir=base, use_ocr=self.chk_ocr.isChecked())
        self._connect_worker_signals()
        self.worker.start()

    def _connect_worker_signals(self):
        self.worker.log_sig.connect(self.log_msg)
        self.worker.stats_sig.connect(self._update_stats)
        self.worker.finished_sig.connect(self._on_worker_finished)
        self.worker.step_sig.connect(lambda i, n: None)

    def _cancel_worker(self):
        if self.worker and self.worker.isRunning():
            self.log_msg("Solicitando cancelamento...", "warning")
            self.worker.cancel()
        else:
            self.log_msg("Nenhum processo em execução para cancelar.", "info")

    def _on_worker_finished(self, status: str):
        GlobalProgress.end()
        self.btn_cancel.setEnabled(False)
        self.lbl_last_status.setText(f"{status.upper()}")
        self.lbl_last_status_time.setText(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        self.log_msg(f"Processo finalizado: {status}", "success" if status=="Concluído" else "warning")
        # IMPORTAR APENAS SE A AÇÃO FOI "GERAR"
        if status == "Concluído" and self._last_action == "gerar":
            resp = QMessageBox.question(
                self, "Importar agora?",
                "TXT gerado para Cleuber, Gilson, Lucas e Adriana.\n\n"
                "Deseja importar automaticamente no sistema,\n"
                "lançando cada talão no seu perfil correspondente?",
                QMessageBox.Yes | QMessageBox.No
            )
            if resp == QMessageBox.Yes:
                self._importar_todos_os_txts_por_perfil()

    def _importar_todos_os_txts_por_perfil(self):
        """
        Importa os TXT gerados, trocando para cada perfil correspondente ANTES de importar.
        Antes de cada import, FILTRA DUPLICADOS (mesmo participante + mesmo nº do doc) com base no BD.
        No fim, exibe um bloco de 'DUPLICADOS (ENERGIA)' no log.
        """
        base = (self.cfg or {}).get("base_dir") or ""
        if not base:
            QMessageBox.warning(self, "Pasta base", "Defina a pasta base primeiro.")
            return

        PROFILE_NAME_MAP = {
            "Cleuber":  "Cleuber Marcos",
            "Gilson":   "Gilson Oliveira",
            "Lucas":    "Lucas Laignier",
            "Adriana":  "Adriana Lucia",
        }

        mw = self.window()
        if mw is None:
            QMessageBox.warning(self, "Janela principal", "Janela principal não encontrada.")
            return

        # Lembrar o perfil atual para voltar ao final
        perfil_original = None
        try:
            combo = getattr(mw, "profile_selector", None)
            perfil_original = combo.currentText() if combo else None
        except Exception:
            pass

        import_errors = []
        dups_log = []  # (perfil, num_doc, cpf_cnpj, historico, arquivo)

        for nome_curto, perfil in PROFILE_NAME_MAP.items():
            txt_path = os.path.join(base, f"{nome_curto}.txt")
            if not os.path.exists(txt_path):
                self.log_msg(f"[{nome_curto}] TXT não encontrado: {txt_path}", "warning")
                continue

            try:
                # Troca de perfil ANTES de checar duplicados (duplicidade é por perfil/BD)
                if hasattr(mw, "switch_profile"):
                    mw.switch_profile(perfil)

                # Ler linhas do TXT e filtrar duplicados
                try:
                    with open(txt_path, "r", encoding="utf-8") as f:
                        linhas = [ln.strip() for ln in f if ln.strip()]
                except Exception as e:
                    self.log_msg(f"[{perfil}] Falha ao ler TXT: {e}", "error")
                    import_errors.append((perfil, f"Leitura TXT: {e}"))
                    continue

                sem_dup = []
                for ln in linhas:
                    parts = ln.split("|")
                    # layout esperado (12 colunas): data|imóvel|conta|num_doc|tipo_doc|histórico|cpf_cnpj|tipo|...
                    if len(parts) < 12:
                        # linha inesperada: não bloqueia
                        sem_dup.append(ln)
                        continue

                    num_doc = (parts[3] or "").replace(" ", "")
                    cpf_cnpj = re.sub(r"\D", "", parts[6] or "")
                    historico = parts[5] if len(parts) > 5 else ""

                    if not num_doc or not cpf_cnpj:
                        sem_dup.append(ln)
                        continue

                    # Checagem de duplicidade via Supabase (sem SQL bruto)
                    exists = False
                    try:
                        # 1) participante por CPF/CNPJ (digits)
                        pid_rows = (_RestTable("participante")
                                      .select("id")
                                      .eq("cpf_cnpj", cpf_cnpj)
                                      .limit(1)
                                      .execute().data) or []
                        if pid_rows:
                            pid = int(pid_rows[0]["id"])

                            # 2) candidatos do mesmo participante; compara num_doc ignorando espaços
                            cand = (_RestTable("lancamento")
                                      .select("id,num_doc,id_participante")
                                      .eq("id_participante", pid)
                                      .order("id", desc=True)
                                      .limit(200)
                                      .execute().data) or []

                            nd_target = re.sub(r"\s+", "", num_doc or "")
                            exists = any(re.sub(r"\s+", "", str(c.get("num_doc") or "")) == nd_target for c in cand)
                    except Exception:
                        exists = False

                    if exists:
                        dups_log.append((perfil, num_doc, cpf_cnpj, historico, os.path.basename(txt_path)))
                    else:
                        sem_dup.append(ln)

                if not sem_dup:
                    self.log_msg(f"⚠️ Nenhum lançamento novo para {perfil} (todos já existem).", "warning")
                    continue

                # Grava em ARQUIVO TEMPORÁRIO e importa por ele (mantendo os TXT salvos no disco)
                with tempfile.NamedTemporaryFile("w", delete=False, suffix=".txt", encoding="utf-8") as tf:
                    tf.write("\n".join(sem_dup))
                    temp_path = tf.name

                try:
                    mw._import_lancamentos_txt(str(temp_path))
                    if hasattr(mw, "carregar_lancamentos"):
                        mw.carregar_lancamentos()
                    if hasattr(mw, "dashboard"):
                        try:
                            mw.dashboard.load_data()
                        except Exception:
                            pass
                    self.log_msg(f"✅ Importado em: {perfil} (TEMP)", "success")
                finally:
                    try:
                        os.unlink(temp_path)
                    except Exception:
                        pass
                    

            except Exception as e:
                import_errors.append((perfil, str(e)))
                self.log_msg(f"[{perfil}] Falha ao importar: {e}", "error")

        # Volta para o perfil original
        try:
            if perfil_original and hasattr(mw, "switch_profile"):
                mw.switch_profile(perfil_original)
        except Exception:
            pass

        # Alertas de importação
        if import_errors:
            msg = "\n".join(f"• {p}: {err}" for p, err in import_errors)
            QMessageBox.warning(self, "Importação concluída com alertas", msg)
        else:
            QMessageBox.information(self, "OK", "Importação automática concluída.")

        # Bloco de DUPLICADOS (ENERGIA)
        if dups_log:
            self.log.append("<div style='text-align:center;color:#ffd166;font-weight:700;font-family:monospace;'>🔁 DUPLICADOS (ENERGIA)</div>")
            self.log.append("<div style='font-family:monospace; color:#ffd166; text-align:center; margin:2px 0 6px 0;'>MESMO PARTICIPANTE + MESMO Nº DO DOC</div>")
            hdr = ("PERFIL".ljust(16) + " │ " +
                   "DOC".ljust(12) + " │ " +
                   "CPF/CNPJ".ljust(14) + " │ " +
                   "HISTÓRICO".ljust(28) + " │ " +
                   "ARQUIVO")
            self.log.append("<div style='font-family:monospace;'><b style='color:#ffd166;'>" + hdr + "</b></div>")
            self.log.append("<div style='font-family:monospace; color:#554a08;'>"
                            "────────────────┼────────────┼──────────────┼────────────────────────────┼────────────────</div>")
            for perfil, num_doc, cpf, hist, arq in dups_log:
                perf = f"{(perfil or '')[:16]:<16}"
                doc  = f"{(num_doc or '')[:12]:<12}"
                cpf2 = f"{(cpf or '')[:14]:<14}"
                hist2 = f"{(hist or '')[:28]:<28}"
                arq2 = (arq or "")[:16]
                line = f"{perf} │ {doc} │ {cpf2} │ {hist2} │ {arq2}"
                self.log.append(f"<span style='font-family:monospace; color:#ffd166;'>{line}</span>")
            self.log.append("<div style='text-align:center;color:#2e3d56;font-family:monospace;'>======================</div>")
        else:
            self.log_msg("✅ Nenhum duplicado detectado para ENERGIA.", "success")
    
    def _reset_stats_ui(self):
        self._update_stats(0,0,0)
        self.log_msg("--------------------------------", "divider")

    # ---------- Close ----------
    def closeEvent(self, event: QCloseEvent):
        # Se estiver processando, confirmar cancelamento
        if self.worker and self.worker.isRunning():
            if QMessageBox.question(self, "Sair", "Há um processo em execução. Deseja cancelar e sair?",
                                    QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
                self._cancel_worker()
            else:
                event.ignore()
                return
    
        # Fechar como ABA dentro do sistema (igual ao Importador XML)
        try:
            mw = self.window()
            tabs = getattr(mw, "tabs", None)
            if tabs:
                idx = tabs.indexOf(self)
                if idx != -1:
                    tabs.removeTab(idx)
                    self.deleteLater()
                    return  # evita duplo fechamento
        except Exception:
            pass
        
        # Fallback: janela standalone
        event.accept()

# ============================
# Main
# ============================
if __name__ == "__main__":
    app = QApplication(sys.argv)
    if ICON_PATH.exists():
        app.setWindowIcon(QIcon(str(ICON_PATH)))
    app.setStyleSheet(STYLE_SHEET)

    w = AutomacaoEnergiaUI()
    w.show()
    sys.exit(app.exec())
