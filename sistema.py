import os
import sys
import re
import json
import csv
import sqlite3
import pandas as pd
import requests
from datetime import datetime

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLineEdit, QDateEdit, QComboBox, QLabel,
    QTableWidget, QTableWidgetItem, QHeaderView, QTabWidget, QDialog,
    QDialogButtonBox, QMessageBox, QFormLayout, QGroupBox, QFrame,
    QStatusBar, QToolBar, QFileDialog, QCheckBox, QMenu, QToolButton,
    QWidgetAction, QInputDialog
)
from PySide6.QtCore import Qt, QDate, QSize, QSettings
from PySide6.QtGui import QFont, QIcon, QColor, QPainter, QAction
from PySide6.QtCharts import QChart, QChartView, QPieSeries

# —————— Validação de CPF ——————
def valida_cpf(cpf: str) -> bool:
    nums = re.sub(r'\D', '', cpf)
    if len(nums) != 11 or nums == nums[0] * 11:
        return False
    def calc_dig(base: str, pesos: list[int]) -> str:
        total = sum(int(d) * p for d, p in zip(base, pesos))
        resto = total % 11
        return '0' if resto < 2 else str(11 - resto)
    d1 = calc_dig(nums[:9], list(range(10, 1, -1)))
    d2 = calc_dig(nums[:9] + d1, list(range(11, 1, -1)))
    return nums.endswith(d1 + d2)

# —————— Cache e consulta Receita ——————
CACHE_FOLDER = 'banco_de_dados'
CACHE_FILE   = os.path.join(CACHE_FOLDER, 'Cleuber Marcos', 'json', 'receita_cache.json')
API_URL_CNPJ = 'https://www.receitaws.com.br/v1/cnpj/'
API_URL_CPF  = 'https://www.receitaws.com.br/v1/cpf/'

# —————— Configuração para salvar último caminho do TXT LCDPR ——————
TXT_PREF_FILE = os.path.join(CACHE_FOLDER, 'Cleuber Marcos', 'json', 'lcdpr_txt_path.json')

def load_last_txt_path() -> str:
    os.makedirs(CACHE_FOLDER, exist_ok=True)
    try:
        with open(TXT_PREF_FILE, 'r', encoding='utf-8') as f:
            return json.load(f).get('last_path', '')
    except:
        return ''

def save_last_txt_path(path: str):
    os.makedirs(CACHE_FOLDER, exist_ok=True)
    with open(TXT_PREF_FILE, 'w', encoding='utf-8') as f:
        json.dump({'last_path': path}, f, ensure_ascii=False, indent=2)

def ensure_cache_file():
    os.makedirs(CACHE_FOLDER, exist_ok=True)
    if not os.path.isfile(CACHE_FILE):
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump({}, f, ensure_ascii=False, indent=2)

def load_cache() -> dict:
    ensure_cache_file()
    with open(CACHE_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_cache(cache: dict):
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def consulta_receita(cpf_cnpj: str, tipo: str = 'cnpj') -> dict:
    """
    Faz consulta na API ou no cache. Chave = "<tipo>:<cpf_cnpj>"
    """
    cache = load_cache()
    key = f"{tipo}:{cpf_cnpj}"
    if key in cache:
        return cache[key]

    url = (API_URL_CPF if tipo == 'cpf' else API_URL_CNPJ) + cpf_cnpj
    res = requests.get(url, timeout=5)
    res.raise_for_status()
    data = res.json()

    cache[key] = data
    save_cache(cache)
    return data

APP_ICON    = 'agro_icon.png'

# 1) Pasta base do seu projeto (onde está esse script)
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
ICONS_DIR = os.path.join(PROJECT_DIR, 'banco_de_dados', 'icons')
LOCK_ICON = os.path.join(ICONS_DIR, 'lock.png')

# Perfil dinâmico
CURRENT_PROFILE = "Cleuber Marcos"

# Usuário que fez login
CURRENT_USER = None

def get_profile_db_filename():
    """
    Retorna o caminho completo para o banco de dados do perfil selecionado,
    criando a pasta se necessário.
    """
    base = os.path.join(PROJECT_DIR, 'banco_de_dados', CURRENT_PROFILE, 'data')
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, 'lcdpr.db')

# ── (1) Configuração da pasta de login ─────────────────────────────
LOGIN_DIR   = os.path.join(PROJECT_DIR, 'banco_de_dados', 'login')
ADMIN_FILE  = os.path.join(LOGIN_DIR, 'admin.json')
USERS_FILE  = os.path.join(LOGIN_DIR, 'users.json')

def ensure_login_files():
    os.makedirs(LOGIN_DIR, exist_ok=True)
    # cria admin.json com senha padrão se não existir
    if not os.path.isfile(ADMIN_FILE):
        with open(ADMIN_FILE, 'w', encoding='utf-8') as f:
            json.dump({"admin_password": "admin123"}, f, ensure_ascii=False, indent=2)
    # cria users.json vazio se não existir
    if not os.path.isfile(USERS_FILE):
        with open(USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump({}, f, ensure_ascii=False, indent=2)

def load_admin_password() -> str:
    with open(ADMIN_FILE, 'r', encoding='utf-8') as f:
        return json.load(f).get("admin_password", "")

def load_users() -> dict:
    with open(USERS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def valida_usuario(username: str, password: str) -> bool:
    """
    Retorna True se o usuário e senha forem válidos.
    - Usuário 'admin' é validado contra admin.json
    - Demais usuários são validados contra users.json
    """
    # garante que os arquivos de login existem
    ensure_login_files()

    # trata admin separadamente
    if username.lower() == "admin":
        return password == load_admin_password()

    # valida demais usuários
    users = load_users()
    return users.get(username) == password

def save_users(users: dict):
    with open(USERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(users, f, ensure_ascii=False, indent=2)


# ── (2) Diálogo de registro de novo usuário ────────────────────────
class RegisterUserDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Registrar Novo Usuário")
        self.setModal(True)
        layout = QFormLayout(self)
        self.user_edit = QLineEdit(); layout.addRow("Novo usuário:", self.user_edit)
        self.pw_edit = QLineEdit(); self.pw_edit.setEchoMode(QLineEdit.Password)
        layout.addRow("Senha:", self.pw_edit)
        self.pw2_edit = QLineEdit(); self.pw2_edit.setEchoMode(QLineEdit.Password)
        layout.addRow("Confirmar senha:", self.pw2_edit)
        btns = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel, Qt.Horizontal, self)
        btns.accepted.connect(self.on_save); btns.rejected.connect(self.reject)
        layout.addRow(btns)

    def on_save(self):
        u, p, p2 = self.user_edit.text().strip(), self.pw_edit.text(), self.pw2_edit.text()
        if not u or not p:
            QMessageBox.warning(self, "Erro", "Preencha usuário e senha."); return
        if p != p2:
            QMessageBox.warning(self, "Erro", "As senhas não conferem."); return
        users = load_users()
        if u in users:
            QMessageBox.warning(self, "Erro", "Usuário já existe."); return
        users[u] = p; save_users(users)
        QMessageBox.information(self, "Sucesso", f"Usuário '{u}' cadastrado."); self.accept()


# ── (3) Diálogo principal de login ──────────────────────────────────
class LoginDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Login")
        self.setWindowIcon(QIcon(LOCK_ICON))
        # aplica o mesmo CSS que o MainWindow
        self.setStyleSheet(STYLE_SHEET)

        self.setModal(True)
        self.resize(350, 150)

        layout = QVBoxLayout(self)
        form   = QFormLayout()
        self.username = QLineEdit(); self.username.setPlaceholderText("usuário")
        self.password = QLineEdit(); self.password.setEchoMode(QLineEdit.Password)
        self.password.setPlaceholderText("senha")
        form.addRow("Usuário:", self.username)
        form.addRow("Senha:  ", self.password)
        layout.addLayout(form)

        btns = QHBoxLayout()
        btn_login    = QPushButton("Logar");    btn_login.clicked.connect(self.try_login)
        btn_register = QPushButton("Registrar"); btn_register.clicked.connect(self.try_register)
        for b in (btn_login, btn_register):
            b.setFixedHeight(28)
            btns.addWidget(b)
        layout.addLayout(btns)

    def try_login(self):
        user = self.username.text().strip()
        pwd  = self.password.text().strip()
        if valida_usuario(user, pwd):
            global CURRENT_USER
            CURRENT_USER = user
            self.accept()
        else:
            QMessageBox.warning(self, "Falha", "Usuário ou senha inválidos.")

    def try_register(self):
        # pede a senha de admin
        senha, ok = QInputDialog.getText(
            self,
            "Senha de Administrador",
            "Digite a senha de administrador:",
            QLineEdit.Password
        )
        if not ok:
            return  # usuário cancelou

        if senha != load_admin_password():
            QMessageBox.warning(self, "Acesso negado", "Senha de administrador incorreta.")
            return

        # se a senha estiver correta, abre o diálogo de registro
        dlg = RegisterUserDialog(self)
        dlg.setStyleSheet(STYLE_SHEET)
        dlg.exec()
        
# ─── Passo 2: Ajuste da classe Database ───
class Database:
    def __init__(self, filename: str = None):
        # Usa o DB do perfil atual se nenhum filename for passado
        if filename is None:
            filename = get_profile_db_filename()
        try:
            self.conn = sqlite3.connect(filename)
        except sqlite3.OperationalError as e:
            raise RuntimeError(f"Não foi possível abrir/criar o banco em '{filename}':\n  {e}")
        self._create_tables()
        self._create_views()
        self._migrate_schema()   # <-- inserir esta chamada

    def _migrate_schema(self):
        """Adiciona a coluna usuario em lancamento se ela ainda não existir."""
        cursor = self.conn.cursor()
        try:
            # testa se já existe
            cursor.execute("SELECT usuario FROM lancamento LIMIT 1")
        except sqlite3.OperationalError:
            # se der erro, adiciona a coluna com valor padrão vazio
            cursor.execute(
                "ALTER TABLE lancamento ADD COLUMN usuario TEXT NOT NULL DEFAULT ''"
            )
            self.conn.commit()
            
    def _create_tables(self):
        self.conn.cursor().executescript("""
        CREATE TABLE IF NOT EXISTS imovel_rural (
            id INTEGER PRIMARY KEY AUTOINCREMENT, cod_imovel TEXT UNIQUE NOT NULL,
            pais TEXT NOT NULL DEFAULT 'BR', moeda TEXT NOT NULL DEFAULT 'BRL',
            cad_itr TEXT, caepf TEXT, insc_estadual TEXT, nome_imovel TEXT NOT NULL,
            endereco TEXT NOT NULL, num TEXT, compl TEXT, bairro TEXT NOT NULL,
            uf TEXT NOT NULL, cod_mun TEXT NOT NULL, cep TEXT NOT NULL,
            tipo_exploracao INTEGER NOT NULL, participacao REAL NOT NULL DEFAULT 100.0,
            area_total REAL, area_utilizada REAL, data_cadastro DATE DEFAULT CURRENT_DATE
        );
        CREATE TABLE IF NOT EXISTS conta_bancaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT, cod_conta TEXT UNIQUE NOT NULL,
            pais_cta TEXT NOT NULL DEFAULT 'BR', banco TEXT, nome_banco TEXT NOT NULL,
            agencia TEXT NOT NULL, num_conta TEXT NOT NULL, saldo_inicial REAL DEFAULT 0,
            data_abertura DATE DEFAULT CURRENT_DATE
        );
        CREATE TABLE IF NOT EXISTS participante (
            id INTEGER PRIMARY KEY AUTOINCREMENT, cpf_cnpj TEXT UNIQUE NOT NULL,
            nome TEXT NOT NULL, tipo_contraparte INTEGER NOT NULL,
            data_cadastro DATE DEFAULT CURRENT_DATE
        );
        CREATE TABLE IF NOT EXISTS cultura (
            id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT NOT NULL, tipo TEXT NOT NULL,
            ciclo TEXT, unidade_medida TEXT
        );
        CREATE TABLE IF NOT EXISTS area_producao (
            id INTEGER PRIMARY KEY AUTOINCREMENT, imovel_id INTEGER NOT NULL,
            cultura_id INTEGER NOT NULL, area REAL NOT NULL, data_plantio DATE,
            data_colheita_estimada DATE, produtividade_estimada REAL,
            FOREIGN KEY(imovel_id) REFERENCES imovel_rural(id),
            FOREIGN KEY(cultura_id) REFERENCES cultura(id)
        );
        CREATE TABLE IF NOT EXISTS estoque (
            id INTEGER PRIMARY KEY AUTOINCREMENT, produto TEXT NOT NULL,
            quantidade REAL NOT NULL, unidade_medida TEXT NOT NULL,
            valor_unitario REAL, local_armazenamento TEXT,
            data_entrada DATE DEFAULT CURRENT_DATE, data_validade DATE,
            imovel_id INTEGER, FOREIGN KEY(imovel_id) REFERENCES imovel_rural(id)
        );
        CREATE TABLE IF NOT EXISTS lancamento (
            id INTEGER PRIMARY KEY AUTOINCREMENT, data DATE NOT NULL,
            cod_imovel INTEGER NOT NULL, cod_conta INTEGER NOT NULL,
            num_doc TEXT, tipo_doc INTEGER NOT NULL, historico TEXT NOT NULL,
            id_participante INTEGER, tipo_lanc INTEGER NOT NULL,
            valor_entrada REAL DEFAULT 0, valor_saida REAL DEFAULT 0,
            saldo_final REAL NOT NULL, natureza_saldo TEXT NOT NULL,
            usuario TEXT NOT NULL, categoria TEXT, area_afetada INTEGER,
            quantidade REAL, unidade_medida TEXT,
            FOREIGN KEY(cod_imovel) REFERENCES imovel_rural(id),
            FOREIGN KEY(cod_conta) REFERENCES conta_bancaria(id),
            FOREIGN KEY(id_participante) REFERENCES participante(id),
            FOREIGN KEY(area_afetada) REFERENCES area_producao(id)
        );
        """)
        self.conn.commit()


    def _create_views(self):
        self.conn.cursor().executescript("""
        CREATE VIEW IF NOT EXISTS saldo_contas AS
        SELECT cb.id, cb.cod_conta, cb.nome_banco,
               l.saldo_final * (CASE l.natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) AS saldo_atual
        FROM conta_bancaria cb
        LEFT JOIN (SELECT cod_conta, MAX(id) AS max_id FROM lancamento GROUP BY cod_conta) last_l
            ON cb.id = last_l.cod_conta
        LEFT JOIN lancamento l ON last_l.max_id = l.id;
    
        CREATE VIEW IF NOT EXISTS resumo_categorias AS
        SELECT categoria, SUM(valor_entrada) AS total_entradas, SUM(valor_saida) AS total_saidas,
               strftime('%Y', data) AS ano, strftime('%m', data) AS mes
        FROM lancamento
        GROUP BY categoria, ano, mes;
        """)
        self.conn.commit()
    
    
    def execute_query(self, sql: str, params: list = None):
        cur = self.conn.cursor()
        cur.execute(sql, params or [])
        self.conn.commit()
        return cur

    def fetch_one(self, sql: str, params: list = None):
        return self.execute_query(sql, params).fetchone()

    def fetch_all(self, sql: str, params: list = None):
        return self.execute_query(sql, params).fetchall()

    def close(self):
        self.conn.close()

# --- ESTILO GLOBAL AGRO  ---
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


class NumericItem(QTableWidgetItem):
    def __init__(self, value, text=None): super().__init__(text or str(value)); self._value = value
    def __lt__(self, other): return self._value < other._value if isinstance(other, NumericItem) else super().__lt__(other)

class CurrencyLineEdit(QLineEdit):
    def __init__(self, parent=None): super().__init__(parent); self.setAlignment(Qt.AlignRight); self.setPlaceholderText("R$ 0,00"); self.textChanged.connect(self._format_currency)
    def _format_currency(self, text):
        digits = re.sub(r'[^\d]', '', text)
        if not digits: self.blockSignals(True); self.setText(''); self.blockSignals(False); return
        value = int(digits); inteiro = value // 100; cents = value % 100
        inteiro_str = f"{inteiro:,}".replace(",", "."); formatted = f"R$ {inteiro_str},{cents:02d}"
        self.blockSignals(True); self.setText(formatted); self.blockSignals(False)


# --- DIALOG BASE PARA CADASTROS ---
class CadastroBaseDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db = Database()
        # Layout principal
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(15, 15, 15, 15)
        # Cabeçalho e formulário serão adicionados nas subclasses
        self.form_layout = QFormLayout()
        self.layout.addLayout(self.form_layout)
        self._add_buttons()

    def _add_buttons(self):
        btn_layout = QHBoxLayout()
        btn_layout.setContentsMargins(0, 20, 0, 0)
        btn_layout.addStretch()
        self.btn_salvar = QPushButton("Salvar")
        self.btn_salvar.setObjectName("success")
        self.btn_salvar.clicked.connect(self.salvar)
        btn_layout.addWidget(self.btn_salvar)
        btn_cancelar = QPushButton("Cancelar")
        btn_cancelar.setObjectName("danger")
        btn_cancelar.clicked.connect(self.reject)
        btn_layout.addWidget(btn_cancelar)
        self.layout.addLayout(btn_layout)

    def salvar(self):
        raise NotImplementedError("Cada diálogo deve implementar seu próprio salvar()")
    
class CadastroImovelDialog(CadastroBaseDialog):
    def __init__(self, parent=None, imovel_id=None):
        super().__init__(parent); self.imovel_id = imovel_id
        self.configure_window(); self._build_ui(); self._load_data()

    def configure_window(self):
        self.setWindowTitle("Cadastro de Imóvel Rural"); self.setMinimumSize(900, 780)

    def _build_ui(self):
        header = QLabel("Cadastro de Imóvel Rural"); header.setFont(QFont('', 14, QFont.Bold))
        header.setStyleSheet("color: #ffffff; margin-bottom: 8px;")
        self.layout.insertWidget(0, header)

        grp1 = QGroupBox("Identificação do Imóvel"); f1 = QFormLayout(grp1)
        self.cod_imovel = QLineEdit(); f1.addRow("Código:", self.cod_imovel)
        self.pais = QComboBox(); self.pais.addItems(["BR", "AR", "US", "…"]); f1.addRow("País:", self.pais)
        self.moeda = QComboBox(); self.moeda.addItems(["BRL", "USD", "EUR", "…"]); f1.addRow("Moeda:", self.moeda)
        self.nome_imovel = QLineEdit(); f1.addRow("Nome:", self.nome_imovel)
        self.cad_itr = QLineEdit(); f1.addRow("CAD ITR:", self.cad_itr)
        self.caepf = QLineEdit(); f1.addRow("CAEPF:", self.caepf)
        self.insc_estadual = QLineEdit(); f1.addRow("Inscrição Est.:", self.insc_estadual)
        self.form_layout.addRow(grp1)

        grp2 = QGroupBox("Localização"); f2 = QFormLayout(grp2)
        self.endereco = QLineEdit(); f2.addRow("Endereço:", self.endereco)
        self.num = QLineEdit(); f2.addRow("Número:", self.num)
        self.compl = QLineEdit(); f2.addRow("Complemento:", self.compl)
        self.bairro = QLineEdit(); f2.addRow("Bairro:", self.bairro)
        self.uf = QLineEdit(); f2.addRow("UF:", self.uf)
        self.cod_mun = QLineEdit(); f2.addRow("Cód. Município:", self.cod_mun)
        self.cep = QLineEdit(); f2.addRow("CEP:", self.cep)
        self.form_layout.addRow(grp2)

        grp3 = QGroupBox("Exploração Agrícola"); f3 = QFormLayout(grp3)
        self.tipo_exploracao = QComboBox(); self.tipo_exploracao.addItems([
            "1 - Exploração individual", "2 - Condomínio", "3 - Imóvel arrendado",
            "4 - Parceria", "5 - Comodato", "6 - Outros"
        ]); f3.addRow("Tipo:", self.tipo_exploracao)
        self.participacao = QLineEdit("100.00"); f3.addRow("Participação (%):", self.participacao)
        self.form_layout.addRow(grp3)

        for w in [self.cod_imovel, self.pais, self.moeda, self.nome_imovel, self.cad_itr, self.caepf,
                  self.insc_estadual, self.endereco, self.num, self.compl, self.bairro, self.uf,
                  self.cod_mun, self.cep, self.tipo_exploracao, self.participacao]: w.setFixedHeight(25)

        grp4 = QGroupBox("Áreas do Imóvel (ha)"); f4 = QFormLayout(grp4)
        self.area_total = QLineEdit(); f4.addRow("Área Total:", self.area_total)
        self.area_utilizada = QLineEdit(); f4.addRow("Área Utilizada:", self.area_utilizada)
        self.form_layout.addRow(grp4)

        for w in [self.area_total, self.area_utilizada]: w.setFixedHeight(25)

    def _load_data(self):
        if not self.imovel_id: return
        row = self.db.fetch_one("""SELECT cod_imovel, pais, moeda, cad_itr, caepf, insc_estadual,
            nome_imovel, endereco, num, compl, bairro, uf, cod_mun, cep,
            tipo_exploracao, participacao, area_total, area_utilizada
            FROM imovel_rural WHERE id=?""", (self.imovel_id,))
        if not row: return
        (cod, pais, moeda, cad, caepf, ie, nome, end, num, comp, bar, uf, mun, cep, tipo, part, at, au) = row
        self.cod_imovel.setText(cod); self.pais.setCurrentText(pais); self.moeda.setCurrentText(moeda)
        self.cad_itr.setText(cad or ""); self.caepf.setText(caepf or ""); self.insc_estadual.setText(ie or "")
        self.nome_imovel.setText(nome); self.endereco.setText(end); self.num.setText(num or "")
        self.compl.setText(comp or ""); self.bairro.setText(bar); self.uf.setText(uf)
        self.cod_mun.setText(mun); self.cep.setText(cep); self.tipo_exploracao.setCurrentIndex(tipo-1)
        self.participacao.setText(f"{part:.2f}"); self.area_total.setText(f"{at or 0:.2f}")
        self.area_utilizada.setText(f"{au or 0:.2f}")

    def salvar(self):
        campos = [self.cod_imovel.text().strip(), self.pais.currentText(), self.moeda.currentText(),
                  self.nome_imovel.text().strip(), self.endereco.text().strip(), self.bairro.text().strip(),
                  self.uf.text().strip(), self.cod_mun.text().strip(), self.cep.text().strip()]
        if not all(campos): QMessageBox.warning(self, "Obrigatório", "Preencha todos os campos obrigatórios!"); return
        data = (
            self.cod_imovel.text().strip(), self.pais.currentText(), self.moeda.currentText(),
            self.cad_itr.text().strip() or None, self.caepf.text().strip() or None,
            self.insc_estadual.text().strip() or None, self.nome_imovel.text().strip(),
            self.endereco.text().strip(), self.num.text().strip() or None, self.compl.text().strip() or None,
            self.bairro.text().strip(), self.uf.text().strip(), self.cod_mun.text().strip(),
            self.cep.text().strip(), self.tipo_exploracao.currentIndex()+1,
            float(self.participacao.text()), float(self.area_total.text() or 0),
            float(self.area_utilizada.text() or 0)
        )
        try:
            if self.imovel_id:
                sql = """UPDATE imovel_rural SET cod_imovel=?,pais=?,moeda=?,cad_itr=?,caepf=?,insc_estadual=?,
                         nome_imovel=?,endereco=?,num=?,compl=?,bairro=?,uf=?,cod_mun=?,cep=?,
                         tipo_exploracao=?,participacao=?, area_total=?, area_utilizada=? WHERE id=?"""
                self.db.execute_query(sql, data + (self.imovel_id,))
                msg = "Atualizado com sucesso!"
            else:
                sql = """INSERT INTO imovel_rural (cod_imovel,pais,moeda,cad_itr,caepf,insc_estadual,
                         nome_imovel,endereco,num,compl,bairro,uf,cod_mun,cep,
                         tipo_exploracao,participacao, area_total, area_utilizada)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
                self.db.execute_query(sql, data)
                msg = "Cadastrado com sucesso!"
            QMessageBox.information(self, "Sucesso", msg); self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Erro", str(e))


# --- DIALOG CADASTRO CONTA BANCÁRIA ---
class CadastroContaDialog(CadastroBaseDialog):
    def __init__(self, parent=None, conta_id=None):
        super().__init__(parent)
        self.conta_id = conta_id
        self.configure_window()
        self._build_ui()
        self._load_data()

    def configure_window(self):
        self.setWindowTitle("Cadastro de Conta Bancária")
        self.setMinimumSize(800, 450)

    def _build_ui(self):
        header = QLabel("Cadastro de Conta Bancária")
        header.setFont(QFont('', 16, QFont.Bold))
        header.setStyleSheet("margin-bottom:15px;")
        self.layout.insertWidget(0, header)

        grp1 = QGroupBox("Identificação da Conta")
        f1 = QFormLayout(grp1)
        self.cod_conta = QLineEdit(); self.cod_conta.setPlaceholderText("Código único"); f1.addRow("Código da Conta:", self.cod_conta)
        self.pais_cta = QComboBox(); self.pais_cta.addItems(["BR","US","…"]); f1.addRow("País:", self.pais_cta)
        self.nome_banco = QLineEdit(); f1.addRow("Nome do Banco:", self.nome_banco)
        self.banco = QLineEdit(); f1.addRow("Código do Banco:", self.banco)
        self.form_layout.addRow(grp1)

        grp2 = QGroupBox("Dados Bancários")
        f2 = QFormLayout(grp2)
        self.agencia = QLineEdit(); f2.addRow("Agência:", self.agencia)
        self.num_conta = QLineEdit(); f2.addRow("Número da Conta:", self.num_conta)
        self.saldo_inicial = CurrencyLineEdit(); f2.addRow("Saldo Inicial:", self.saldo_inicial)
        self.form_layout.addRow(grp2)

    def _load_data(self):
        if not self.conta_id: return
        row = self.db.fetch_one("SELECT cod_conta, pais_cta, banco, nome_banco, agencia, num_conta, saldo_inicial FROM conta_bancaria WHERE id = ?", (self.conta_id,))
        if not row: return
        cod, pais, banco, nome, agencia, num_conta, saldo = row
        self.cod_conta.setText(cod); self.pais_cta.setCurrentText(pais); self.banco.setText(banco or "")
        self.nome_banco.setText(nome or ""); self.agencia.setText(agencia or ""); self.num_conta.setText(num_conta or "")
        self.saldo_inicial.setText(f"{saldo:.2f}")

    def salvar(self):
        cod_conta = self.cod_conta.text().strip(); nome_banco = self.nome_banco.text().strip()
        banco = self.banco.text().strip(); agencia = self.agencia.text().strip()
        num_conta = self.num_conta.text().strip(); saldo_raw = self.saldo_inicial.text().strip()
        if not (cod_conta and nome_banco and agencia and num_conta):
            QMessageBox.warning(self, "Campos Obrigatórios", "Preencha Código da Conta, Nome do Banco, Agência e Número da Conta."); return

        def parse_currency(text): digits = re.sub(r"[^\d]", "", text); return 0.0 if not digits else int(digits) // 100 + (int(digits) % 100) / 100.0
        saldo_inicial = parse_currency(saldo_raw)

        data = (cod_conta, "BR", banco, nome_banco, agencia, num_conta, saldo_inicial)

        try:
            if self.conta_id:
                sql = "UPDATE conta_bancaria SET cod_conta=?, pais_cta=?, banco=?, nome_banco=?, agencia=?, num_conta=?, saldo_inicial=? WHERE id=?"
                self.db.execute_query(sql, data + (self.conta_id,)); msg = "Conta bancária atualizada com sucesso!"
            else:
                sql = "INSERT INTO conta_bancaria (cod_conta, pais_cta, banco, nome_banco, agencia, num_conta, saldo_inicial) VALUES (?, ?, ?, ?, ?, ?, ?)"
                self.db.execute_query(sql, data); msg = "Conta bancária cadastrada com sucesso!"
            QMessageBox.information(self, "Sucesso", msg); self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Erro ao Salvar", f"Não foi possível salvar a conta bancária:\n{e}")


class CadastroParticipanteDialog(QDialog):
    def __init__(self, parent=None, participante_id=None):
        super().__init__(parent); self.participante_id = participante_id
        self.setWindowTitle("Cadastro de Participante"); self.setMinimumSize(400, 250)
        self.db = Database(); layout = QVBoxLayout(self)

        hdr = QLabel("Cadastro de Participante"); hdr.setFont(QFont('', 16, QFont.Bold)); hdr.setStyleSheet("margin-bottom:15px;"); layout.addWidget(hdr)

        form_layout = QFormLayout(); grp = QGroupBox("Dados do Participante"); grp.setLayout(form_layout); layout.addWidget(grp)

        self.tipo = QComboBox(); self.tipo.addItems(["Pessoa Jurídica", "Pessoa Física", "Órgão Público", "Outros"])
        self.tipo.currentIndexChanged.connect(self._ajustar_mask); form_layout.addRow("Tipo:", self.tipo)

        self.cpf_cnpj = QLineEdit(); self.cpf_cnpj.setPlaceholderText("Digite CPF ou CNPJ")
        self.cpf_cnpj.editingFinished.connect(self._on_cpf_cnpj); form_layout.addRow("CPF/CNPJ:", self.cpf_cnpj)
        self._ajustar_mask(self.tipo.currentIndex())

        self.nome = QLineEdit(); form_layout.addRow("Nome:", self.nome)

        btns = QHBoxLayout(); btns.addStretch()
        salvar = QPushButton("Salvar"); salvar.setObjectName("success"); salvar.clicked.connect(self.salvar); btns.addWidget(salvar)
        cancelar = QPushButton("Cancelar"); cancelar.setObjectName("danger"); cancelar.clicked.connect(self.reject); btns.addWidget(cancelar)
        layout.addLayout(btns)

        if participante_id:
            row = self.db.fetch_one("SELECT cpf_cnpj, nome, tipo_contraparte FROM participante WHERE id=?", (participante_id,))
            if row: self.tipo.setCurrentIndex(row[2] - 1); self.cpf_cnpj.setText(row[0]); self.nome.setText(row[1])

    def _ajustar_mask(self, idx):
        cur = self.cpf_cnpj.cursorPosition()
        self.cpf_cnpj.setInputMask("00.000.000/0000-00;_" if idx == 0 else "000.000.000-00;_" if idx == 1 else "")
        self.cpf_cnpj.setCursorPosition(cur)

    def _on_cpf_cnpj(self):
        raw = self.cpf_cnpj.text().strip(); digits = re.sub(r'\D', '', raw); idx = self.tipo.currentIndex()
        if idx == 0 and not valida_cpf(raw): QMessageBox.warning(self, "CPF inválido", "O CPF digitado não é válido."); self.nome.clear(); return
        if idx == 1 and len(digits) != 14: return
        try:
            kind = 'cpf' if idx == 0 else 'cnpj'; info = consulta_receita(digits, tipo=kind)
            if info.get('status') == 'OK':
                nome_api = info.get('nome') or info.get('fantasia')
                if nome_api: self.nome.setText(nome_api)
        except: return

    def salvar(self):
        raw = self.cpf_cnpj.text().strip(); digits = re.sub(r'\D', '', raw); idx = self.tipo.currentIndex()
        if idx == 0 and not valida_cpf(raw): QMessageBox.warning(self, "Inválido", "CPF inválido."); return
        if idx == 1:
            if len(digits) != 14: QMessageBox.warning(self, "Inválido", "CNPJ deve ter 14 dígitos."); return
            try: info = consulta_receita(digits, tipo='cnpj')
            except requests.HTTPError: QMessageBox.warning(self, "Inválido", "Não foi possível consultar o CNPJ na Receita Federal."); return
            if info.get('status') != 'OK': QMessageBox.warning(self, "Não Encontrado", "CNPJ não localizado na Receita Federal."); return

        nome = self.nome.text().strip()
        if not nome: QMessageBox.warning(self, "Inválido", "Nome não pode ficar vazio."); return

        exists = self.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj = ?", (digits,))
        if exists and not self.participante_id:
            QMessageBox.information(self, "Já existe", f"Participante já cadastrado (ID {exists[0]})."); return

        data = (digits, nome, idx + 1)
        try:
            if self.participante_id:
                self.db.execute_query("UPDATE participante SET cpf_cnpj = ?, nome = ?, tipo_contraparte = ? WHERE id = ?", data + (self.participante_id,))
            else:
                self.db.execute_query("INSERT INTO participante (cpf_cnpj, nome, tipo_contraparte) VALUES (?, ?, ?)", data)
            QMessageBox.information(self, "Sucesso", "Participante salvo com sucesso!"); self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Erro ao Salvar", f"Não foi possível salvar participante:\n{e}")

class ParametrosDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Parâmetros do Contribuinte")
        self.setMinimumSize(400, 500)
        self.settings = QSettings("Automatize Tech", "AgroApp")
        layout = QFormLayout(self)

        # Versão do leiaute
        self.version = QLineEdit(self.settings.value("param/version", "0013"))
        layout.addRow("Versão do Leiaute:", self.version)

        # Indicador de Movimentação
        self.ind_mov = QComboBox()
        self.ind_mov.addItems(["0 - Sem Movimento", "1 - Com Movimento"])
        iv = self.settings.value("param/ind_mov", "0")
        self.ind_mov.setCurrentText(f"{iv} - " + ("Sem Movimento" if iv=="0" else "Com Movimento"))
        layout.addRow("Ind. de Movimentação:", self.ind_mov)

        # Indicador de Recepção
        self.ind_rec = QComboBox()
        self.ind_rec.addItems(["0 - Original", "1 - Retificadora"])
        ir = self.settings.value("param/ind_rec", "0")
        self.ind_rec.setCurrentText(f"{ir} - " + ("Original" if ir=="0" else "Retificadora"))
        layout.addRow("Ind. de Recepção:", self.ind_rec)

        # ——— CNPJ/CPF (agora só CPF) ———
        self.ident = QLineEdit(self.settings.value("param/ident", ""))
        self.ident.setInputMask("000.000.000-00;_")
        layout.addRow("CPF:", self.ident)

        # Nome / Razão Social
        self.nome = QLineEdit(self.settings.value("param/nome", ""))
        layout.addRow("Nome / Razão Social:", self.nome)

        # Endereço
        self.logradouro  = QLineEdit(self.settings.value("param/logradouro", ""))
        self.numero      = QLineEdit(self.settings.value("param/numero", ""))
        self.complemento = QLineEdit(self.settings.value("param/complemento", ""))
        self.bairro      = QLineEdit(self.settings.value("param/bairro", ""))
        layout.addRow("Logradouro:", self.logradouro)
        layout.addRow("Número:", self.numero)
        layout.addRow("Complemento:", self.complemento)
        layout.addRow("Bairro:", self.bairro)

        # Localização
        self.uf     = QLineEdit(self.settings.value("param/uf", ""))
        self.cod_mun= QLineEdit(self.settings.value("param/cod_mun", ""))
        self.cep    = QLineEdit(self.settings.value("param/cep", ""))
        layout.addRow("UF:", self.uf)
        layout.addRow("Cód. Município:", self.cod_mun)
        layout.addRow("CEP:", self.cep)

        # Contato
        self.telefone = QLineEdit(self.settings.value("param/telefone", ""))
        self.email    = QLineEdit(self.settings.value("param/email", ""))
        layout.addRow("Telefone:", self.telefone)
        layout.addRow("Email:", self.email)

        # Botões
        btns = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.salvar)
        btns.rejected.connect(self.reject)
        layout.addRow(btns)

    def _ajustar_mask(self):
        if self.tipo.currentText() == "Pessoa Jurídica":
            self.ident.setInputMask("00.000.000/0000-00;_")
        else:
            self.ident.setInputMask("000.000.000-00;_")

    def salvar(self):
        s = self.settings
        s.setValue("param/version",    self.version.text())
        s.setValue("param/ind_mov",    self.ind_mov.currentText().split(" - ")[0])
        s.setValue("param/ind_rec",    self.ind_rec.currentText().split(" - ")[0])
        s.setValue("param/tipo",       self.tipo.currentText())
        s.setValue("param/ident",      self.ident.text())
        s.setValue("param/nome",       self.nome.text())
        s.setValue("param/logradouro", self.logradouro.text())
        s.setValue("param/numero",     self.numero.text())
        s.setValue("param/complemento",self.complemento.text())
        s.setValue("param/bairro",     self.bairro.text())
        s.setValue("param/uf",         self.uf.text())
        s.setValue("param/cod_mun",    self.cod_mun.text())
        s.setValue("param/cep",        self.cep.text())
        s.setValue("param/telefone",   self.telefone.text())
        s.setValue("param/email",      self.email.text())
        s.sync()
        QMessageBox.information(self, "Sucesso", "Parâmetros salvos com sucesso!")
        self.accept()

# --- DIALOG DE RELATÓRIO POR PERÍODO ---
class RelatorioPeriodoDialog(QDialog):
    def __init__(self, tipo, parent=None):
        super().__init__(parent)
        self.setWindowTitle(tipo)
        self.setMinimumSize(300, 150)
        layout = QFormLayout(self)
        self.dt_fim = QDateEdit(QDate.currentDate())
        self.dt_fim.setCalendarPopup(True)
        self.dt_fim.setDisplayFormat("dd/MM/yyyy")
        layout.addRow("Data final:", self.dt_fim)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addRow(btns)

    @property
    def periodo(self):
        return (
            self.dt_ini.date().toString("dd/MM/yyyy"),
            self.dt_fim.date().toString("dd/MM/yyyy")
        )

# --- WIDGET DASHBOARD (Painel) COM FILTRO INICIAL/FINAL E %
class DashboardWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db = Database()
        self.settings = QSettings("Automatize Tech", "AgroApp")
        self.layout = QVBoxLayout(self)
        self._build_filter_ui()
        self._build_cards_ui()
        self._build_piechart_ui()
        self.load_data()

    def _build_filter_ui(self):
        hl = QHBoxLayout()
        hl.addWidget(QLabel("De:"))
        ini = self.settings.value("dashFilterIni", QDate.currentDate().addMonths(-1), type=QDate)
        self.dt_dash_ini = QDateEdit(ini)
        self.dt_dash_ini.setCalendarPopup(True)
        self.dt_dash_ini.setDisplayFormat("dd/MM/yyyy")
        hl.addWidget(self.dt_dash_ini)
        hl.addWidget(QLabel("Até:"))
        fim = self.settings.value("dashFilterFim", QDate.currentDate(), type=QDate)
        self.dt_dash_fim = QDateEdit(fim)
        self.dt_dash_fim.setCalendarPopup(True)
        self.dt_dash_fim.setDisplayFormat("dd/MM/yyyy")
        hl.addWidget(self.dt_dash_fim)
        btn = QPushButton("Aplicar filtro"); btn.clicked.connect(self.on_dash_filter_changed)
        hl.addWidget(btn)
        hl.addStretch()
        self.layout.addLayout(hl)

    def _build_cards_ui(self):
        self.cards_layout = QHBoxLayout()
        self.cards_layout.setSpacing(20)
        self.saldo_card = self._card("Saldo Total", "R$ 0,00", "#2ecc71")
        self.receita_card = self._card("Receitas", "R$ 0,00", "#3498db")
        self.despesa_card = self._card("Despesas", "R$ 0,00", "#e74c3c")
        for c in [self.saldo_card, self.receita_card, self.despesa_card]:
            self.cards_layout.addWidget(c, 1)
        self.layout.addLayout(self.cards_layout)

    def _build_piechart_ui(self):
        self.pie_group = QGroupBox("Receitas x Despesas")
        gl = QVBoxLayout(self.pie_group)
        self.series = QPieSeries()
        chart = QChart(); chart.addSeries(self.series)
        chart.setAnimationOptions(QChart.SeriesAnimations)
        self.chart_view = QChartView(chart)
        self.chart_view.setRenderHint(QPainter.Antialiasing)
        gl.addWidget(self.chart_view)
        self.layout.addWidget(self.pie_group)

    def _card(self, title, value, color):
        frm = QFrame()
        frm.setStyleSheet(f"""
            QFrame {{ background-color:white; border-radius:8px; border-left:5px solid {color}; padding:15px; }}
            QLabel#title {{ color:#7f8c8d; font-size:14px; }}
            QLabel#value {{ color:#2c3e50; font-size:24px; font-weight:bold; }}
        """)
        fl = QVBoxLayout(frm)
        t = QLabel(title); t.setObjectName("title")
        v = QLabel(value); v.setObjectName("value")
        fl.addWidget(t); fl.addWidget(v)
        return frm

    def on_dash_filter_changed(self):
        self.settings.setValue("dashFilterIni", self.dt_dash_ini.date())
        self.settings.setValue("dashFilterFim", self.dt_dash_fim.date())
        self.load_data()

    def load_data(self):
        d1 = self.dt_dash_ini.date().toString("dd/MM/yyyy")
        d2 = self.dt_dash_fim.date().toString("dd/MM/yyyy")
        # Saldo total
        saldo = self.db.fetch_one("SELECT SUM(saldo_atual) FROM saldo_contas")[0] or 0
        s = f"{saldo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        self.saldo_card.findChild(QLabel, "value").setText(f"R$ {s}")
        # Receitas e Despesas no intervalo
        rec = self.db.fetch_one(
            "SELECT SUM(valor_entrada) FROM lancamento WHERE data BETWEEN ? AND ?", (d1, d2)
        )[0] or 0
        desp = self.db.fetch_one(
            "SELECT SUM(valor_saida)   FROM lancamento WHERE data BETWEEN ? AND ?", (d1, d2)
        )[0] or 0
        r = f"{rec:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        self.receita_card.findChild(QLabel, "value").setText(f"R$ {r}")
        d = f"{desp:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        self.despesa_card.findChild(QLabel, "value").setText(f"R$ {d}")
        # Gráfico de pizza com %
        self.series.clear()
        s1 = self.series.append("Receitas", rec)
        s2 = self.series.append("Despesas", desp)
        for slice in self.series.slices():
            pct = slice.percentage() * 100
            slice.setLabelVisible(True)
            slice.setLabel(f"{slice.label()} ({pct:.1f}%)")

# --- DIALOG PARA LANÇAMENTOS CONTÁBEIS ---
class LancamentoDialog(QDialog):
    def __init__(self, parent=None, lanc_id=None):
        super().__init__(parent)
        self.lanc_id = lanc_id
        self.configure_window()
        self.db = Database()
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(15, 15, 15, 15)
        self._build_ui()
        self._load_data()

    def configure_window(self):
        self.setWindowTitle("Lançamento Contábil")
        self.setMinimumSize(700, 400)

    def _build_ui(self):
        form = QFormLayout()
        # Data
        self.data = QDateEdit(QDate.currentDate())
        self.data.setCalendarPopup(True)
        form.addRow("Data:", self.data)
        self.data = QDateEdit(QDate.currentDate())
        self.data.setCalendarPopup(True)
        self.data.setDisplayFormat("dd/MM/yyyy")
        # Imóvel
        self.imovel = QComboBox()
        self.imovel.addItem("Selecione...", None)
        for id_, nome in self.db.fetch_all("SELECT id, nome_imovel FROM imovel_rural"):
            self.imovel.addItem(nome, id_)
        form.addRow("Imóvel Rural:", self.imovel)
        # Conta
        self.conta = QComboBox()
        self.conta.addItem("Selecione...", None)
        for id_, nome in self.db.fetch_all("SELECT id, nome_banco FROM conta_bancaria"):
            self.conta.addItem(nome, id_)
        form.addRow("Conta Bancária:", self.conta)
        # Participante
        self.participante = QComboBox()
        self.participante.addItem("Selecione...", None)
        for id_, nome in self.db.fetch_all("SELECT id, nome FROM participante"):
            self.participante.addItem(nome, id_)
        form.addRow("Participante:", self.participante)
        # Documento
        hl = QHBoxLayout()
        hl.addWidget(QLabel("Número:"))
        self.num_doc = QLineEdit()
        hl.addWidget(self.num_doc)
        hl.addWidget(QLabel("Tipo:"))
        self.tipo_doc = QComboBox()
        self.tipo_doc.addItems(["Nota Fiscal", "Recibo", "Boleto", "Fatura", "Folha", "Outros"])
        hl.addWidget(self.tipo_doc)
        form.addRow("Documento:", hl)
        # Histórico
        self.historico = QLineEdit()
        form.addRow("Histórico:", self.historico)
        # Tipo de lançamento
        self.tipo_lanc = QComboBox()
        self.tipo_lanc.addItems(["1 - Receita", "2 - Despesa", "3 - Adiantamento"])
        form.addRow("Tipo de Lançamento:", self.tipo_lanc)
        # Valores
        hl2 = QHBoxLayout()
        hl2.addWidget(QLabel("Entrada:"))
        self.valor_entrada = CurrencyLineEdit()
        hl2.addWidget(self.valor_entrada)
        hl2.addWidget(QLabel("Saída:"))
        self.valor_saida   = CurrencyLineEdit()
        hl2.addWidget(self.valor_saida)
        form.addRow("Valor:", hl2)

        self.layout.addLayout(form)
        btns = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.salvar)
        btns.rejected.connect(self.reject)
        self.layout.addWidget(btns)

    def _load_data(self):
        if not self.lanc_id:
            return
    
        row = self.db.fetch_one(
            "SELECT data, cod_imovel, cod_conta, num_doc, tipo_doc, historico, "
            "id_participante, tipo_lanc, valor_entrada, valor_saida, natureza_saldo "
            "FROM lancamento WHERE id = ?",
            (self.lanc_id,)
        )
        if not row:
            return
    
        (
            data, imovel_id, conta_id, num_doc, tipo_doc,
            historico, part_id, tipo_lanc, ent, sai, nat
        ) = row
    
        # data
        self.data.setDate(QDate.fromString(data, "dd/MM/yyyy"))
    
        # imóvel
        idx_im = self.imovel.findData(imovel_id)
        if idx_im >= 0:
            self.imovel.setCurrentIndex(idx_im)
    
        # conta
        idx_ct = self.conta.findData(conta_id)
        if idx_ct >= 0:
            self.conta.setCurrentIndex(idx_ct)
    
        # documento
        self.num_doc.setText(num_doc or "")
        self.tipo_doc.setCurrentIndex(tipo_doc - 1)
    
        # histórico e participante
        self.historico.setText(historico)
        idx_pr = self.participante.findData(part_id)
        if idx_pr >= 0:
            self.participante.setCurrentIndex(idx_pr)
    
        # tipo de lançamento
        self.tipo_lanc.setCurrentIndex(tipo_lanc - 1)
    
        # valores
        self.valor_entrada.setText(f"{ent:.2f}")
        self.valor_saida.setText(f"{sai:.2f}")
    
    def salvar(self):
        try:
            # Campos obrigatórios
            if not (self.imovel.currentData() and self.conta.currentData() and self.historico.text().strip()):
                QMessageBox.warning(self, "Campos Obrigatórios", "Preencha todos os campos obrigatórios!")
                return

            num = self.num_doc.text().strip()
            part = self.participante.currentData()

            # Verifica duplicata: mesmo número de documento + mesmo participante
            if num:
                sql = """
                    SELECT id FROM lancamento
                    WHERE num_doc = ? AND id_participante = ?
                """
                params = [num, part]
                if self.lanc_id:
                    sql += " AND id != ?"
                    params.append(self.lanc_id)
                existente = self.db.fetch_one(sql, params)
                if existente:
                    QMessageBox.warning(
                        self, "Lançamento Duplicado",
                        f"Já existe um lançamento (ID {existente[0]})\n"
                        f"com nota nº {num} para este participante."
                    )
                    return

            # Conversão de valores
            def parse_currency(text: str) -> float:
                digits = re.sub(r'[^\d]', '', text)
                if not digits:
                    return 0.0
                inteiro = int(digits) // 100
                centavos = int(digits) % 100
                return inteiro + centavos / 100.0

            ent = parse_currency(self.valor_entrada.text())
            sai = parse_currency(self.valor_saida.text())

            # Calcula saldo anterior e saldo final
            row = self.db.fetch_one(
                "SELECT (saldo_final * CASE natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) "
                "FROM lancamento WHERE cod_conta = ? ORDER BY id DESC LIMIT 1",
                (self.conta.currentData(),)
            )
            saldo_ant = row[0] if row and row[0] is not None else 0.0
            saldo_f = saldo_ant + ent - sai
            nat = 'P' if saldo_f >= 0 else 'N'

            # antes de tudo, capture data e hora atuais
            now = datetime.now().strftime("%d/%m/%Y %H:%M")
            usuario_ts = f"{CURRENT_USER} dia {now}"

            # Parâmetros para INSERT/UPDATE (sem categoria)
            params = [
                self.data.date().toString("dd/MM/yyyy"),
                self.imovel.currentData(),
                self.conta.currentData(),
                num or None,
                self.tipo_doc.currentIndex() + 1,
                self.historico.text().strip(),
                part,
                self.tipo_lanc.currentIndex() + 1,
                ent,
                sai,
                abs(saldo_f),
                nat,
                usuario_ts
            ]
            
            if self.lanc_id:
                sql = """
                UPDATE lancamento SET
                    data = ?, cod_imovel = ?, cod_conta = ?, num_doc = ?, tipo_doc = ?,
                    historico = ?, id_participante = ?, tipo_lanc = ?,
                    valor_entrada = ?, valor_saida = ?, saldo_final = ?,
                    natureza_saldo = ?, usuario = ?
                WHERE id = ?
                """
                self.db.execute_query(sql, params + [self.lanc_id])
            else:
                sql = """
                INSERT INTO lancamento (
                    data, cod_imovel, cod_conta, num_doc, tipo_doc,
                    historico, id_participante, tipo_lanc,
                    valor_entrada, valor_saida, saldo_final,
                    natureza_saldo, usuario
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """
                self.db.execute_query(sql, params)

            QMessageBox.information(self, "Sucesso", "Lançamento salvo com sucesso!")
            self.accept()

        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao salvar lançamento: {e}")

    def carregar_imoveis(self):
        termo = f"%{self.pesquisa.text()}%"
        rows = self.db.fetch_all("""
            SELECT id,cod_imovel,nome_imovel,uf,area_total,area_utilizada,participacao
            FROM imovel_rural
            WHERE cod_imovel LIKE ? OR nome_imovel LIKE ?
            ORDER BY nome_imovel
        """, (termo, termo))
        self.tabela.setRowCount(len(rows))
        for r,(id_,cod,nome,uf,at,au,part) in enumerate(rows):
            for c,val in enumerate([
                cod, nome, uf,
                f"{at or 0:.2f} ha",
                f"{au or 0:.2f} ha",
                f"{part:.2f}%"
            ]):
                item = QTableWidgetItem(val)
                self.tabela.setItem(r, c, item)
            self.tabela.item(r, 0).setData(Qt.UserRole, id_)

    def _select_row(self, row, _):
        self.selected_row = row
        self.btn_editar.setEnabled(True)
        self.btn_excluir.setEnabled(True)

    def nova_conta(self):
        dlg = CadastroImovelDialog(self)
        if dlg.exec():
            self.carregar_imoveis()

    def editar_imovel(self):
        id_ = self.tabela.item(self.selected_row, 0).data(Qt.UserRole)
        dlg = CadastroImovelDialog(self, id_)
        if dlg.exec():
            self.carregar_imoveis()

    def excluir_imovel(self):
        id_ = self.tabela.item(self.selected_row, 0).data(Qt.UserRole)
        nome = self.tabela.item(self.selected_row, 1).text()
        ans = QMessageBox.question(self, "Confirmar Exclusão",
                                   f"Excluir imóvel '{nome}'?",
                                   QMessageBox.Yes | QMessageBox.No)
        if ans == QMessageBox.Yes:
            try:
                self.db.execute_query("DELETE FROM imovel_rural WHERE id=?", (id_,))
                QMessageBox.information(self, "Sucesso", "Imóvel excluído!")
                self.carregar_imoveis()
            except Exception as e:
                QMessageBox.critical(self, "Erro", f"Erro ao excluir: {e}")

# --- WIDGET GERENCIAMENTO CONTAS ---
class GerenciamentoContasWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db = Database()
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(10,10,10,10)

        # cabeçalhos e estado de ordenação
        self._contas_labels = ["Código","Banco","Agência","Conta","Saldo Inicial"]
        self._contas_sort_state = {}

        self._build_ui()
        self._load_column_filter()
        self.carregar_contas()

    def _build_ui(self):
        # ===== Top bar: botões + pesquisa + filtro =====
        tl = QHBoxLayout()
        tl.setContentsMargins(0, 0, 10, 10)

        # Botões CRUD
        self.btn_novo = QPushButton("Nova Conta")
        self.btn_novo.clicked.connect(self.nova_conta)
        tl.addWidget(self.btn_novo)

        self.btn_editar = QPushButton("Editar")
        self.btn_editar.setEnabled(False)
        self.btn_editar.clicked.connect(self.editar_conta)
        tl.addWidget(self.btn_editar)

        self.btn_excluir = QPushButton("Excluir")
        self.btn_excluir.setEnabled(False)
        self.btn_excluir.clicked.connect(self.excluir_conta)
        tl.addWidget(self.btn_excluir)

        self.btn_importar = QPushButton("Importar")
        self.btn_importar.clicked.connect(self.importar_contas)
        tl.addWidget(self.btn_importar)

        # Barra de pesquisa
        self.search_contas = QLineEdit()
        self.search_contas.setPlaceholderText("Pesquisar contas…")
        self.search_contas.textChanged.connect(self._filter_contas)
        tl.addWidget(self.search_contas)

        # Botão de filtro de colunas (⚙️) no cantinho
        btn_filter = QToolButton()
        btn_filter.setText("⚙️")
        btn_filter.setAutoRaise(True)
        btn_filter.setPopupMode(QToolButton.InstantPopup)
        self._filter_menu = QMenu(self)
        for col, lbl in enumerate(self._contas_labels):
            wa = QWidgetAction(self._filter_menu)
            chk = QCheckBox(lbl)
            chk.setChecked(True)
            chk.toggled.connect(lambda vis, c=col: self._toggle_column(c, vis))
            wa.setDefaultWidget(chk)
            self._filter_menu.addAction(wa)
        btn_filter.setMenu(self._filter_menu)
        tl.addWidget(btn_filter)

        tl.addStretch()
        self.layout.addLayout(tl)

        # ===== Tabela de Contas =====
        self.tabela = QTableWidget(0, len(self._contas_labels))
        self.tabela.setHorizontalHeaderLabels(self._contas_labels)
        # evita edição direta
        self.tabela.setEditTriggers(QTableWidget.NoEditTriggers)
        # seleção de linha inteira
        self.tabela.setSelectionBehavior(QTableWidget.SelectRows)
        self.tabela.setSelectionMode(QTableWidget.SingleSelection)
        self.tabela.setAlternatingRowColors(True)
        self.tabela.setShowGrid(False)
        self.tabela.verticalHeader().setVisible(False)

        hdr = self.tabela.horizontalHeader()
        hdr.setHighlightSections(False)
        hdr.setDefaultAlignment(Qt.AlignCenter)
        hdr.setSectionResizeMode(QHeaderView.Stretch)
        # ordenação cíclica
        hdr.sectionDoubleClicked.connect(self._toggle_sort)
        # clique ativa botões
        self.tabela.cellClicked.connect(self._select_row)

        self.layout.addWidget(self.tabela)

    def _filter_contas(self, text: str):
        text = text.lower()
        for row in range(self.tabela.rowCount()):
            hide = True
            for col in range(self.tabela.columnCount()):
                item = self.tabela.item(row, col)
                if item and text in item.text().lower():
                    hide = False
                    break
            self.tabela.setRowHidden(row, hide)

    def importar_contas(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Importar Contas", "", "TXT (*.txt);;Excel (*.xlsx *.xls)"
        )
        if not path:
            return
        try:
            if path.lower().endswith('.txt'):
                self._import_contas_txt(path)
            else:
                self._import_contas_excel(path)
            self.carregar_contas()
        except Exception:
            QMessageBox.warning(
                self, "Importação Falhou",
                "Arquivo não segue o layout esperado e não foi importado."
            )

    def _import_contas_txt(self, path):
        with open(path, encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) != 7:
                    raise ValueError("Layout de TXT inválido")
                cod, pais_cta, banco, nome_banco, agencia, num_conta, saldo = parts
                self.db.execute_query(
                    """
                    INSERT OR REPLACE INTO conta_bancaria (
                        cod_conta, pais_cta, banco, nome_banco,
                        agencia, num_conta, saldo_inicial
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (cod, pais_cta, banco, nome_banco, agencia, num_conta, float(saldo))
                )

    def _import_contas_excel(self, path):
        df = pd.read_excel(path, dtype=str)
        required = ['cod_conta','pais_cta','banco','nome_banco','agencia','num_conta','saldo_inicial']
        if not all(col in df.columns for col in required):
            raise ValueError("Layout de Excel inválido")
        df.fillna('', inplace=True)
        for row in df.itertuples(index=False):
            self.db.execute_query(
                """
                INSERT OR REPLACE INTO conta_bancaria (
                    cod_conta, pais_cta, banco, nome_banco,
                    agencia, num_conta, saldo_inicial
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.cod_conta, row.pais_cta, row.banco, row.nome_banco,
                    row.agencia, row.num_conta, float(row.saldo_inicial)
                )
            )

    def _toggle_sort(self, index: int):
        """Cicla entre sem ordenação, asc e desc."""
        state = self._contas_sort_state.get(index, 0)
        if state == 0:
            self.tabela.sortItems(index, Qt.AscendingOrder)
            new = 1
        elif state == 1:
            self.tabela.sortItems(index, Qt.DescendingOrder)
            new = 2
        else:
            # volta à ordem original
            self.carregar_contas()
            new = 0
        self._contas_sort_state = {index: new}

    def _toggle_column(self, col: int, visible: bool):
        """Exibe/oculta coluna e salva no JSON."""
        self.tabela.setColumnHidden(col, not visible)
        self._save_column_filter()

    def _save_column_filter(self):
        path = os.path.join(CACHE_FOLDER, 'Cleuber Marcos', 'json', "lanc_filter.json")
        try:
            cfg = json.load(open(path, "r", encoding="utf-8"))
        except:
            cfg = {}
        # salve um dicionário com duas chaves: "lanc" e "contas"
        cfg["contas"] = [
            not self.tabela.isColumnHidden(c)
            for c in range(self.tabela.columnCount())
        ]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)

    def _load_column_filter(self):
        """Aplica o JSON salvo (mesmo arquivo de lanc) à tabela de contas."""
        path = os.path.join(CACHE_FOLDER, 'Cleuber Marcos', 'json', "lanc_filter.json")
        try:
            cfg = json.load(open(path, "r", encoding="utf-8"))
            vis = cfg.get("contas", [])
        except:
            return
        for c, shown in enumerate(vis):
            self.tabela.setColumnHidden(c, not shown)
        # sincroniza o menu de checkboxes
        for action in self._filter_menu.actions():
            chk = action.defaultWidget()
            if isinstance(chk, QCheckBox):
                lbl = chk.text()
                idx = self._contas_labels.index(lbl)
                chk.setChecked(not self.tabela.isColumnHidden(idx))

    def carregar_contas(self):
        rows = self.db.fetch_all(
            "SELECT id,cod_conta,nome_banco,agencia,num_conta,saldo_inicial FROM conta_bancaria ORDER BY nome_banco"
        )
        self.tabela.setRowCount(len(rows))
        for r, (id_, cod, banco, ag, cont, saldo) in enumerate(rows):
            br = f"{saldo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            vals = [cod, banco, ag, cont, f"R$ {br}"]
            for c, v in enumerate(vals):
                itm = QTableWidgetItem(v)
                itm.setTextAlignment(Qt.AlignCenter)
                self.tabela.setItem(r, c, itm)
            # grava o ID no UserRole da primeira coluna
            self.tabela.item(r, 0).setData(Qt.UserRole, id_)

        # limpa seleção e botoes
        self.btn_editar.setEnabled(False)
        self.btn_excluir.setEnabled(False)

    def _select_row(self, row, _):
        self.selected_row = row
        self.btn_editar.setEnabled(True)
        self.btn_excluir.setEnabled(True)

    def nova_conta(self):
        dlg = CadastroContaDialog(self)
        if dlg.exec():
            self.carregar_contas()

    def editar_conta(self):
        id_ = self.tabela.item(self.selected_row, 0).data(Qt.UserRole)
        dlg = CadastroContaDialog(self, id_)
        if dlg.exec():
            self.carregar_contas()

    def excluir_conta(self):
        rows = self.tabela.selectionModel().selectedRows()
        if not rows:
            return
        cods = [self.tabela.item(r.row(),1).text() for r in rows]
        if QMessageBox.question(self, "Excluir", f"Excluir contas: {', '.join(cods)}?") != QMessageBox.Yes:
            return
        for r in rows:
            cid = self.tabela.item(r.row(),0).data(Qt.UserRole)
            self.db.execute_query("DELETE FROM conta_bancaria WHERE id=?", (cid,))
        self.carregar_contas()

# --- WIDGET GERENCIAMENTO IMÓVEIS ---
class GerenciamentoImoveisWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db = Database()
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(10, 10, 10, 10)

        # cabeçalhos e estado de ordenação
        self._imoveis_labels = ["Código", "Nome", "UF", "Área Total", "Área Utilizada", "% Part."]
        self._imoveis_sort_state = {}

        # monta a UI (cria self.tabela e self._imoveis_filter_menu)
        self._build_ui()

        # agora a tabela existe, carregue o filtro salvo
        self._load_imoveis_column_filter()

        # carrega dados iniciais
        self.carregar_imoveis()

    def _build_ui(self):
        tl = QHBoxLayout()
        tl.setContentsMargins(0, 0, 10, 10)

        # Botões CRUD
        self.btn_novo = QPushButton("Novo Imóvel")
        self.btn_novo.clicked.connect(self.novo_imovel)
        tl.addWidget(self.btn_novo)

        self.btn_editar = QPushButton("Editar")
        self.btn_editar.setEnabled(False)
        self.btn_editar.clicked.connect(self.editar_imovel)
        tl.addWidget(self.btn_editar)

        self.btn_excluir = QPushButton("Excluir")
        self.btn_excluir.setEnabled(False)
        self.btn_excluir.clicked.connect(self.excluir_imovel)
        tl.addWidget(self.btn_excluir)

        self.btn_importar = QPushButton("Importar")
        self.btn_importar.clicked.connect(self.importar_imoveis)
        tl.addWidget(self.btn_importar)

        # barra de pesquisa
        self.search_imoveis = QLineEdit()
        self.search_imoveis.setPlaceholderText("Pesquisar imóveis…")
        self.search_imoveis.textChanged.connect(self._filter_imoveis)
        tl.addWidget(self.search_imoveis)

        # botão de filtro de colunas
        btn_filter = QToolButton()
        btn_filter.setText("⚙️")
        btn_filter.setAutoRaise(True)
        btn_filter.setPopupMode(QToolButton.InstantPopup)
        self._imoveis_filter_menu = QMenu(self)
        for col, lbl in enumerate(self._imoveis_labels):
            wa = QWidgetAction(self._imoveis_filter_menu)
            chk = QCheckBox(lbl)
            chk.setChecked(True)
            chk.toggled.connect(lambda vis, c=col: self._toggle_imoveis_column(c, vis))
            wa.setDefaultWidget(chk)
            self._imoveis_filter_menu.addAction(wa)
        btn_filter.setMenu(self._imoveis_filter_menu)
        tl.addWidget(btn_filter)

        tl.addStretch()
        self.layout.addLayout(tl)

        # Tabela
        self.tabela = QTableWidget(0, len(self._imoveis_labels))
        self.tabela.setHorizontalHeaderLabels(self._imoveis_labels)
        self.tabela.setEditTriggers(QTableWidget.NoEditTriggers)
        self.tabela.setSelectionBehavior(QTableWidget.SelectRows)
        self.tabela.setSelectionMode(QTableWidget.SingleSelection)
        self.tabela.setAlternatingRowColors(True)
        self.tabela.setShowGrid(False)
        self.tabela.verticalHeader().setVisible(False)
        hdr = self.tabela.horizontalHeader()
        hdr.setHighlightSections(False)
        hdr.setDefaultAlignment(Qt.AlignCenter)
        hdr.setSectionResizeMode(QHeaderView.Stretch)
        hdr.sectionDoubleClicked.connect(self._toggle_sort_imoveis)
        self.tabela.cellClicked.connect(self._select_row)
        self.layout.addWidget(self.tabela)

    def _toggle_sort_imoveis(self, index: int):
        state = self._imoveis_sort_state.get(index, 0)
        if state == 0:
            self.tabela.sortItems(index, Qt.AscendingOrder); new = 1
        elif state == 1:
            self.tabela.sortItems(index, Qt.DescendingOrder); new = 2
        else:
            self.carregar_imoveis(); new = 0
        self._imoveis_sort_state = {index: new}

    def _toggle_imoveis_column(self, col: int, visible: bool):
        """Esconde/exibe a coluna e salva apenas a seção 'imoveis' no lanc_filter.json."""
        self.tabela.setColumnHidden(col, not visible)
        self._save_imoveis_column_filter()

    def _save_imoveis_column_filter(self):
        """Atualiza só o tópico 'imoveis' em lanc_filter.json, preservando as outras seções."""
        path = os.path.join(CACHE_FOLDER, 'Cleuber Marcos', 'json', "lanc_filter.json")
        # carrega tudo (ou cria vazio)
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}
        # gera lista de visibilidade
        vis = [not self.tabela.isColumnHidden(c)
               for c in range(self.tabela.columnCount())]
        # atualiza só o tópico imoveis
        cfg["imoveis"] = vis
        # salva de volta
        os.makedirs(CACHE_FOLDER, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)

    def _load_imoveis_column_filter(self):
        """Lê o tópico 'imoveis' de lanc_filter.json e aplica à tabela."""
        path = os.path.join(CACHE_FOLDER, 'Cleuber Marcos', 'json', "lanc_filter.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            vis = cfg.get("imoveis", [])
        except Exception:
            return
        # aplica visibilidade
        for c, shown in enumerate(vis):
            self.tabela.setColumnHidden(c, not shown)
        # sincroniza o menu de checkboxes
        for wa in self._imoveis_filter_menu.actions():
            chk = wa.defaultWidget()
            if isinstance(chk, QCheckBox):
                idx = self._imoveis_labels.index(chk.text())
                chk.setChecked(not self.tabela.isColumnHidden(idx))

    def _filter_imoveis(self, text: str):
        text = text.lower()
        for row in range(self.tabela.rowCount()):
            hide = True
            for col in range(self.tabela.columnCount()):
                item = self.tabela.item(row, col)
                if item and text in item.text().lower():
                    hide = False
                    break
            self.tabela.setRowHidden(row, hide)

    def importar_imoveis(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Importar Imóveis", "", "TXT (*.txt);;Excel (*.xlsx *.xls)"
        )
        if not path:
            return

        try:
            # Lê apenas a primeira linha para contar o número de campos
            with open(path, encoding='utf-8') as f:
                first = f.readline().strip().split('|')

            # Se for arquivo de lançamentos (11 ou 12 campos), delega para o importador correto
            if len(first) in (11, 12):
                main_win = self.window()  # assume que é MainWindow
                if path.lower().endswith('.txt'):
                    main_win._import_lancamentos_txt(path)
                else:
                    main_win._import_lancamentos_excel(path)
                main_win.carregar_lancamentos()
                main_win.dashboard.load_data()
            else:
                # Caso contrário, importa como arquivo de imóveis
                if path.lower().endswith('.txt'):
                    self._import_imoveis_txt(path)
                else:
                    self._import_imoveis_excel(path)
                self.carregar_imoveis()

        except Exception as e:
            QMessageBox.warning(self, "Importação Falhou", str(e))


    def _import_imoveis_txt(self, path: str):
        with open(path, encoding='utf-8') as f:
            for lineno, line in enumerate(f, 1):
                parts = line.strip().split("|")
                if len(parts) != 18:
                    raise ValueError(
                        f"Linha {lineno}: esperado 18 campos, encontrou {len(parts)}"
                    )
                (
                    cod_imovel, pais, moeda, cad_itr, caepf, insc_estadual,
                    nome_imovel, endereco, num, compl, bairro, uf,
                    cod_mun, cep, tipo_exploracao, participacao,
                    area_total, area_utilizada
                ) = parts
                self.db.execute_query(
                    """
                    INSERT OR REPLACE INTO imovel_rural (
                        cod_imovel, pais, moeda, cad_itr, caepf, insc_estadual,
                        nome_imovel, endereco, num, compl, bairro, uf,
                        cod_mun, cep, tipo_exploracao, participacao,
                        area_total, area_utilizada
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    [
                        cod_imovel, pais, moeda,
                        cad_itr or None, caepf or None, insc_estadual or None,
                        nome_imovel, endereco,
                        num or None, compl or None, bairro, uf,
                        cod_mun, cep,
                        int(tipo_exploracao), float(participacao),
                        float(area_total), float(area_utilizada)
                    ]
                )

    def _import_imoveis_excel(self, path: str):
        df = pd.read_excel(path, dtype=str)
        required = [
            'cod_imovel','pais','moeda','cad_itr','caepf','insc_estadual',
            'nome_imovel','endereco','num','compl','bairro','uf',
            'cod_mun','cep','tipo_exploracao','participacao',
            'area_total','area_utilizada'
        ]
        if not all(col in df.columns for col in required):
            raise ValueError("Layout de Excel inválido")
        df.fillna('', inplace=True)
        for row in df.itertuples(index=False):
            self.db.execute_query(
                """
                INSERT OR REPLACE INTO imovel_rural (
                    cod_imovel, pais, moeda, cad_itr, caepf, insc_estadual,
                    nome_imovel, endereco, num, compl, bairro, uf,
                    cod_mun, cep, tipo_exploracao, participacao,
                    area_total, area_utilizada
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row.cod_imovel, row.pais, row.moeda,
                    row.cad_itr or None, row.caepf or None, row.insc_estadual or None,
                    row.nome_imovel, row.endereco,
                    row.num or None, row.compl or None, row.bairro, row.uf,
                    row.cod_mun, row.cep,
                    int(row.tipo_exploracao), float(row.participacao),
                    float(row.area_total), float(row.area_utilizada)
                )
            )

    def carregar_imoveis(self):
        rows = self.db.fetch_all(
            "SELECT id, cod_imovel, nome_imovel, uf, area_total, area_utilizada, participacao "
            "FROM imovel_rural ORDER BY nome_imovel"
        )
        self.tabela.setRowCount(len(rows))
        for r, (id_, cod, nome, uf, at, au, part) in enumerate(rows):
            vals = [cod, nome, uf, f"{at or 0:.2f} ha", f"{au or 0:.2f} ha", f"{part:.2f}%"]
            for c, v in enumerate(vals):
                itm = QTableWidgetItem(v)
                itm.setTextAlignment(Qt.AlignCenter)
                self.tabela.setItem(r, c, itm)
            # grava o ID no item para que o editar saiba qual registro carregar
            self.tabela.item(r, 0).setData(Qt.UserRole, id_)

    def _on_select(self, row, _):
        self.selected_row = row
        self.btn_editar.setEnabled(True)
        self.btn_excluir.setEnabled(True)

    def _select_row(self,row,_):
        self.selected_row = row
        self.btn_editar.setEnabled(True)
        self.btn_excluir.setEnabled(True)

    def novo_imovel(self):
        dlg = CadastroImovelDialog(self)
        if dlg.exec():
            self.carregar_imoveis()

    def editar_imovel(self):
        id_ = self.tabela.item(self.selected_row,0).data(Qt.UserRole)
        dlg = CadastroImovelDialog(self, id_)
        if dlg.exec():
            self.carregar_imoveis()

    def excluir_imovel(self):
        indices = self.tabela.selectionModel().selectedRows()
        if not indices:
            return
        nomes = [self.tabela.item(idx.row(), 1).text() for idx in indices]
        resp = QMessageBox.question(
            self, "Confirmar Exclusão",
            f"Excluir imóveis: {', '.join(nomes)}?",
            QMessageBox.Yes | QMessageBox.No
        )
        if resp != QMessageBox.Yes:
            return
        for idx in indices:
            id_ = self.tabela.item(idx.row(), 0).data(Qt.UserRole)
            try:
                self.db.execute_query("DELETE FROM imovel_rural WHERE id=?", (id_,))
            except Exception as e:
                QMessageBox.critical(self, "Erro", f"Erro ao excluir imóvel ID {id_}: {e}")
        self.carregar_imoveis()

            
class GerenciamentoParticipantesWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db = Database()
        self._participantes_labels = ["CPF/CNPJ","Nome","Tipo","Cadastro"]
        self._participantes_sort_state = {}
        self.layout = QVBoxLayout(self); self.layout.setContentsMargins(10,10,10,10)
        self._build_ui(); self._load_participantes_column_filter(); self.carregar_participantes()

    def _build_ui(self):
        tl = QHBoxLayout(); tl.setContentsMargins(0,0,10,10)
        self.btn_novo = QPushButton("Novo Participante"); self.btn_novo.setIcon(QIcon.fromTheme("document-new")); self.btn_novo.clicked.connect(self.novo_participante); tl.addWidget(self.btn_novo)
        self.btn_editar = QPushButton("Editar"); self.btn_editar.setEnabled(False); self.btn_editar.setIcon(QIcon.fromTheme("document-edit")); self.btn_editar.clicked.connect(self.editar_participante); tl.addWidget(self.btn_editar)
        self.btn_excluir = QPushButton("Excluir"); self.btn_excluir.setEnabled(False); self.btn_excluir.setIcon(QIcon.fromTheme("edit-delete")); self.btn_excluir.clicked.connect(self.excluir_participante); tl.addWidget(self.btn_excluir)
        self.btn_importar = QPushButton("Importar"); self.btn_importar.setIcon(QIcon.fromTheme("document-import")); self.btn_importar.clicked.connect(self.importar_participantes); tl.addWidget(self.btn_importar)
        self.search_part = QLineEdit(); self.search_part.setPlaceholderText("Pesquisar participantes…"); self.search_part.textChanged.connect(self._filter_participantes); tl.addWidget(self.search_part)

        btn_filter = QToolButton(); btn_filter.setText("⚙️"); btn_filter.setAutoRaise(True); btn_filter.setPopupMode(QToolButton.InstantPopup)
        self._part_filter_menu = QMenu(self)
        for col, lbl in enumerate(self._participantes_labels):
            wa = QWidgetAction(self._part_filter_menu); chk = QCheckBox(lbl); chk.setChecked(True)
            chk.toggled.connect(lambda vis, c=col: self._toggle_participantes_column(c, vis))
            wa.setDefaultWidget(chk); self._part_filter_menu.addAction(wa)
        btn_filter.setMenu(self._part_filter_menu); tl.addWidget(btn_filter)

        tl.addStretch(); self.layout.addLayout(tl)

        self.tabela = QTableWidget(0, len(self._participantes_labels))
        self.tabela.setHorizontalHeaderLabels(self._participantes_labels)
        self.tabela.setAlternatingRowColors(True); self.tabela.setShowGrid(False); self.tabela.verticalHeader().setVisible(False)
        self.tabela.setSelectionBehavior(QTableWidget.SelectRows); self.tabela.setEditTriggers(QTableWidget.NoEditTriggers)
        self.tabela.cellClicked.connect(self._select_row)
        hdr = self.tabela.horizontalHeader(); hdr.setHighlightSections(False); hdr.setDefaultAlignment(Qt.AlignCenter)
        hdr.setSectionResizeMode(QHeaderView.Stretch); hdr.sectionDoubleClicked.connect(self._toggle_sort_participantes)
        self.layout.addWidget(self.tabela)


    def _toggle_sort_participantes(self, index: int):
        state = self._participantes_sort_state.get(index, 0)
        if state == 0: self.tabela.sortItems(index, Qt.AscendingOrder); new = 1
        elif state == 1: self.tabela.sortItems(index, Qt.DescendingOrder); new = 2
        else: self.carregar_participantes(); new = 0
        self._participantes_sort_state = {index: new}

    def _toggle_participantes_column(self, col: int, visible: bool):
        self.tabela.setColumnHidden(col, not visible)
        self._save_participantes_column_filter()

    def _save_participantes_column_filter(self):
        path = os.path.join(CACHE_FOLDER, 'Cleuber Marcos', 'json', "lanc_filter.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except:
            cfg = {}
        cfg["participantes"] = [not self.tabela.isColumnHidden(c) for c in range(self.tabela.columnCount())]
        os.makedirs(CACHE_FOLDER, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)

    def _load_participantes_column_filter(self):
        path = os.path.join(CACHE_FOLDER, 'Cleuber Marcos', 'json', "lanc_filter.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            vis = cfg.get("participantes", [])
        except:
            return
        for c, shown in enumerate(vis):
            self.tabela.setColumnHidden(c, not shown)
        for wa in self._part_filter_menu.actions():
            chk = wa.defaultWidget()
            if isinstance(chk, QCheckBox):
                idx = self._participantes_labels.index(chk.text())
                chk.setChecked(not self.tabela.isColumnHidden(idx))


    def _filter_participantes(self, text: str):
        text = text.lower()
        for row in range(self.tabela.rowCount()):
            hide = all(text not in (self.tabela.item(row, col).text().lower() if self.tabela.item(row, col) else '') for col in range(self.tabela.columnCount()))
            self.tabela.setRowHidden(row, hide)

    def importar_participantes(self):
        path, _ = QFileDialog.getOpenFileName(self, "Importar Participantes", "", "TXT (*.txt);;Excel (*.xlsx *.xls)")
        if not path: return
        try:
            self._import_participantes_txt(path) if path.lower().endswith('.txt') else self._import_participantes_excel(path)
            self.carregar_participantes()
        except Exception:
            QMessageBox.warning(self, "Importação Falhou", "Arquivo não segue o layout esperado e não foi importado.")

    def _import_participantes_txt(self, path):
        with open(path, encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) != 3: raise ValueError("Layout de TXT inválido")
                cpf_cnpj, nome, tipo = parts
                self.db.execute_query("INSERT OR REPLACE INTO participante (cpf_cnpj, nome, tipo_contraparte) VALUES (?, ?, ?)", (cpf_cnpj.strip(), nome.strip(), int(tipo)))

    def _import_participantes_excel(self, path):
        df = pd.read_excel(path, dtype=str)
        required = ['cpf_cnpj','nome','tipo_contraparte']
        if not all(col in df.columns for col in required): raise ValueError("Layout de Excel inválido")
        df.fillna('', inplace=True)
        for row in df.itertuples(index=False):
            self.db.execute_query("INSERT OR REPLACE INTO participante (cpf_cnpj, nome, tipo_contraparte) VALUES (?, ?, ?)", (row.cpf_cnpj.strip(), row.nome.strip(), int(row.tipo_contraparte)))

    def carregar_participantes(self):
        rows = self.db.fetch_all("SELECT id,cpf_cnpj,nome,tipo_contraparte,data_cadastro FROM participante ORDER BY data_cadastro DESC")
        self.tabela.setRowCount(len(rows)); tipos = {1:"PJ",2:"PF",3:"Órgão Público",4:"Outros"}
        for r, (id_, cpf, nome, tipo, data_str) in enumerate(rows):
            formatted_date = QDate.fromString(data_str, "yyyy-MM-dd").toString("dd/MM/yyyy")
            for c, v in enumerate([cpf, nome, tipos.get(tipo, str(tipo)), formatted_date]):
                item = QTableWidgetItem(v); item.setTextAlignment(Qt.AlignCenter); self.tabela.setItem(r, c, item)
            self.tabela.item(r, 0).setData(Qt.UserRole, id_)
        self.btn_editar.setEnabled(False); self.btn_excluir.setEnabled(False)

    def _select_row(self, row, _):
        self.selected_row = row
        self.btn_editar.setEnabled(True)
        self.btn_excluir.setEnabled(True)

    def novo_participante(self):
        dlg = CadastroParticipanteDialog(self)
        if dlg.exec(): self.carregar_participantes()

    def editar_participante(self):
        id_ = self.tabela.item(self.selected_row,0).data(Qt.UserRole)
        dlg = CadastroParticipanteDialog(self, id_)
        if dlg.exec(): self.carregar_participantes()

    def excluir_participante(self):
        indices = self.tabela.selectionModel().selectedRows()
        if not indices: return
        nomes = [self.tabela.item(idx.row(), 1).text() for idx in indices]
        resp = QMessageBox.question(self, "Confirmar Exclusão", f"Excluir participantes: {', '.join(nomes)}?", QMessageBox.Yes | QMessageBox.No)
        if resp != QMessageBox.Yes: return
        for idx in indices:
            pid = self.tabela.item(idx.row(), 0).data(Qt.UserRole)
            try: self.db.execute_query("DELETE FROM participante WHERE id=?", (pid,))
            except Exception as e: QMessageBox.critical(self, "Erro", f"Erro ao excluir participante ID {pid}: {e}")
        self.carregar_participantes()


# --- WIDGET CADASTROS COM ABAS ---
class CadastrosWidget(QTabWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTabPosition(QTabWidget.West)
        self.addTab(GerenciamentoImoveisWidget(), "Imóveis")
        self.addTab(GerenciamentoContasWidget(), "Contas")
        self.addTab(GerenciamentoParticipantesWidget(), "Participantes")
        self.addTab(QWidget(), "Culturas")
        self.addTab(QWidget(), "Áreas")
        self.addTab(QWidget(), "Estoque")
        icons = ["home","credit-card","user-group","tree","map","box"]
        for i,ic in enumerate(icons):
            self.setTabIcon(i, QIcon.fromTheme(ic))

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__(); self.db = Database()
        self.setWindowTitle("Sistema AgroContábil - LCDPR")
        self.setGeometry(100,100,1200,800)
        self.setStyleSheet(STYLE_SHEET)
        self._lanc_labels = ["ID","Data","Imóvel","Documento","Participante","Histórico","Tipo","Entrada","Saída","Saldo","Usuário"]
        self._setup_ui(); self._lanc_sort_state = {}

    def _setup_ui(self):
        self.setWindowIcon(QIcon(APP_ICON))
        self._create_menu(); self._create_toolbar()
        self.tabs = QTabWidget(); self.tabs.setContentsMargins(10,10,10,10)
        self.setCentralWidget(self.tabs)

        # Painel
        self.dashboard = DashboardWidget(); self.tabs.addTab(self.dashboard, "Painel")

        # Lançamentos
        w_l = QWidget(); l_l = QVBoxLayout(w_l); l_l.setContentsMargins(10,10,10,10)
        self.lanc_filter_layout = QHBoxLayout()
        self.lanc_filter_layout.addWidget(QLabel("De:"))
        self.dt_ini = QDateEdit(QDate.currentDate().addMonths(-1)); self.dt_ini.setCalendarPopup(True); self.dt_ini.setDisplayFormat("dd/MM/yyyy")
        self.lanc_filter_layout.addWidget(self.dt_ini)
        self.lanc_filter_layout.addWidget(QLabel("Até:"))
        self.dt_fim = QDateEdit(QDate.currentDate()); self.dt_fim.setCalendarPopup(True); self.dt_fim.setDisplayFormat("dd/MM/yyyy")
        self.lanc_filter_layout.addWidget(self.dt_fim)

        btn_filtrar = QPushButton("Filtrar"); btn_filtrar.clicked.connect(self.carregar_lancamentos); self.lanc_filter_layout.addWidget(btn_filtrar)
        self.btn_edit_lanc = QPushButton("Editar Lançamento"); self.btn_edit_lanc.setEnabled(False); self.btn_edit_lanc.clicked.connect(self.editar_lancamento)
        self.lanc_filter_layout.addWidget(self.btn_edit_lanc)
        self.btn_del_lanc = QPushButton("Excluir Lançamento"); self.btn_del_lanc.setEnabled(False); self.btn_del_lanc.clicked.connect(self.excluir_lancamento)
        self.lanc_filter_layout.addWidget(self.btn_del_lanc)
        self.btn_import_lanc = QPushButton("Importar Lançamentos"); self.btn_import_lanc.setIcon(QIcon.fromTheme("document-import"))
        self.btn_import_lanc.clicked.connect(self.importar_lancamentos); self.lanc_filter_layout.addWidget(self.btn_import_lanc)
        self.search_lanc = QLineEdit(); self.search_lanc.setPlaceholderText("Pesquisar…"); self.search_lanc.textChanged.connect(self._filter_lancamentos)
        self.lanc_filter_layout.addWidget(self.search_lanc)
        self.btn_filter = QToolButton(); self.btn_filter.setText("⚙️"); self.btn_filter.setAutoRaise(True); self.btn_filter.setPopupMode(QToolButton.InstantPopup)
        self.lanc_filter_layout.addWidget(self.btn_filter)

        self._lanc_filter_menu = QMenu(self)
        for col, lbl in enumerate(self._lanc_labels):
            wa = QWidgetAction(self._lanc_filter_menu)
            chk = QCheckBox(lbl); chk.setChecked(True)
            chk.toggled.connect(lambda vis, c=col: self._toggle_lanc_column(c, vis))
            wa.setDefaultWidget(chk); self._lanc_filter_menu.addAction(wa)
        self.btn_filter.setMenu(self._lanc_filter_menu)

        l_l.addLayout(self.lanc_filter_layout)
        self.tab_lanc = QTableWidget(0, len(self._lanc_labels)); self.tab_lanc.setHorizontalHeaderLabels(self._lanc_labels)
        self.tab_lanc.setSelectionBehavior(QTableWidget.SelectRows); self.tab_lanc.setEditTriggers(QTableWidget.NoEditTriggers)
        self.tab_lanc.cellClicked.connect(lambda r,_: (self.btn_edit_lanc.setEnabled(True), self.btn_del_lanc.setEnabled(True)))
        l_l.addWidget(self.tab_lanc)

        config_file = os.path.join(CACHE_FOLDER, 'Cleuber Marcos', 'json', 'lanc_columns.json')
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                vis = json.load(f)
            for i, label in enumerate(self.tab_lanc.horizontalHeaderLabels()):
                self.tab_lanc.setColumnHidden(i, not vis.get(label, True))

        hdr = self.tab_lanc.horizontalHeader()
        for i, _ in enumerate(self._lanc_labels):
            hdr.setSectionResizeMode(i, QHeaderView.ResizeToContents if self._lanc_labels[i]=="Usuário" else QHeaderView.Stretch)
        self.tab_lanc.setAlternatingRowColors(True); self.tab_lanc.setShowGrid(False); self.tab_lanc.verticalHeader().setVisible(False)
        hdr.setHighlightSections(False); hdr.setDefaultAlignment(Qt.AlignCenter)
        self.tab_lanc.setStyleSheet("QTableWidget::item { padding: 8px; } QHeaderView::section { padding: 8px; font-weight: bold; }")
        self.tabs.addTab(w_l, "Lançamentos")

        # Cadastros
        self.cadw = CadastrosWidget(); self.tabs.addTab(self.cadw, "Cadastros")

        # Planejamento
        w_p = QWidget(); l_p = QVBoxLayout(w_p); l_p.setContentsMargins(10,10,10,10)
        self.tab_plan = QTableWidget(0, 5); self.tab_plan.setHorizontalHeaderLabels(["Cultura", "Área", "Plantio", "Colheita Est.", "Prod. Est."])
        self.tab_plan.setSelectionBehavior(QTableWidget.SelectRows); self.tab_plan.setEditTriggers(QTableWidget.NoEditTriggers)
        l_p.addWidget(self.tab_plan)
        self.tab_plan.setAlternatingRowColors(True); self.tab_plan.setShowGrid(False); self.tab_plan.verticalHeader().setVisible(False)
        hdr2 = self.tab_plan.horizontalHeader(); hdr2.setHighlightSections(False); hdr2.setDefaultAlignment(Qt.AlignCenter); hdr2.setSectionResizeMode(QHeaderView.Stretch)
        self.tab_plan.setStyleSheet("QTableWidget::item { padding: 8px; } QHeaderView::section { padding: 8px; font-weight: bold; }")
        self.tabs.addTab(w_p, "Planejamento")

        # Status
        self.status = QStatusBar(); self.setStatusBar(self.status); self.status.showMessage("Sistema iniciado com sucesso!")

        # Dados iniciais
        self.carregar_lancamentos(); self.profile_selector.setCurrentText("Cleuber Marcos")
        self.carregar_planejamento(); self._load_lanc_filter_settings()

    def _toggle_lanc_column(self, col: int, visible: bool):
        self.tab_lanc.setColumnHidden(col, not visible); self._save_lanc_filter_settings()

    def _save_lanc_filter_settings(self):
        os.makedirs(CACHE_FOLDER, exist_ok=True)
        path = os.path.join(CACHE_FOLDER, 'Cleuber Marcos', 'json', "lanc_filter.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}
        cfg["lancamentos"] = [not self.tab_lanc.isColumnHidden(c) for c in range(self.tab_lanc.columnCount())]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    
    def _load_lanc_filter_settings(self):
        path = os.path.join(CACHE_FOLDER, 'Cleuber Marcos', 'json', "lanc_filter.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
                vis = cfg.get("lancamentos", [])
        except Exception:
            return
        for c, shown in enumerate(vis):
            self.tab_lanc.setColumnHidden(c, not shown)
        for wa in self._lanc_filter_menu.actions():
            if isinstance(wa, QWidgetAction):
                chk = wa.defaultWidget()
                if isinstance(chk, QCheckBox):
                    label = chk.text()
                    try:
                        idx = self._lanc_labels.index(label)
                    except ValueError:
                        continue
                    chk.setChecked(not self.tab_lanc.isColumnHidden(idx))
    
    
    def toggle_sort_lanc(self, index: int):
        state = self._lanc_sort_state.get(index, 0)
        if state == 0: self.tab_lanc.sortItems(index, Qt.AscendingOrder); new = 1
        elif state == 1: self.tab_lanc.sortItems(index, Qt.DescendingOrder); new = 2
        else: self.carregar_lancamentos(); new = 0
        self._lanc_sort_state = {index: new}

    def show_lanc_filter_dialog(self):
        dlg = QDialog(self); dlg.setWindowTitle("Filtro de Colunas"); layout = QVBoxLayout(dlg)
        labels = [self.tab_lanc.horizontalHeaderItem(col).text() for col in range(self.tab_lanc.columnCount())]
        for col, label in enumerate(labels):
            chk = QCheckBox(label); chk.setChecked(not self.tab_lanc.isColumnHidden(col))
            chk.stateChanged.connect(lambda state, c=col: self.tab_lanc.setColumnHidden(c, state != Qt.Checked))
            layout.addWidget(chk)
        dlg.exec()

    def _filter_lancamentos(self, text: str):
        text = text.lower()
        for row in range(self.tab_lanc.rowCount()):
            hide = all(text not in (self.tab_lanc.item(row, col).text().lower() if self.tab_lanc.item(row, col) else '') for col in range(self.tab_lanc.columnCount()))
            self.tab_lanc.setRowHidden(row, hide)

    def _create_menu(self):
        mb = self.menuBar(); m1 = mb.addMenu("&Arquivo")
        m1.addAction(QAction("Novo Lançamento", self, triggered=self.novo_lancamento))
        m1.addAction(QAction("Exportar Dados", self, triggered=self.exportar_dados)); m1.addSeparator()
        m1.addAction(QAction("Sair", self, triggered=self.close))
        m2 = mb.addMenu("&Cadastros")
        for txt, fn in [("Imóvel Rural", lambda: self.tabs.setCurrentIndex(1)),
                        ("Conta Bancária", lambda: self.tabs.setCurrentIndex(2)),
                        ("Participante", lambda: self.tabs.setCurrentIndex(3)),
                        ("Cultura", lambda: QMessageBox.information(self, "Cultura", "Em desenvolvimento"))]:
            m2.addAction(QAction(txt, self, triggered=fn))
        m2.addAction(QAction("Parâmetros", self, triggered=self.abrir_parametros))
        m3 = mb.addMenu("&Relatórios")
        m3.addAction(QAction("Balancete", self, triggered=self.abrir_balancete))
        m3.addAction(QAction("Razão", self, triggered=self.abrir_razao))
        m4 = mb.addMenu("&Ajuda")
        m4.addAction(QAction("Manual do Usuário", self))
        m4.addAction(QAction("Sobre o Sistema", self, triggered=self.mostrar_sobre))

    def abrir_parametros(self): ParametrosDialog(self).exec()

    def _create_toolbar(self):
        tb = QToolBar("Barra de Ferramentas", self); tb.setIconSize(QSize(32, 32))
        self.addToolBar(Qt.LeftToolBarArea, tb)
        tb.addAction(QAction(QIcon(os.path.join(ICONS_DIR, "add.png")), "Novo Lançamento", self, triggered=self.novo_lancamento))
        tb.addAction(QAction(QIcon(os.path.join(ICONS_DIR, "farm.png")), "Cad. Imóvel", self, triggered=self.cad_imovel))
        tb.addAction(QAction(QIcon(os.path.join(ICONS_DIR, "bank.png")), "Cad. Conta", self, triggered=self.cad_conta))
        tb.addAction(QAction(QIcon(os.path.join(ICONS_DIR, "users.png")), "Cad. Participante", self, triggered=self.cad_participante))
        tb.addAction(QAction(QIcon(os.path.join(ICONS_DIR, "report.png")), "Relatórios", self, triggered=lambda: self.tabs.setCurrentIndex(4)))
        tb.addAction(QAction(QIcon(os.path.join(ICONS_DIR, "txt.png")), "Arquivo LCDPR", self, triggered=self.arquivo_lcdpr))
        tb.addSeparator(); tb.addWidget(QLabel("Perfil:"))
        self.profile_selector = QComboBox()
        self.profile_selector.addItems(["Cleuber Marcos", "Gilson Oliveira", "Adriana Lucia", "Lucas Laignier"])
        self.profile_selector.setCurrentText(CURRENT_PROFILE)
        self.profile_selector.currentTextChanged.connect(self.switch_profile)
        tb.addWidget(self.profile_selector)

    def switch_profile(self, profile: str):
        global CURRENT_PROFILE
        if profile == CURRENT_PROFILE: return
        CURRENT_PROFILE = profile
        self.db.conn.close(); self.db = Database()
        self.dashboard.db.conn.close(); self.dashboard.db = Database(); self.dashboard.load_data()
        self.carregar_lancamentos(); self.carregar_planejamento()
        im_w = self.cadw.widget(0); im_w.db.conn.close(); im_w.db = Database(); im_w.carregar_imoveis()
        ct_w = self.cadw.widget(1); ct_w.db.conn.close(); ct_w.db = Database(); ct_w.carregar_contas()
        QMessageBox.information(self, "Perfil alterado", f"Perfil Trocado para: '{profile}'.")

    def arquivo_lcdpr(self):
        dlg = QDialog(self); dlg.setWindowTitle("Arquivo LCDPR"); dlg.setMinimumSize(400, 200); layout = QVBoxLayout(dlg)
        btn_export_txt = QPushButton("Exportar TXT LCDPR"); btn_export_plan = QPushButton("Exportar Planilha LCDPR")
        btn_import_txt = QPushButton("Importar TXT LCDPR"); btn_import_plan = QPushButton("Importar Planilha LCDPR")
        for btn in (btn_export_txt, btn_export_plan, btn_import_txt, btn_import_plan): layout.addWidget(btn)
        btn_export_txt.clicked.connect(lambda: self.show_export_dialog(dlg))
        btn_export_plan.clicked.connect(lambda: (dlg.accept(), self._exportar_planilha_lcdpr()))
        btn_import_txt.clicked.connect(lambda: (dlg.accept(), self.importar_arquivo_lcdpr_txt()))
        btn_import_plan.clicked.connect(lambda: (dlg.accept(), self.importar_arquivo_lcdpr_planilha()))
        dlg.exec()

    def carregar_lancamentos(self):
        d1, d2 = self.dt_ini.date().toString("dd/MM/yyyy"), self.dt_fim.date().toString("dd/MM/yyyy")
        q = f"""SELECT l.id, l.data, i.nome_imovel, l.num_doc, p.nome, l.historico,
                CASE l.tipo_lanc WHEN 1 THEN 'Receita' WHEN 2 THEN 'Despesa' ELSE 'Adiantamento' END,
                l.valor_entrada, l.valor_saida,
                (l.saldo_final * CASE l.natureza_saldo WHEN 'P' THEN 1 ELSE -1 END),
                l.usuario FROM lancamento l
                JOIN imovel_rural i ON l.cod_imovel = i.id
                LEFT JOIN participante p ON l.id_participante = p.id
                WHERE l.data BETWEEN '{d1}' AND '{d2}' ORDER BY l.data DESC"""
        rows = self.db.fetch_all(q); self.tab_lanc.setRowCount(len(rows))
        for r, row in enumerate(rows):
            for c, val in enumerate(row):
                if c == 0: item = NumericItem(int(val))
                elif c == 1: item = QTableWidgetItem(QDate.fromString(val, "dd/MM/yyyy").toString("dd/MM/yyyy"))
                elif c in (7,8,9):
                    num = float(val); br = f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    item = NumericItem(num, f"R$ {br}")
                else: item = QTableWidgetItem(str(val))
                item.setTextAlignment(Qt.AlignCenter)
                if c == 7: item.setForeground(QColor("#27ae60"))
                elif c == 8: item.setForeground(QColor("#e74c3c"))
                elif c == 9: item.setForeground(QColor("#27ae60" if float(val) >= 0 else "#e74c3c"))
                self.tab_lanc.setItem(r, c, item)
            self.tab_lanc.resizeColumnToContents(self._lanc_labels.index("Usuário"))

    def editar_lancamento(self):
        row = self.tab_lanc.currentRow(); lanc_id = int(self.tab_lanc.item(row,0).text())
        dlg = LancamentoDialog(self, lanc_id)
        if dlg.exec(): self.carregar_lancamentos(); self.dashboard.load_data()

    def excluir_lancamento(self):
        indices = self.tab_lanc.selectionModel().selectedRows()
        if not indices: return
        ids = [int(self.tab_lanc.item(idx.row(), 0).text()) for idx in indices]
        resp = QMessageBox.question(self, "Confirmar Exclusão", f"Excluir lançamentos IDs: {', '.join(map(str, ids))}?", QMessageBox.Yes | QMessageBox.No)
        if resp != QMessageBox.Yes: return
        for id_ in ids:
            try: self.db.execute_query("DELETE FROM lancamento WHERE id=?", (id_,))
            except Exception as e: QMessageBox.critical(self, "Erro", f"Erro ao excluir lançamento ID {id_}: {e}")
        if self.db.fetch_one("SELECT COUNT(*) FROM lancamento")[0] == 0:
            self.db.execute_query("DELETE FROM sqlite_sequence WHERE name='lancamento'")
        self.carregar_lancamentos(); self.dashboard.load_data()

    def carregar_planejamento(self):
        perfil = self.profile_selector.currentText()
        db_path = os.path.join(PROJECT_DIR, 'banco_de_dados', perfil, 'data', 'lcdpr.db')
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = sqlite3.connect(db_path); cur = conn.cursor()
        cur.execute("""SELECT c.nome, a.area, a.data_plantio, a.data_colheita_estimada, a.produtividade_estimada
                       FROM area_producao a JOIN cultura c ON a.cultura_id = c.id""")
        rows = cur.fetchall(); conn.close()
        self.tab_plan.setRowCount(len(rows))
        for r, (cultura, area, pl, ce, prod) in enumerate(rows):
            self.tab_plan.setItem(r, 0, QTableWidgetItem(cultura))
            self.tab_plan.setItem(r, 1, QTableWidgetItem(f"{area} ha"))
            self.tab_plan.setItem(r, 2, QTableWidgetItem(pl or ""))
            self.tab_plan.setItem(r, 3, QTableWidgetItem(ce or ""))
            self.tab_plan.setItem(r, 4, QTableWidgetItem(f"{prod}"))

    def novo_lancamento(self):
        dlg = LancamentoDialog(self)
        if dlg.exec(): self.carregar_lancamentos(); self.dashboard.load_data()

    def cad_imovel(self): self.tabs.setCurrentIndex(2); self.cadw.setCurrentIndex(0)
    def cad_conta(self): self.tabs.setCurrentIndex(2); self.cadw.setCurrentIndex(1)
    def cad_participante(self): self.tabs.setCurrentIndex(2); self.cadw.setCurrentIndex(2)


    def exportar_dados(self):
        path, _ = QFileDialog.getSaveFileName(self, "Exportar Dados", "", "CSV (*.csv)")
        if not path: return
        try:
            lancs = self.db.fetch_all("SELECT * FROM lancamento")
            with open(path, 'w', newline='', encoding='utf-8') as f:
                w = csv.writer(f, delimiter=';')
                w.writerow(["ID", "Data", "Imóvel", "Conta", "Documento", "Tipo Doc", "Histórico", "Participante", "Tipo", "Entrada", "Saída", "Saldo", "Natureza", "Categoria"])
                for l in lancs: w.writerow(l[1:])
            QMessageBox.information(self, "Exportação", "Dados exportados com sucesso!")
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro na exportação: {e}")

    def gerar_txt(self, path: str = None):
        if path is None:
            last = load_last_txt_path(); path, _ = QFileDialog.getSaveFileName(self, "Salvar LCDPR", last, "TXT (*.txt)")
            if not path: return

        settings = QSettings("Automatize Tech", "AgroApp")
        ver = settings.value("param/version", "0013"); iden = settings.value("param/ident", ""); nome = settings.value("param/nome", "")
        mov = settings.value("param/ind_mov", "0"); rec = settings.value("param/ind_rec", "0")
        d1, d2 = self.dt_ini.date().toString("dd/MM/yyyy"), self.dt_fim.date().toString("dd/MM/yyyy")

        with open(path, 'w', encoding='utf-8') as f:
            f.write(f"0000|LCDPR|{ver}|{iden}|{nome}|{mov}|{rec}||{self.dt_ini.date().toString('ddMMyyyy')}|{self.dt_fim.date().toString('ddMMyyyy')}\n")
            f.write("0010|1\n")
            log = settings.value("param/logradouro", ""); num = settings.value("param/numero", ""); comp = settings.value("param/complemento", "")
            bai = settings.value("param/bairro", ""); uf = settings.value("param/uf", ""); mun = settings.value("param/cod_mun", "")
            cep = settings.value("param/cep", ""); tel = settings.value("param/telefone", ""); em = settings.value("param/email", "")
            f.write(f"0030|{log}|{num}|{comp}|{bai}|{uf}|{mun}|{cep}|{tel}|{em}\n")

            for im in self.db.fetch_all("SELECT cod_imovel,pais,moeda,cad_itr,caepf,insc_estadual,nome_imovel,endereco,num,compl,bairro,uf,cod_mun,cep,tipo_exploracao,participacao,area_total,area_utilizada FROM imovel_rural"):
                f.write("0040|" + "|".join(str(x or "") for x in im) + "\n")

            for ct in self.db.fetch_all("SELECT cod_conta,pais_cta,banco,nome_banco,agencia,num_conta,saldo_inicial FROM conta_bancaria"):
                cod, pais, ban, nom, ag, numcta, sal = ct
                f.write(f"0050|{cod}|{pais}|{ban or ''}|{nom or ''}|{ag}|{numcta}|{sal:.2f}\n")

            for cpf, nm, tp in self.db.fetch_all("SELECT cpf_cnpj,nome,tipo_contraparte FROM participante"):
                f.write(f"0100|{cpf}|{nm}|{tp}\n")

            for data, cod_im, cod_ct, doc, td, hist, pid, tl, ent, sai, sf, nat in self.db.fetch_all("""
                SELECT l.data,
                       im.cod_imovel,
                       ct.cod_conta,
                       l.num_doc, l.tipo_doc, l.historico, l.id_participante, l.tipo_lanc,
                       l.valor_entrada, l.valor_saida, l.saldo_final, l.natureza_saldo
                  FROM lancamento l
                  JOIN imovel_rural  im ON im.id = l.cod_imovel
                  JOIN conta_bancaria ct ON ct.id = l.cod_conta
                 ORDER BY l.data, l.id
            """):
                f.write("Q100|" + "|".join([data, cod_im, cod_ct, str(doc or ''), str(td), hist or '', str(pid or ''), str(tl), f"{sai:.2f}", f"{ent:.2f}", f"{sf:.2f}", nat]) + "\n")


            d1_str = self.dt_ini.date().toString("yyyy-MM-dd"); d2_str = self.dt_fim.date().toString("yyyy-MM-dd")
            resumo = self.db.fetch_all("SELECT strftime('%m%Y', data), SUM(valor_entrada), SUM(valor_saida) FROM lancamento WHERE data BETWEEN ? AND ? GROUP BY strftime('%m%Y', data)", (d1_str, d2_str))

            for mesano, total_ent, total_sai in resumo:
                row = self.db.fetch_one("SELECT (saldo_final * CASE natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) FROM lancamento WHERE strftime('%m%Y', data)=? ORDER BY id DESC LIMIT 1", (mesano,))
                saldo_mes = row[0] if row and row[0] is not None else 0.0
                ent_ct, sai_ct = int(total_ent * 100), int(total_sai * 100); flag = 'P' if saldo_mes >= 0 else 'N'
                f.write(f"Q200|{mesano}|000|{ent_ct}|{sai_ct}|{flag}\n")

        total_linhas = sum(1 for _ in open(path, 'r', encoding='utf-8')) + 1
        with open(path, 'a', encoding='utf-8') as f:
            f.write(f"9999||||||{total_linhas}\n")

        save_last_txt_path(path)
        QMessageBox.information(self, "Sucesso", f"Arquivo {os.path.basename(path)} gerado!")

        
    def importar_arquivo_lcdpr_txt(self):
        path, _ = QFileDialog.getOpenFileName(self, "Importar arquivo LCDPR", "", "TXT (*.txt)")
        if not path:
            return

        warnings = []
        db = getattr(self, "db", Database())

        def to_float(s: str) -> float:
            s = (s or "").strip()
            if not s:
                return 0.0
            return float(s.replace(".", "").replace(",", ".")) if ("," in s and "." in s and s.rfind(",") > s.rfind(".")) else float(s.replace(",", "."))

        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                for lineno, linha in enumerate(f, start=1):
                    parts = linha.rstrip("\n").split("|")
                    if not parts:
                        continue

                    reg = parts[0]
                    campos = parts[1:]

                    # 0040 – Imóvel Rural (upsert)
                    if reg == "0040" and len(campos) >= 16:
                        cod_imovel, pais, moeda, cad_itr, caepf, insc_est, nome, endereco, num, compl, bairro, uf, cod_mun, cep, tipo_expl, particip = campos[:16]
                        exists = db.fetch_one("SELECT id FROM imovel_rural WHERE cod_imovel = ?", (cod_imovel,))
                        if exists:
                            db.execute_query("""
                                UPDATE imovel_rural
                                   SET pais=?, moeda=?, cad_itr=?, caepf=?, insc_estadual=?, nome_imovel=?,
                                       endereco=?, num=?, compl=?, bairro=?, uf=?, cod_mun=?, cep=?,
                                       tipo_exploracao=?, participacao=?
                                 WHERE cod_imovel=?""",
                                [pais or "BR", moeda or "BRL", cad_itr or None, caepf or None, insc_est or None, nome or "",
                                 endereco or "", num or None, compl or None, bairro or "", uf or "", cod_mun or "", cep or "",
                                 int(tipo_expl or 1), float(particip or 100.0), cod_imovel])
                        else:
                            db.execute_query("""
                                INSERT INTO imovel_rural (cod_imovel, pais, moeda, cad_itr, caepf, insc_estadual,
                                                          nome_imovel, endereco, num, compl, bairro, uf, cod_mun, cep,
                                                          tipo_exploracao, participacao)
                                             VALUES     (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                [cod_imovel, pais or "BR", moeda or "BRL", cad_itr or None, caepf or None, insc_est or None,
                                 nome or "", endereco or "", num or None, compl or None, bairro or "", uf or "", cod_mun or "",
                                 cep or "", int(tipo_expl or 1), float(particip or 100.0)])

                    # 0050 – Conta bancária (upsert)
                    elif reg == "0050" and len(campos) >= 7:
                        cod_conta, pais_cta, banco, nome_banco, agencia, num_conta, saldo_str = campos[:7]
                        saldo_inicial = to_float(saldo_str)
                        exists = db.fetch_one("SELECT id FROM conta_bancaria WHERE cod_conta = ?", (cod_conta,))
                        if exists:
                            db.execute_query("""
                                UPDATE conta_bancaria
                                   SET pais_cta=?, banco=?, nome_banco=?, agencia=?, num_conta=?, saldo_inicial=?
                                 WHERE cod_conta=?""",
                                [pais_cta or "BR", banco or None, nome_banco or "", agencia or "", num_conta or "",
                                 saldo_inicial, cod_conta])
                        else:
                            db.execute_query("""
                                INSERT INTO conta_bancaria (cod_conta, pais_cta, banco, nome_banco, agencia, num_conta, saldo_inicial)
                                                    VALUES (?,        ?,        ?,     ?,          ?,       ?,         ?)""",
                                [cod_conta, pais_cta or "BR", banco or None, nome_banco or "", agencia or "", num_conta or "",
                                 saldo_inicial])

                    # 0100 – Participante (upsert)
                    elif reg == "0100" and len(campos) >= 3:
                        raw_cpf_cnpj, nome_p, tipo_pc = campos[:3]

                        # Apenas dígitos; valida tamanho exato
                        digits = re.sub(r"\D", "", raw_cpf_cnpj or "")
                        if len(digits) == 11:
                            cpf_cnpj_norm = digits  # CPF
                        elif len(digits) == 14:
                            cpf_cnpj_norm = digits  # CNPJ
                        else:
                            warnings.append(f"L{lineno}: 0100 com CPF/CNPJ inválido '{raw_cpf_cnpj}'")
                            continue

                        try:
                            tipo_pc = int((tipo_pc or "3"))
                        except:
                            tipo_pc = 3

                        row = db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj = ?", (cpf_cnpj_norm,))
                        if row:
                            db.execute_query(
                                "UPDATE participante SET nome = ?, tipo_contraparte = ? WHERE id = ?",
                                [nome_p or "", tipo_pc, row[0]]
                            )
                        else:
                            db.execute_query(
                                "INSERT INTO participante (cpf_cnpj, nome, tipo_contraparte) VALUES (?,?,?)",
                                [cpf_cnpj_norm, nome_p or "", tipo_pc]
                            )
                    elif reg == "Q100" and len(campos) >= 12:
                        # Layout no SEU TXT: [9]=ENTRADA, [10]=SAÍDA (ajuste crítico)
                        #   data, cod_imovel, cod_conta, num_doc, tipo_doc, historico,
                        #   cpf_cnpj, tipo_lanc(ignorado), col9=entrada, col10=saida, saldo, natureza
                        data_iso, cod_imv, cod_cta, num_doc, raw_tipo_doc, historico, cpf_cnpj_raw, raw_tipo_lanc, col9, col10, raw_saldo, natureza = campos[:12]

                        # códigos com zeros à esquerda
                        cod_imv = (cod_imv or '').strip().zfill(3)
                        cod_cta = (cod_cta or '').strip().zfill(3)

                        # data para dd/MM/yyyy
                        try:
                            data_str = datetime.strptime(data_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
                        except ValueError:
                            try:
                                data_str = datetime.strptime(data_iso, "%d/%m/%Y").strftime("%d/%m/%Y")
                            except ValueError:
                                warnings.append(f"Linha {lineno}: data inválida '{data_iso}'")
                                continue

                        tipo_doc = int(raw_tipo_doc or 0) or 4

                        # AJUSTE 1: no seu arquivo, coluna 9 é ENTRADA e coluna 10 é SAÍDA
                        valor_entrada = to_float(col9)
                        valor_saida   = to_float(col10)

                        # AJUSTE 2: define tipo_lanc pela movimentação (ignora raw_tipo_lanc do TXT)
                        tipo_lanc = 1 if valor_saida > 0 else (2 if valor_entrada > 0 else (int(raw_tipo_lanc or 0) or 1))

                        saldo_final = abs(to_float(raw_saldo))
                        natureza_saldo = (natureza or "P").strip()[:1]

                        # AJUSTE 3: participante - normaliza CPF/CNPJ e fallback por nome (entre parênteses no histórico)
                        digits = re.sub(r"\D", "", cpf_cnpj_raw or "")
                        row_pa = None
                        if digits:
                            if len(digits) <= 11:
                                row_pa = db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj = ?", (digits.zfill(11),))
                                if not row_pa:
                                    row_pa = db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj = ?", (digits,))
                            else:
                                row_pa = db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj = ?", (digits.zfill(14),))
                                if not row_pa:
                                    row_pa = db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj = ?", (digits,))
                        if not row_pa:
                            m = re.search(r"\(([^)]+)\)\s*$", historico or "")
                            if m:
                                nome_busca = m.group(1).strip()
                                row_pa = db.fetch_one("SELECT id FROM participante WHERE UPPER(nome) LIKE UPPER(?)", (f"%{nome_busca}%",))

                        id_pa = row_pa[0] if row_pa else None

                        # chaves estrangeiras
                        row_im = db.fetch_one("SELECT id FROM imovel_rural  WHERE cod_imovel = ?", (cod_imv,))
                        row_ct = db.fetch_one("SELECT id FROM conta_bancaria WHERE cod_conta  = ?", (cod_cta,))
                        if not row_im:
                            warnings.append(f"Linha {lineno}: imóvel '{cod_imv}' não encontrado (0040)."); continue
                        if not row_ct:
                            warnings.append(f"Linha {lineno}: conta '{cod_cta}' não encontrada (0050)."); continue

                        # insere lançamento já com despesa/receita corretas e participante acertado
                        db.execute_query("""
                            INSERT INTO lancamento (
                                data, cod_imovel, cod_conta, num_doc, tipo_doc, historico,
                                id_participante, tipo_lanc, valor_entrada, valor_saida,
                                saldo_final, natureza_saldo, usuario
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            [data_str, row_im[0], row_ct[0], (num_doc or None), tipo_doc, (historico or ""),
                             id_pa, tipo_lanc, valor_entrada, valor_saida,
                             saldo_final, natureza_saldo, "Importado LCDPR"]
                        )

                    # Q200 – resumo mensal (ignora para o banco)
                    elif reg == "Q200":
                        continue

                    # 9999 – trailer (aviso apenas; não bloqueia)
                    elif reg == "9999" and campos:
                        try:
                            esperado = int(campos[-1])
                            if esperado != lineno:
                                warnings.append(f"Linha {lineno}: trailer espera {esperado} linhas")
                        except Exception:
                            warnings.append(f"Linha {lineno}: trailer inválido")

                    # Demais/linhas vazias -> ignora

            if warnings:
                QMessageBox.warning(self, "Importação concluída com avisos", "\n".join(warnings), QMessageBox.Ok)

            # Atualiza as telas
            self.cadw.widget(0).carregar_imoveis()
            self.cadw.widget(1).carregar_contas()
            self.cadw.widget(2).carregar_participantes()
            self.carregar_lancamentos()
            self.dashboard.load_data()
            QMessageBox.information(self, "Importação", "Arquivo LCDPR importado com sucesso!")
        except Exception as e:
            QMessageBox.warning(self, "Importação Falhou", str(e))


    def _exportar_planilha_lcdpr(self):
        path, _ = QFileDialog.getSaveFileName(self, "Salvar Planilha LCDPR", load_last_txt_path(), "Excel (*.xlsx *.xls)")
        if not path: return
        if not path.lower().endswith(('.xlsx', '.xls')): path += '.xlsx'
        import pandas as pd; rows = []; settings = QSettings("Automatize Tech", "AgroApp")
        ver = settings.value("param/version", "0013"); iden = settings.value("param/ident", "")
        nome = settings.value("param/nome", ""); mov = settings.value("param/ind_mov", "0"); rec = settings.value("param/ind_rec", "0")
        dt1 = self.dt_ini.date().toString("ddMMyyyy"); dt2 = self.dt_fim.date().toString("ddMMyyyy")

        rows.append({"registro":"0000","campo1":"LCDPR","versao":ver,"ident":iden,"nome":nome,"ind_mov":mov,"ind_rec":rec,"vazio":"","data_ini":dt1,"data_fim":dt2})
        rows.append({"registro": "0010", "valor": "1"})

        logradouro = settings.value("param/logradouro", ""); numero = settings.value("param/numero", ""); complemento = settings.value("param/complemento", "")
        bairro = settings.value("param/bairro", ""); uf = settings.value("param/uf", ""); cod_mun = settings.value("param/cod_mun", "")
        cep = settings.value("param/cep", ""); telefone = settings.value("param/telefone", ""); email = settings.value("param/email", "")
        rows.append({"registro": "0030", "logradouro": logradouro, "numero": numero, "complemento": complemento, "bairro": bairro,
                     "uf": uf, "cod_mun": cod_mun, "cep": cep, "telefone": telefone, "email": email})

        for im in self.db.fetch_all("SELECT cod_imovel,pais,moeda,cad_itr,caepf,insc_estadual,nome_imovel,endereco,num,compl,bairro,uf,cod_mun,cep,tipo_exploracao,participacao,area_total,area_utilizada FROM imovel_rural"):
            rows.append(dict(zip(["registro","cod_imovel","pais","moeda","cad_itr","caepf","insc_estadual","nome_imovel","endereco","num","compl","bairro","uf","cod_mun","cep","tipo_exploracao","participacao","area_total","area_utilizada"], ["0040"] + [str(x or "") for x in im])))

        for ct in self.db.fetch_all("SELECT cod_conta,pais_cta,banco,nome_banco,agencia,num_conta,saldo_inicial FROM conta_bancaria"):
            cod_cta, pais_cta, banco, nome_banco, agencia, num_cta, saldo = ct
            rows.append({"registro": "0050", "cod_conta": cod_cta, "pais_cta": pais_cta, "banco": banco or "", "nome_banco": nome_banco or "", "agencia": agencia, "num_conta": num_cta, "saldo_inicial": f"{saldo:.2f}"})

        for cpf, nm, tp in self.db.fetch_all("SELECT cpf_cnpj,nome,tipo_contraparte FROM participante"):
            rows.append({"registro": "0100", "cpf_cnpj": cpf, "nome": nm, "tipo": str(tp)})

        for (data, im_id, ct_id, num_doc, tipo_doc, historico, part_id, tipo_lanc, ent, sai, saldo_f, nat) in self.db.fetch_all("SELECT data,cod_imovel,cod_conta,num_doc,tipo_doc,historico,id_participante,tipo_lanc,valor_entrada,valor_saida,saldo_final,natureza_saldo FROM lancamento"):
            rows.append({"registro": "Q100", "data": data, "cod_imovel": str(im_id), "cod_conta": str(ct_id), "num_doc": num_doc or "", "tipo_doc": str(tipo_doc),
                         "historico": historico, "id_participante": str(part_id or ""), "tipo_lanc": str(tipo_lanc),
                         "valor_entrada": f"{ent:.2f}", "valor_saida": f"{sai:.2f}", "saldo_final": f"{saldo_f:.2f}", "natureza": nat})

        rows.append({"registro": "9999", "EOF": ""}); df = pd.DataFrame(rows); df.to_excel(path, index=False, engine='openpyxl')
        save_last_txt_path(path); QMessageBox.information(self, "Sucesso", f"Planilha {os.path.basename(path)} gerada!")


    def importar_arquivo_lcdpr_planilha(self):
        path, _ = QFileDialog.getOpenFileName(self, "Importar Planilha LCDPR", "", "Excel (*.xlsx *.xls)")
        if not path: return
        import pandas as pd; df = pd.read_excel(path, dtype=str)
        for idx, row in df.iterrows():
            reg = row.get("registro")
            if reg == "0040": pass  # lógica para imóvel
            elif reg == "0050": pass  # lógica para conta
            elif reg == "0100": pass  # lógica para participante
            elif reg == "Q100": pass  # lógica para lançamentos
        self.cadw.widget(0).carregar_imoveis(); self.cadw.widget(1).carregar_contas()
        self.cadw.widget(2).carregar_participantes(); self.carregar_lancamentos(); self.dashboard.load_data()
        QMessageBox.information(self, "Importação", "Arquivo LCDPR Planilha importado com sucesso!")

    def importar_arquivo_lcdpr(self):
        path, _ = QFileDialog.getOpenFileName(self, "Importar arquivo LCDPR", "", "TXT (*.txt);;Todos os arquivos (*)")
        if not path: return
        expected_fields = ["cod_imovel","pais","moeda","cad_itr","caepf","insc_estadual","nome_imovel","endereco","num","compl","bairro","uf","cod_mun","cep","tipo_exploracao","participacao","area_total","area_utilizada"]
        warnings = []
        try:
            with open(path, 'rb') as f:
                for lineno, raw in enumerate(f, start=1):
                    try: linha = raw.decode('utf-8')
                    except UnicodeDecodeError: linha = raw.decode('latin-1')
                    parts = linha.rstrip('\r\n').split("|"); parts = parts[1:] if parts and parts[0] == "" else parts
                    if len(parts) < 2: continue
                    reg, campos = parts[0], parts[1:]

                    if reg == "0040":
                        if len(campos) < 18:
                            nome_im = campos[6].strip() if len(campos) > 6 else "<sem nome>"
                            falt = [expected_fields[i] for i in range(18) if i >= len(campos) or not campos[i].strip()]
                            warnings.append(f"L{lineno}: imóvel '{nome_im}' faltando: {', '.join(falt)}"); campos += [""] * (18 - len(campos))
                        cod_imovel, pais, moeda, cad_itr, caepf, insc_estadual, nome_imovel, endereco, num, compl, bairro, uf, cod_mun, cep, tipo_exploracao, participacao, area_total, area_utilizada = campos[:18]
                        cod_imovel = (cod_imovel or '').strip().zfill(3)  # ← preserva “001”, “002”, …
                        try: participacao_val = float(participacao) / 100.0
                        except: participacao_val = None
                        self.db.execute_query("""INSERT OR REPLACE INTO imovel_rural (cod_imovel,pais,moeda,cad_itr,caepf,insc_estadual,nome_imovel,endereco,num,compl,bairro,uf,cod_mun,cep,tipo_exploracao,participacao,area_total,area_utilizada) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            [cod_imovel or None, pais or None, moeda or None, cad_itr or None, caepf or None, insc_estadual or None, nome_imovel or None, endereco or None, num or None, compl or None, bairro or None, uf or None, cod_mun or None, cep or None, int(tipo_exploracao) if tipo_exploracao.isdigit() else None, participacao_val, float(area_total) if area_total else None, float(area_utilizada) if area_utilizada else None])

                    elif reg == "0050":
                        if len(campos) < 6: continue
                        cod_cta, pais_cta, banco_cod, nome_banco, agencia, num_conta = (campos + [""]*6)[:6]
                        cod_cta = (cod_cta or '').strip().zfill(3)        # ← preserva “001”, “002”, …
                        if len(campos) >= 7:
                            try: saldo_val = float(campos[6].strip().replace(',', '.'))
                            except: saldo_val = 0.0
                        else: saldo_val = 0.0
                        self.db.execute_query("""INSERT OR REPLACE INTO conta_bancaria (cod_conta, pais_cta, banco, nome_banco, agencia, num_conta, saldo_inicial) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            [cod_cta or None, pais_cta or None, banco_cod or None, nome_banco or None, agencia or None, num_conta or None, saldo_val])

                    elif reg == "Q100":
                        # Campos: data, cod_imovel, cod_conta, num_doc, tipo_doc,
                        # historico, cpf_cnpj, tipo_lanc(IGNORAR), valor_saida, valor_entrada, saldo, natureza
                        if len(campos) < 12:
                            warnings.append(f"L{lineno}: Q100 com colunas insuficientes ({len(campos)})")
                            continue

                        data_str, cod_im_raw, cod_cta_raw, num_doc, raw_tipo_doc, historico, cpf_cnpj_raw, _, raw_sai, raw_ent, _, _ = (campos + [""]*12)[:12]

                        # Normaliza códigos (ex.: '6' -> '006')
                        cod_imovel = str(cod_im_raw).strip().zfill(3)
                        cod_conta  = str(cod_cta_raw).strip().zfill(3)

                        # Converte tipos e valores
                        try:
                            tipo_doc = int((raw_tipo_doc or "0").strip())
                        except:
                            tipo_doc = 0

                        def _to_float(s: str) -> float:
                            s = (s or "0").strip().replace(".", "").replace(",", ".")
                            try:
                                return float(s)
                            except:
                                return 0.0

                        # No seu arquivo, o valor NÃO-ZERO está na 10ª coluna e representa SAÍDA (Despesa)
                        sai = _to_float(raw_ent)  # trata a 10ª coluna como SAÍDA
                        ent = _to_float(raw_sai)  # 9ª coluna como ENTRADA (geralmente 0.00)

                        # Tipo do lançamento definido pelo valor: 1=Despesa(saída), 2=Receita(entrada)
                        tipo_lanc = 1 if sai > 0 else (2 if ent > 0 else 1)

                        # Participante: usar somente CPF/CNPJ; se não existir, CADASTRA e vincula
                        cpf_digits = re.sub(r"\D", "", cpf_cnpj_raw or "")
                        id_participante = None
                        if len(cpf_digits) in (11, 14):
                            row_pa = self.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj=?", (cpf_digits,))
                            if row_pa:
                                id_participante = row_pa[0]
                            else:
                                # cadastra automaticamente
                                tipo_pc = 1 if len(cpf_digits) == 14 else 2  # 1=PJ, 2=PF (ajuste conforme seu enum)
                                nome_padrao = (historico or "").strip()[:120] or f"Participante {cpf_digits}"
                                self.db.execute_query(
                                    "INSERT INTO participante (cpf_cnpj, nome, tipo_contraparte) VALUES (?,?,?)",
                                    (cpf_digits, nome_padrao, tipo_pc)
                                )
                                row_pa = self.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj=?", (cpf_digits,))
                                if row_pa:
                                    id_participante = row_pa[0]
                        elif cpf_digits:
                            warnings.append(f"L{lineno}: CPF/CNPJ inválido '{cpf_cnpj_raw}' – lançamento importado sem participante")

                        # Busca chaves estrangeiras
                        im = self.db.fetch_one("SELECT id FROM imovel_rural WHERE cod_imovel=?", (cod_imovel,))
                        if not im:
                            warnings.append(f"L{lineno}: imóvel '{cod_imovel}' não encontrado")
                            continue
                        ct = self.db.fetch_one("SELECT id FROM conta_bancaria WHERE cod_conta=?", (cod_conta,))
                        if not ct:
                            warnings.append(f"L{lineno}: conta '{cod_conta}' não encontrada")
                            continue

                        id_imovel, id_conta = im[0], ct[0]

                        # Calcula saldo anterior e saldo final dessa conta
                        last = self.db.fetch_one(
                            "SELECT (saldo_final * CASE natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) "
                            "FROM lancamento WHERE cod_conta=? ORDER BY id DESC LIMIT 1",
                            (id_conta,)
                        )
                        saldo_ant = last[0] if last and last[0] is not None else 0.0
                        saldo_f = saldo_ant + ent - sai
                        nat = 'P' if saldo_f >= 0 else 'N'

                        # Insere lançamento
                        self.db.execute_query(
                            "INSERT INTO lancamento (data, cod_imovel, cod_conta, num_doc, tipo_doc, historico, "
                            "id_participante, tipo_lanc, valor_entrada, valor_saida, saldo_final, natureza_saldo, usuario) "
                            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (data_str, id_imovel, id_conta, (num_doc or None), tipo_doc, historico,
                             id_participante, int(tipo_lanc), ent, sai, abs(saldo_f), nat, CURRENT_USER)
                        )


            if warnings:
                QMessageBox.warning(self, "Importação concluída com avisos", "\n".join(warnings), QMessageBox.Ok)

            self.cadw.widget(0).carregar_imoveis(); self.cadw.widget(1).carregar_contas()
            self.cadw.widget(2).carregar_participantes(); self.carregar_lancamentos(); self.dashboard.load_data()
            QMessageBox.information(self, "Importação", "Arquivo LCDPR importado com sucesso!")

        except Exception as e:
            QMessageBox.warning(self, "Importação Falhou", str(e))

    def show_export_dialog(self, parent_dialog):
        parent_dialog.hide(); dlg = QDialog(self); dlg.setWindowTitle("Exportar Arquivo LCDPR"); dlg.setMinimumSize(400, 120)
        layout = QVBoxLayout(dlg); hl = QHBoxLayout()
        path_edit = QLineEdit(load_last_txt_path()); path_edit.setPlaceholderText("Cole o caminho ou clique em ...")
        browse = QPushButton("..."); hl.addWidget(path_edit); hl.addWidget(browse); layout.addLayout(hl)

        bl = QHBoxLayout(); voltar = QPushButton("Voltar"); cancelar = QPushButton("Cancelar"); salvar = QPushButton("Salvar")
        bl.addWidget(voltar); bl.addWidget(cancelar); bl.addStretch(); bl.addWidget(salvar); layout.addLayout(bl)

        browse.clicked.connect(lambda: self._browse_save_path(path_edit))
        voltar.clicked.connect(lambda: (dlg.close(), parent_dialog.show()))
        cancelar.clicked.connect(dlg.close)
        salvar.clicked.connect(lambda: self._do_export_and_close(dlg, parent_dialog, path_edit.text()))
        dlg.exec()

    def _browse_save_path(self, path_edit: QLineEdit):
        last = load_last_txt_path()
        path, _ = QFileDialog.getSaveFileName(self, "Salvar Arquivo LCDPR", last, "Arquivo TXT (*.txt)")
        if path: path_edit.setText(path)

    def _do_export_and_close(self, dlg_export: QDialog, dlg_menu: QDialog, path: str):
        if not path.strip(): QMessageBox.warning(self, "Aviso", "Informe um caminho válido para salvar."); return
        try:
            self.gerar_txt(path); save_last_txt_path(path)
            QMessageBox.information(self, "Sucesso", "Arquivo LCDPR salvo com sucesso!")
        except Exception as e:
            QMessageBox.critical(self, "Erro ao salvar", str(e))
        finally:
            dlg_export.close(); dlg_menu.accept()

    def abrir_balancete(self):
        dlg = RelatorioPeriodoDialog("Balancete", self)
        if dlg.exec(): d1, d2 = dlg.periodo  # lógica de balancete

    def abrir_razao(self):
        dlg = RelatorioPeriodoDialog("Razão", self)
        if dlg.exec(): d1, d2 = dlg.periodo  # lógica de razão

    def mostrar_sobre(self):
        QMessageBox.information(self, "Sobre o Sistema",
            "Sistema AgroContábil - LCDPR\n\nVersão: 2.0\n© 2023 AgroTech Solutions\n\n"
            "Funcionalidades:\n- Gestão de propriedades rurais\n- Controle financeiro completo\n"
            "- Planejamento de safras\n- Gerenciamento de estoque\n- Geração do LCDPR")

    def importar_lancamentos(self):
        path, _ = QFileDialog.getOpenFileName(self, "Importar Lançamentos", "", "TXT (*.txt);;Excel (*.xlsx *.xls)")
        if not path: return
        try:
            self._import_lancamentos_txt(path) if path.lower().endswith('.txt') else self._import_lancamentos_excel(path)
            self.carregar_lancamentos(); self.dashboard.load_data()
        except Exception as e:
            QMessageBox.warning(self, "Importação Falhou", f"Arquivo não segue o layout esperado:\n{e}")

    def _import_lancamentos_txt(self, path):
        with open(path, encoding='utf-8') as f:
            for lineno, line in enumerate(f, 1):
                parts = line.strip().split("|")

                if len(parts) == 11 and re.match(r"\d{4}-\d{2}-\d{2}", parts[0]):
                    data_str, cod_imovel, cod_conta, num_doc, raw_tipo_doc, historico, id_participante, tipo_lanc, raw_ent, raw_sai, _ = parts
                    tipo_doc = int(raw_tipo_doc); ent = float(raw_ent.replace(",", ".")) if raw_ent else 0.0; sai = float(raw_sai.replace(",", ".")) if raw_sai else 0.0
                elif len(parts) == 12 and re.match(r"\d{2}-\d{2}-\d{4}", parts[0]):
                    data_br, cod_imovel, cod_conta, num_doc, _, historico, id_participante, tipo_lanc, _, raw_val, _, _ = parts
                    d, m, y = data_br.split("-"); data_str = f"{y}-{m}-{d}"; tipo_doc = 4; ent = 0.0; sai = float(raw_val)/100.0 if raw_val.isdigit() else 0.0
                else:
                    raise ValueError(f"Linha {lineno}: formato não reconhecido ({len(parts)} colunas)")

                desc = historico.upper()
                if "TALAO" in desc or any(k in desc for k in ("FOLHA", "IRPJ", "INSS", "FGTS")): tipo_doc = 4

                im = self.db.fetch_one("SELECT id FROM imovel_rural WHERE cod_imovel=?", (cod_imovel,))
                if not im: raise ValueError(f"Linha {lineno}: imóvel '{cod_imovel}' não encontrado")
                ct = self.db.fetch_one("SELECT id FROM conta_bancaria WHERE cod_conta=?", (cod_conta,))
                if not ct: raise ValueError(f"Linha {lineno}: conta '{cod_conta}' não encontrada")
                id_imovel, id_conta = im[0], ct[0]

                last = self.db.fetch_one("SELECT (saldo_final * CASE natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) FROM lancamento WHERE cod_conta=? ORDER BY id DESC LIMIT 1", (id_conta,))
                saldo_ant = last[0] if last and last[0] is not None else 0.0; saldo_f = saldo_ant + ent - sai; nat = 'P' if saldo_f >= 0 else 'N'

                self.db.execute_query("""INSERT INTO lancamento (data, cod_imovel, cod_conta, num_doc, tipo_doc, historico, id_participante, tipo_lanc, valor_entrada, valor_saida, saldo_final, natureza_saldo, usuario, categoria) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)""",
                    [data_str, id_imovel, id_conta, num_doc or None, tipo_doc, historico, int(id_participante), int(tipo_lanc), ent, sai, abs(saldo_f), nat, CURRENT_USER])

    def _import_lancamentos_excel(self, path):
        df = pd.read_excel(path, dtype=str); required = ['data','cod_imovel','cod_conta','num_doc','tipo_doc','historico','id_participante','tipo_lanc','valor_entrada','valor_saida','categoria']
        missing = [c for c in required if c not in df.columns]; df.fillna('', inplace=True)
        if missing: raise ValueError(f"Colunas faltando no Excel: {', '.join(missing)}")

        for lineno, row in enumerate(df.itertuples(index=False), start=2):
            im = self.db.fetch_one("SELECT id FROM imovel_rural WHERE cod_imovel=?", ((row.cod_imovel or '').zfill(3),))
            if not im: raise ValueError(f"Linha {lineno}: imóvel '{row.cod_imovel}' não encontrado")
            ct = self.db.fetch_one("SELECT id FROM conta_bancaria WHERE cod_conta=?", ((row.cod_conta  or '').zfill(3),))
            if not ct: raise ValueError(f"Linha {lineno}: conta '{row.cod_conta}' não encontrada")
            id_imovel, id_conta = im[0], ct[0]; ent, sai = float(row.valor_entrada), float(row.valor_saida)

            last = self.db.fetch_one("SELECT (saldo_final * CASE natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) FROM lancamento WHERE cod_conta=? ORDER BY id DESC LIMIT 1", (id_conta,))
            saldo_ant = last[0] if last and last[0] is not None else 0.0; saldo_f = saldo_ant + ent - sai; nat = 'P' if saldo_f >= 0 else 'N'

            self.db.execute_query("""INSERT INTO lancamento (data, cod_imovel, cod_conta, num_doc, tipo_doc, historico, id_participante, tipo_lanc, valor_entrada, valor_saida, saldo_final, natureza_saldo, usuario, categoria) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (row.data, id_imovel, id_conta, row.num_doc or None, int(row.tipo_doc), row.historico, int(row.id_participante), int(row.tipo_lanc), ent, sai, abs(saldo_f), nat, CURRENT_USER, row.categoria))

# ── (4) Ajuste no bloco principal para chamar o LoginDialog ───────
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    # antes de tudo, mostrar login
    login = LoginDialog()
    if not login.exec():
        sys.exit(0)
    # só então a janela principal
    window = MainWindow()
    window.show()
    sys.exit(app.exec())