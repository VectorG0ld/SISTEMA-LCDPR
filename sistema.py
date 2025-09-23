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
    QWidgetAction, QInputDialog, QProgressDialog, QSizePolicy, QCompleter,
    QStackedWidget, QTextBrowser, QSplitter
)

from PySide6.QtCore import Qt, QDate, QSize, QSettings, QCoreApplication, QTimer, QSignalBlocker, QObject, QEvent, QPoint
from PySide6.QtGui import QFont, QIcon, QColor, QPainter, QAction
from PySide6.QtCharts import QChart, QChartView, QPieSeries, QBarSeries, QBarSet, QBarCategoryAxis, QValueAxis
from PySide6.QtPrintSupport import QPrinter

from contextlib import contextmanager
import shiboken6

from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP
from decimal import Decimal


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

def _profile_root():
    return os.path.join(CACHE_FOLDER, CURRENT_PROFILE)

def _json_dir():
    p = os.path.join(_profile_root(), 'json')
    os.makedirs(p, exist_ok=True)
    return p

def cache_file_path():
    return os.path.join(_json_dir(), 'receita_cache.json')

def txt_pref_file_path():
    return os.path.join(_json_dir(), 'lcdpr_txt_path.json')

API_URL_CNPJ = 'https://www.receitaws.com.br/v1/cnpj/'
API_URL_CPF  = 'https://www.receitaws.com.br/v1/cpf/'

# —————— Configuração para salvar último caminho do TXT LCDPR ——————
TXT_PREF_FILE = os.path.join(CACHE_FOLDER, 'Cleuber Marcos', 'json', 'lcdpr_txt_path.json')

def load_last_txt_path():
    try:
        with open(txt_pref_file_path(), 'r', encoding='utf-8') as f:
            return json.load(f).get('last_path', '')
    except:
        return ''

def save_last_txt_path(path: str):
    with open(txt_pref_file_path(), 'w', encoding='utf-8') as f:
        json.dump({'last_path': path}, f, ensure_ascii=False, indent=2)

def ensure_cache_file():
    fp = cache_file_path()
    os.makedirs(os.path.dirname(fp), exist_ok=True)
    if not os.path.isfile(fp):
        with open(fp, 'w', encoding='utf-8') as f:
            json.dump({}, f, ensure_ascii=False, indent=2)

def load_cache() -> dict:
    ensure_cache_file()
    with open(cache_file_path(), 'r', encoding='utf-8') as f:
        return json.load(f)

def save_cache(cache: dict):
    with open(cache_file_path(), 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

# Substitua toda a função por esta versão (usa o mesmo cache existente)
RATE_LIMIT_RETRIES = 4          # nº de tentativas
RATE_LIMIT_BASE_DELAY = 2.0     # segundos (backoff exponencial: 2,4,8,16)
MIN_INTERVAL_BETWEEN_CALLS = 1.0  # garante espaçamento mínimo entre hits
_RECEITA_LAST_HIT_TS = 0.0

def consulta_receita(cpf_cnpj: str, tipo: str = 'cnpj') -> dict:
    """
    Faz consulta na API ou no cache. Chave = "<tipo>:<cpf_cnpj>"
    Tenta respeitar limite de requisições e faz backoff em 429/erros transitórios.
    """
    import time
    cache = load_cache()
    key = f"{tipo}:{cpf_cnpj}"
    if key in cache:
        return cache[key]

    global _RECEITA_LAST_HIT_TS
    url = (API_URL_CPF if tipo == 'cpf' else API_URL_CNPJ) + cpf_cnpj

    for attempt in range(RATE_LIMIT_RETRIES):
        # espaçamento mínimo entre chamadas
        elapsed = time.time() - _RECEITA_LAST_HIT_TS
        if elapsed < MIN_INTERVAL_BETWEEN_CALLS:
            time.sleep(MIN_INTERVAL_BETWEEN_CALLS - elapsed)

        try:
            res = requests.get(url, timeout=8)
            _RECEITA_LAST_HIT_TS = time.time()

            # 429 (Too Many Requests) → backoff e tenta de novo
            if res.status_code == 429:
                time.sleep(RATE_LIMIT_BASE_DELAY * (2 ** attempt))
                continue

            res.raise_for_status()
            data = res.json()

            # receitaws retorna {"status":"ERROR","message":"muitas consultas"...}
            if isinstance(data, dict):
                status = str(data.get('status', '')).upper()
                msg = str(data.get('message', '')).lower()
                if status == 'ERROR' and ('muita' in msg or 'many' in msg or 'limite' in msg):
                    time.sleep(RATE_LIMIT_BASE_DELAY * (2 ** attempt))
                    continue

            cache[key] = data
            save_cache(cache)
            return data

        except requests.RequestException:
            # erro transitório → backoff e tenta novamente
            time.sleep(RATE_LIMIT_BASE_DELAY * (2 ** attempt))
            continue

    # todas as tentativas falharam → sinaliza erro (sem estourar exceção)
    return {"status": "ERROR", "message": "RATE_LIMIT_OR_NETWORK"}

def _extract_name_from_historico(historico: str) -> str:
    """Pega o texto dentro do último parêntese do histórico (para CPF)."""
    import re
    if not historico:
        return ""
    m = re.findall(r"\(([^)]+)\)", historico)
    return (m[-1].strip() if m else "")

def _nome_cnpj_from_receita(data: dict) -> str:
    """Extrai o melhor nome de um retorno da Receita para CNPJ."""
    if not isinstance(data, dict):
        return ""
    for k in ("nome", "razao_social", "razaosocial", "razaoSocial", "fantasia", "nome_fantasia"):
        v = data.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

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

def profile_settings():
    return QSettings("Automatize Tech", f"AgroApp::{CURRENT_PROFILE}")

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
        if filename is None:
            filename = get_profile_db_filename()
        try:
            self.conn = sqlite3.connect(filename)
        except sqlite3.OperationalError as e:
            raise RuntimeError(f"Não foi possível abrir/criar o banco em '{filename}':\n  {e}")

        # Uma única passagem de criação/migração
        self._create_tables()
        self._create_views()
        self._migrate_schema()
        self._migrate_add_data_ord()
        self._create_indexes()

        # PRAGMAs de desempenho (apenas aqui)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA cache_size=-200000")  # ~200 MB (ajuste se quiser)

    def _migrate_schema(self):
        """
        Migrações estruturais:
          - Garante coluna 'usuario' em lancamento.
          - Garante que 'lancamento.id' seja AUTOINCREMENT (migração segura).
          - Sincroniza sqlite_sequence para não reutilizar IDs apagados.
        """
        cur = self.conn.cursor()

        # 1) Garante coluna 'usuario'
        try:
            cur.execute("SELECT usuario FROM lancamento LIMIT 1")
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE lancamento ADD COLUMN usuario TEXT NOT NULL DEFAULT ''")
            self.conn.commit()

        # 2) Checa se a tabela 'lancamento' já possui AUTOINCREMENT
        row = cur.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='lancamento'"
        ).fetchone()
        ddl = (row[0] or "").upper() if row else ""
        has_autoinc = "AUTOINCREMENT" in ddl

        if not has_autoinc:
            # Derruba views que dependem de 'lancamento' para evitar conflitos
            try:
                cur.executescript("""
                    DROP VIEW IF EXISTS saldo_contas;
                    DROP VIEW IF EXISTS resumo_categorias;
                """)
            except Exception:
                pass

            # Migração: renomeia, recria com AUTOINCREMENT, copia dados e apaga a antiga
            cur.execute("ALTER TABLE lancamento RENAME TO lancamento_old")

            # Cria a nova tabela com a DDL atual (com AUTOINCREMENT)
            cur.executescript("""
            CREATE TABLE lancamento (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data DATE NOT NULL,
                cod_imovel INTEGER NOT NULL,
                cod_conta INTEGER NOT NULL,
                num_doc TEXT,
                tipo_doc INTEGER NOT NULL,
                historico TEXT NOT NULL,
                id_participante INTEGER,
                tipo_lanc INTEGER NOT NULL,
                valor_entrada REAL DEFAULT 0,
                valor_saida REAL DEFAULT 0,
                saldo_final REAL NOT NULL,
                natureza_saldo TEXT NOT NULL,
                usuario TEXT NOT NULL,
                categoria TEXT,
                data_ord INTEGER,
                area_afetada INTEGER,
                quantidade REAL,
                unidade_medida TEXT,
                FOREIGN KEY(cod_imovel) REFERENCES imovel_rural(id),
                FOREIGN KEY(cod_conta) REFERENCES conta_bancaria(id),
                FOREIGN KEY(id_participante) REFERENCES participante(id),
                FOREIGN KEY(area_afetada) REFERENCES area_producao(id)
            );
            """)

            # Descobre colunas em comum e copia preservando os IDs existentes
            old_cols = [r[1] for r in cur.execute("PRAGMA table_info(lancamento_old)").fetchall()]
            new_cols = [r[1] for r in cur.execute("PRAGMA table_info(lancamento)").fetchall()]
            common   = [c for c in old_cols if c in new_cols]  # mantém ordem do antigo
            col_list = ", ".join(common)
            cur.execute(f"INSERT INTO lancamento ({col_list}) SELECT {col_list} FROM lancamento_old")

            # Remove tabela antiga
            cur.execute("DROP TABLE lancamento_old")

            # Recria índices e views
            self._create_indexes()
            self._create_views()

            self.conn.commit()

        # 3) Sincroniza sqlite_sequence com o MAX(id) atual (se existir/for aplicável)
        try:
            max_id = cur.execute("SELECT IFNULL(MAX(id), 0) FROM lancamento").fetchone()[0] or 0
            row = cur.execute("SELECT seq FROM sqlite_sequence WHERE name='lancamento'").fetchone()
            if row is None:
                cur.execute("INSERT INTO sqlite_sequence(name, seq) VALUES('lancamento', ?)", (max_id,))
            elif (row[0] or 0) < max_id:
                cur.execute("UPDATE sqlite_sequence SET seq=? WHERE name='lancamento'", (max_id,))
            self.conn.commit()
        except sqlite3.OperationalError:
            # sqlite_sequence só existe se houver ao menos uma tabela AUTOINCREMENT
            pass

    def get_perfil_param(self, profile: str):
        sql = """SELECT version, ind_ini_per, sit_especial, ident, nome,
                        logradouro, numero, complemento, bairro, uf,
                        cod_mun, cep, telefone, email
                 FROM perfil_param WHERE profile=?"""
        return self.fetch_one(sql, (profile,))

    def upsert_perfil_param(self, profile: str, p: dict):
        sql = """
        INSERT INTO perfil_param (
          profile, version, ind_ini_per, sit_especial, ident, nome,
          logradouro, numero, complemento, bairro, uf, cod_mun, cep,
          telefone, email, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(profile) DO UPDATE SET
          version=excluded.version,
          ind_ini_per=excluded.ind_ini_per,
          sit_especial=excluded.sit_especial,
          ident=excluded.ident,
          nome=excluded.nome,
          logradouro=excluded.logradouro,
          numero=excluded.numero,
          complemento=excluded.complemento,
          bairro=excluded.bairro,
          uf=excluded.uf,
          cod_mun=excluded.cod_mun,
          cep=excluded.cep,
          telefone=excluded.telefone,
          email=excluded.email,
          updated_at=CURRENT_TIMESTAMP
        """
        params = (
            profile, p["version"], p["ind_ini_per"], p["sit_especial"], p["ident"], p["nome"],
            p["logradouro"], p["numero"], p["complemento"], p["bairro"], p["uf"],
            p["cod_mun"], p["cep"], p["telefone"], p["email"]
        )
        self.execute_query(sql, params)

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
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data DATE NOT NULL,
            cod_imovel INTEGER NOT NULL,
            cod_conta INTEGER NOT NULL,
            num_doc TEXT,
            tipo_doc INTEGER NOT NULL,
            historico TEXT NOT NULL,
            id_participante INTEGER,
            tipo_lanc INTEGER NOT NULL,
            valor_entrada REAL DEFAULT 0,
            valor_saida REAL DEFAULT 0,
            saldo_final REAL NOT NULL,
            natureza_saldo TEXT NOT NULL,
            usuario TEXT NOT NULL,
            categoria TEXT,
            data_ord INTEGER,                 -- <<< NOVO CAMPO
            area_afetada INTEGER,
            quantidade REAL,
            unidade_medida TEXT,
            FOREIGN KEY(cod_imovel) REFERENCES imovel_rural(id),
            FOREIGN KEY(cod_conta) REFERENCES conta_bancaria(id),
            FOREIGN KEY(id_participante) REFERENCES participante(id),
            FOREIGN KEY(area_afetada) REFERENCES area_producao(id)
        );
        CREATE TABLE IF NOT EXISTS perfil_param (
            profile TEXT PRIMARY KEY,
            version TEXT,
            ind_ini_per TEXT,
            sit_especial TEXT,
            ident TEXT,
            nome TEXT,
            logradouro TEXT,
            numero TEXT,
            complemento TEXT,
            bairro TEXT,
            uf TEXT,
            cod_mun TEXT,
            cep TEXT,
            telefone TEXT,
            email TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
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
    
    
    def execute_query(self, sql: str, params: list = None, autocommit: bool = True):
        cur = self.conn.cursor()
        cur.execute(sql, params or [])
        if autocommit:
            self.conn.commit()
        return cur

    @contextmanager
    def bulk(self):
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def _migrate_add_data_ord(self):
        cur = self.conn.cursor()
        # Descobre as colunas atuais
        cols = [r[1] for r in cur.execute("PRAGMA table_info(lancamento)").fetchall()]
        # 1) Se a coluna não existir, adiciona
        if "data_ord" not in cols:
            cur.execute("ALTER TABLE lancamento ADD COLUMN data_ord INTEGER")
            self.conn.commit()
            # 2) Preenche para registros antigos (data DD/MM/AAAA ou YYYY-MM-DD)
            # DD/MM/AAAA -> AAAAMMDD
            cur.execute("""
                UPDATE lancamento
                   SET data_ord = CAST(substr(data, 7, 4) || substr(data, 4, 2) || substr(data, 1, 2) AS INTEGER)
                 WHERE instr(data, '/') > 0 AND length(data) = 10
            """)
            # YYYY-MM-DD -> AAAAMMDD
            cur.execute("""
                UPDATE lancamento
                   SET data_ord = CAST(replace(data, '-', '') AS INTEGER)
                 WHERE instr(data, '-') > 0 AND length(data) = 10
            """)
            self.conn.commit()

    def fetch_one(self, sql: str, params: list = None):
        return self.execute_query(sql, params).fetchone()

    def fetch_all(self, sql: str, params: list = None):
        return self.execute_query(sql, params).fetchall()

    def close(self):
        self.conn.close()

    def _create_indexes(self):
        self.conn.cursor().executescript("""
        CREATE INDEX IF NOT EXISTS idx_part_cpf        ON participante(cpf_cnpj);
        CREATE INDEX IF NOT EXISTS idx_part_nome       ON participante(nome);
        CREATE INDEX IF NOT EXISTS idx_cta_cod         ON conta_bancaria(cod_conta);
        CREATE INDEX IF NOT EXISTS idx_cta_nome        ON conta_bancaria(nome_banco);
        CREATE INDEX IF NOT EXISTS idx_imov_cod        ON imovel_rural(cod_imovel);
        CREATE INDEX IF NOT EXISTS idx_imov_nome       ON imovel_rural(nome_imovel);
        CREATE INDEX IF NOT EXISTS idx_lanc_data_ord   ON lancamento(data_ord);   -- **chave**
        CREATE INDEX IF NOT EXISTS idx_lanc_cta        ON lancamento(cod_conta);
        CREATE INDEX IF NOT EXISTS idx_lanc_part       ON lancamento(id_participante);
        CREATE INDEX IF NOT EXISTS idx_lanc_docp       ON lancamento(num_doc, id_participante);
        CREATE INDEX IF NOT EXISTS idx_lanc_data_ord_valent ON lancamento(data_ord, valor_entrada);
        CREATE INDEX IF NOT EXISTS idx_lanc_data_ord_valsai ON lancamento(data_ord, valor_saida);
        """)
        self.conn.commit()

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
def fmt_money(v) -> str:
    try:
        d = Decimal(v).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except Exception:
        d = Decimal("0.00")
    s = f"{d:,.2f}"
    return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")

def qdate_to_ord(qd: QDate) -> int:
    return int(qd.toString("yyyyMMdd"))

# ==== PROGRESSO GLOBAL (UMA ÚNICA INSTÂNCIA) ==================================
class GlobalProgress:
    """
    Tela de progresso global. Use:
        GlobalProgress.begin("Importando...", maximo, parent=self.window())
        ... (loop) GlobalProgress.set_value(i)  ou GlobalProgress.step()
        GlobalProgress.end()
    Se não souber o total ainda, chame begin(maximo=0) que vira 'busy'.
    """
    _dlg = None

    @classmethod
    def _ensure(cls, parent=None):
        if cls._dlg is None:
            cls._dlg = QProgressDialog("", "Cancelar", 0, 0, parent)
            cls._dlg.setWindowTitle("Processando…")
            cls._dlg.setAutoClose(False)
            cls._dlg.setAutoReset(False)
            cls._dlg.setWindowModality(Qt.ApplicationModal)

    @classmethod
    def begin(cls, texto: str, maximo: int = 0, parent=None):
        cls._ensure(parent or QApplication.activeWindow())
        dlg = cls._dlg
        dlg.setLabelText(texto)
        dlg.setRange(0, maximo if maximo and maximo > 0 else 0)  # 0..0 == busy
        dlg.setValue(0)
        dlg.show()
        QCoreApplication.processEvents()

    @classmethod
    def set_max(cls, maximo: int):
        cls._ensure()
        cls._dlg.setRange(0, maximo if maximo and maximo > 0 else 0)
        QCoreApplication.processEvents()

    @classmethod
    def set_value(cls, valor: int):
        if cls._dlg:
            cls._dlg.setValue(valor)
            QCoreApplication.processEvents()

    @classmethod
    def step(cls, inc: int = 1):
        if not cls._dlg:
            return
        if cls._dlg.maximum() == 0:
            return  # está em busy; nada a fazer
        cls._dlg.setValue(cls._dlg.value() + (inc or 1))
        QCoreApplication.processEvents()

    @classmethod
    def end(cls):
        if cls._dlg:
            cls._dlg.reset()
            cls._dlg.hide()
            QCoreApplication.processEvents()

# ==== ACELERADOR UNIVERSAL DE LISTAS (OTIMIZADO) ==============================
class ListAccelerator:
    """
    Filtro universal com cache por linha (armazenado em Qt.UserRole+1 do item da coluna 0),
    debounce (150ms) e aplicação sem repaints por linha.
    Use:
        ListAccelerator.install(self)  # 1x no root (opcional, mantido por compatibilidade)
        ListAccelerator.filter(tabela, texto)
    """
    _timers = {}       # tabela -> QTimer (debounce)
    _last_txt = {}     # tabela -> último texto aplicado

    @staticmethod
    def install(root: QWidget):
        # Mantido por compatibilidade; não conecta sinais pesados.
        # Se quiser, poderia varrer as tabelas aqui; não é necessário.
        return

    @staticmethod
    def _ensure_row_cache(table: QTableWidget, row: int):
        """Garante que a linha 'row' tenha cache de busca em UserRole+1 do item(0)."""
        it0 = table.item(row, 0)
        if it0 is None:
            it0 = QTableWidgetItem("")
            table.setItem(row, 0, it0)
        cache = it0.data(Qt.UserRole + 1)
        if cache is None:
            cols = table.columnCount()
            parts = []
            for c in range(cols):
                it = table.item(row, c)
                if it:
                    parts.append(it.text())
            it0.setData(Qt.UserRole + 1, " | ".join(parts).casefold())
        return it0.data(Qt.UserRole + 1)

    @staticmethod
    def _apply_filter(table: QTableWidget, text: str):
        needle = (text or "").casefold()

        # pausa ordenação e repaints
        sort_enabled = table.isSortingEnabled()
        if sort_enabled:
            table.setSortingEnabled(False)

        table.setUpdatesEnabled(False)
        try:
            if not needle:
                # mostrar tudo rápido
                for r in range(table.rowCount()):
                    if table.isRowHidden(r):
                        table.setRowHidden(r, False)
                return

            for r in range(table.rowCount()):
                cache = ListAccelerator._ensure_row_cache(table, r)
                hide = needle not in cache
                if table.isRowHidden(r) != hide:
                    table.setRowHidden(r, hide)
        finally:
            table.setUpdatesEnabled(True)
            if sort_enabled:
                table.setSortingEnabled(True)
            QCoreApplication.processEvents()

    @staticmethod
    def filter(table: QTableWidget, text: str, delay_ms: int = 150):
        """
        Aplica filtro com debounce. Chamar livremente em textChanged.
        """
        ListAccelerator._last_txt[table] = text
        t = ListAccelerator._timers.get(table)
        if t is None:
            t = QTimer(table)
            t.setSingleShot(True)
            ListAccelerator._timers[table] = t
            def _run():
                txt = ListAccelerator._last_txt.get(table, "")
                ListAccelerator._apply_filter(table, txt)
                # >>> ATUALIZA O BADGE APÓS O FILTRO <<<
                try:
                    ListCounter.refresh(table)
                except Exception:
                    pass
            t.timeout.connect(_run)
        t.start(max(0, delay_ms))

    @staticmethod
    def build_cache(table: QTableWidget):
        table.setUpdatesEnabled(False)
        try:
            for r in range(table.rowCount()):
                ListAccelerator._ensure_row_cache(table, r)
        finally:
            table.setUpdatesEnabled(True)

# ==== CONTADOR GLOBAL DE ITENS VISÍVEIS (para todas as QTableWidget) =========
class _BadgeHelper(QObject):
    def __init__(self, table: QTableWidget, label: QLabel):
        super().__init__(table)
        self.table = table
        self.label = label
        table.installEventFilter(self)
        self._reposition()

    def _reposition(self):
        try:
            p = self.table.mapToParent(QPoint(0, 0))
            x = p.x() + self.table.width() - self.label.width() - 8
            # tenta posicionar ACIMA da tabela (fora da lista)
            y_acima = p.y() - self.label.height() - 6
            # fallback: se não houver espaço, posiciona DENTRO no alto
            y_dentro = p.y() + 6
            y = y_acima if y_acima > 0 else y_dentro
            self.label.move(x, y)
            self.label.raise_()
            self.label.show()
        except Exception:
            pass

    def eventFilter(self, obj, ev):
        if ev.type() in (QEvent.Show, QEvent.Resize, QEvent.Move):
            self._reposition()
        return False

class ListCounter:
    """Badge pequeno no cantinho com 'visíveis/total' para cada QTableWidget."""
    _helpers = {}  # table -> _BadgeHelper

    @staticmethod
    def attach(table: QTableWidget):
        if table in ListCounter._helpers:
            return
        parent = table.parentWidget() or table
        lbl = QLabel(parent)
        lbl.setObjectName("listCounterBadge")
        lbl.setStyleSheet("""
            QLabel#listCounterBadge{
                background:#0d1b3d; color:#ffffff;
                border:1px solid #11398a; border-radius:10px;
                padding:1px 6px; font-size:11px;
            }
        """)
        lbl.setAttribute(Qt.WA_TransparentForMouseEvents)
        helper = _BadgeHelper(table, lbl)
        ListCounter._helpers[table] = helper

        m = table.model()
        # Atualiza em qualquer mudança estrutural/visual (com checagem no refresh)
        m.rowsInserted.connect(lambda *_: ListCounter.refresh(table))
        m.rowsRemoved.connect(lambda *_: ListCounter.refresh(table))
        m.modelReset.connect(lambda *_: ListCounter.refresh(table))
        m.layoutChanged.connect(lambda *_: ListCounter.refresh(table))
        m.dataChanged.connect(lambda *_: ListCounter.refresh(table))

        # limpa quando a tabela for destruída
        table.destroyed.connect(lambda *_: ListCounter._on_table_destroyed(table))

        ListCounter.refresh(table)

    @staticmethod
    def refresh(table: QTableWidget):
        # se a tabela já foi destruída, sai e limpa
        if table is None or not shiboken6.isValid(table):
            ListCounter.detach(table)
            return

        helper = ListCounter._helpers.get(table)
        if not helper:
            return

        lbl = getattr(helper, "label", None)
        if lbl is None or not shiboken6.isValid(lbl):
            return

        # pode acontecer de ainda receber sinal com C++ destruído → proteja o acesso
        try:
            total = table.rowCount()
        except RuntimeError:
            ListCounter.detach(table)
            return

        visiveis = 0
        for i in range(total):
            try:
                if not table.isRowHidden(i):
                    visiveis += 1
            except RuntimeError:
                ListCounter.detach(table)
                return

        texto = f"{visiveis}/{total}" if total else "0/0"
        lbl.setText(texto)
        lbl.adjustSize()
        try:
            helper._reposition()
        except Exception:
            pass

    @staticmethod
    def detach(table: QTableWidget):
        """Remove o helper e destrói o label, se existirem."""
        helper = ListCounter._helpers.pop(table, None)
        if helper:
            try:
                if getattr(helper, "label", None) and shiboken6.isValid(helper.label):
                    helper.label.deleteLater()
            except Exception:
                pass

    @staticmethod
    def _on_table_destroyed(table: QTableWidget):
        """Slot chamado quando a QTableWidget é destruída."""
        ListCounter.detach(table)

def install_counters_for_all_tables(root: QWidget):
    """Chame 1x após montar a UI: instala badge em todas as QTableWidget da tela."""
    for tbl in root.findChildren(QTableWidget):
        if tbl.property("counter_in_layout"):  # <<< NÃO criar badge flutuante para essas
            continue
        ListCounter.attach(tbl)

def attach_counter_in_layout(table: QTableWidget, layout: QHBoxLayout):
    """Acopla o contador no layout (direita), sem badge flutuante."""
    table.setProperty("counter_in_layout", True)

    lbl = QLabel()
    lbl.setObjectName("listCounterBadge")
    lbl.setStyleSheet("""
        QLabel#listCounterBadge{
            background:#0d1b3d; color:#ffffff;
            border:1px solid #11398a; border-radius:10px;
            padding:1px 6px; font-size:11px;
        }
    """)
    lbl.setAttribute(Qt.WA_TransparentForMouseEvents)
    lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)

    # empurra para a direita e adiciona o label no fim da barra de filtros
    layout.addStretch()
    layout.addWidget(lbl)

    # registra no mesmo dicionário do ListCounter (sem reposicionamento)
    class _H: pass
    h = _H(); h.table = table; h.label = lbl; h._reposition = lambda: None
    ListCounter._helpers[table] = h

    m = table.model()
    m.rowsInserted.connect(lambda *_: ListCounter.refresh(table))
    m.rowsRemoved.connect(lambda *_: ListCounter.refresh(table))
    m.modelReset.connect(lambda *_: ListCounter.refresh(table))
    m.layoutChanged.connect(lambda *_: ListCounter.refresh(table))
    m.dataChanged.connect(lambda *_: ListCounter.refresh(table))

    ListCounter.refresh(table)
    table.destroyed.connect(lambda *_: ListCounter._on_table_destroyed(table))
# =================================================================================

class NumericItem(QTableWidgetItem):
    def __init__(self, value, text=None): super().__init__(text or str(value)); self._value = value
    def __lt__(self, other): return self._value < other._value if isinstance(other, NumericItem) else super().__lt__(other)

class DateItem(QTableWidgetItem):
    """Item que ordena por data (aceita dd/MM/yyyy ou yyyy-MM-dd)."""
    def __init__(self, value: str):
        val = str(value or "")
        if "/" in val:  # dd/MM/yyyy
            d, m, y = val.split("/")
            self._key = (int(y), int(m), int(d))
            text = f"{int(d):02d}/{int(m):02d}/{y}"
        elif "-" in val:  # yyyy-MM-dd
            y, m, d = val.split("-")
            self._key = (int(y), int(m), int(d))
            text = f"{int(d):02d}/{int(m):02d}/{y}"
        else:
            self._key = (0, 0, 0)
            text = val
        super().__init__(text)

    def __lt__(self, other):
        if isinstance(other, DateItem):
            return self._key < other._key
        return super().__lt__(other)

# ===== Ordenação global por duplo clique no cabeçalho (OTIMIZADA) ============
def _install_header_double_click_sort(table: QTableWidget):
    if getattr(table, "_sort_installed", False):
        return
    hdr = table.horizontalHeader()
    hdr.setSortIndicatorShown(True)
    table._sort_installed = True
    table._sort_state = {}         # col -> ordem
    table._wrapped_cols = set()    # colunas já convertidas p/ NumericItem/DateItem

    def _wrap_column_once(col: int):
        """Detecta tipo e converte a coluna só uma vez para itens otimizados."""
        if col in table._wrapped_cols:
            return
        # amostra
        sample = None
        for r in range(table.rowCount()):
            it = table.item(r, col)
            if it and it.text().strip():
                sample = it.text().strip()
                break
        if not sample:
            table._wrapped_cols.add(col)
            return

        import re
        is_date = bool(re.match(r"^\d{2}/\d{2}/\d{4}$", sample) or re.match(r"^\d{4}-\d{2}-\d{2}$", sample))
        is_money_or_num = bool(re.match(r"^\s*(R\$\s*)?[-\d\.\,]+\s*$", sample) or
                               re.match(r"^\s*-?\d+(?:[.,]\d+)?\s*$", sample))
        if not (is_date or is_money_or_num):
            table._wrapped_cols.add(col)
            return

        model = table.model()
        blocker = QSignalBlocker(model)  # evita 'dataChanged' em massa
        table.setUpdatesEnabled(False)
        try:
            for r in range(table.rowCount()):
                old = table.item(r, col)
                if not old:
                    continue
                txt = old.text()
                role = old.data(Qt.UserRole)      # preserva payload
                role1 = old.data(Qt.UserRole + 1) # preserva cache (acelerador)
                align = old.textAlignment()
                if is_date:
                    new = DateItem(txt)
                else:
                    raw = re.sub(r"[^\d,\.\-]", "", txt)
                    val = float(raw.replace(".", "").replace(",", ".")) if raw else 0.0
                    new = NumericItem(val, text=txt)
                new.setTextAlignment(align)
                if role is not None:
                    new.setData(Qt.UserRole, role)
                if role1 is not None:
                    new.setData(Qt.UserRole + 1, role1)
                table.setItem(r, col, new)
        finally:
            table.setUpdatesEnabled(True)
        table._wrapped_cols.add(col)

    def _on_header_double_clicked(col: int):
        _wrap_column_once(col)
        order = table._sort_state.get(col, Qt.DescendingOrder)
        order = Qt.AscendingOrder if order == Qt.DescendingOrder else Qt.DescendingOrder
        table._sort_state = {col: order}
        table.sortItems(col, order)
        hdr.setSortIndicator(col, order)

    hdr.sectionDoubleClicked.connect(_on_header_double_clicked)

def install_sorting_for_all_tables(root: QWidget):
    for tbl in root.findChildren(QTableWidget):
        _install_header_double_click_sort(tbl)
# ==============================================================================
        
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
        self._ajustar_mask()  # inicializa a máscara

        self.nome = QLineEdit(); form_layout.addRow("Nome:", self.nome)

        btns = QHBoxLayout(); btns.addStretch()
        salvar = QPushButton("Salvar"); salvar.setObjectName("success"); salvar.clicked.connect(self.salvar); btns.addWidget(salvar)
        cancelar = QPushButton("Cancelar"); cancelar.setObjectName("danger"); cancelar.clicked.connect(self.reject); btns.addWidget(cancelar)
        layout.addLayout(btns)

        if participante_id:
            row = self.db.fetch_one("SELECT cpf_cnpj, nome, tipo_contraparte FROM participante WHERE id=?", (participante_id,))
            if row: self.tipo.setCurrentIndex(row[2] - 1); self.cpf_cnpj.setText(row[0]); self.nome.setText(row[1])

    def _ajustar_mask(self, *_):
        # 0 = Pessoa Jurídica (CNPJ), 1 = Pessoa Física (CPF), demais = livre
        idx = self.tipo.currentIndex()
        if idx == 1:
            self.cpf_cnpj.setInputMask("000.000.000-00;_")
            self.cpf_cnpj.setPlaceholderText("CPF")
        elif idx == 0:
            self.cpf_cnpj.setInputMask("00.000.000/0000-00;_")
            self.cpf_cnpj.setPlaceholderText("CNPJ")
        else:
            self.cpf_cnpj.setInputMask("")
            self.cpf_cnpj.setPlaceholderText("Documento")
    
    def _on_cpf_cnpj(self):
        import re
        raw = self.cpf_cnpj.text().strip()
        digits = re.sub(r'\D', '', raw)
        idx = self.tipo.currentIndex()  # 0=Pessoa Jurídica (CNPJ), 1=Pessoa Física (CPF)

        # Pessoa Física (CPF)
        if idx == 1:
            if not valida_cpf(raw):
                QMessageBox.warning(self, "CPF inválido", "O CPF digitado não é válido.")
                self.nome.clear()
                return
            # Tenta Receita para preencher nome (se disponível)
            try:
                info = consulta_receita(digits, tipo='cpf')
                nome_api = (info.get('nome') or "").strip()
                if nome_api:
                    self.nome.setText(nome_api)
            except Exception:
                pass

        # Pessoa Jurídica (CNPJ)
        elif idx == 0:
            if len(digits) != 14:
                return
            try:
                info = consulta_receita(digits, tipo='cnpj')
                nome_api = _nome_cnpj_from_receita(info)
                if nome_api:
                    self.nome.setText(nome_api)
            except Exception:
                pass

    def salvar(self):
        import re, requests
        raw = self.cpf_cnpj.text().strip()
        digits = re.sub(r'\D', '', raw)
        idx = self.tipo.currentIndex()  # 0=Pessoa Jurídica (CNPJ), 1=Pessoa Física (CPF)

        # Validações corretas por tipo
        if idx == 1:
            if not valida_cpf(raw):
                QMessageBox.warning(self, "Inválido", "CPF inválido.")
                return
        elif idx == 0:
            if len(digits) != 14:
                QMessageBox.warning(self, "Inválido", "CNPJ deve ter 14 dígitos.")
                return
            try:
                info = consulta_receita(digits, tipo='cnpj')
            except requests.HTTPError:
                QMessageBox.warning(self, "Inválido", "Não foi possível consultar o CNPJ na Receita Federal.")
                return
            # Aceita se vier nome/fantasia, mesmo que não tenha 'status'
            if not _nome_cnpj_from_receita(info):
                QMessageBox.warning(self, "Não Encontrado", "CNPJ não localizado na Receita Federal.")
                return

        nome = self.nome.text().strip()
        if not nome:
            QMessageBox.warning(self, "Inválido", "Nome não pode ficar vazio.")
            return

        exists = self.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj = ?", (digits,))
        if exists and not self.participante_id:
            QMessageBox.information(self, "Já existe", f"Participante já cadastrado (ID {exists[0]}).")
            return

        data = (digits, nome, idx + 1)  # 1=Juridica, 2=Fisica
        try:
            if self.participante_id:
                self.db.execute_query(
                    "UPDATE participante SET cpf_cnpj = ?, nome = ?, tipo_contraparte = ? WHERE id = ?",
                    data + (self.participante_id,))
            else:
                self.db.execute_query(
                    "INSERT INTO participante (cpf_cnpj, nome, tipo_contraparte) VALUES (?, ?, ?)", data)
            QMessageBox.information(self, "Sucesso", "Participante salvo com sucesso!")
            # Ao salvar manualmente, também atualiza combos abertos:
            if hasattr(self.parent(), "_broadcast_participantes_changed"):
                try:
                    self.parent()._broadcast_participantes_changed()
                except Exception:
                    pass
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Erro ao Salvar", f"Não foi possível salvar participante:\n{e}")

class ParametrosDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Parâmetros do Contribuinte")
        self.setMinimumSize(400, 500)
        self.settings = profile_settings()
        self.db = Database()
        row = self.db.get_perfil_param(CURRENT_PROFILE)
        def _val(k, default=""):
            if not row:  # sem registro no SQL ainda → usa QSettings como fallback
                return self.settings.value(f"param/{k}", default)
            # mapeamento na mesma ordem do SELECT em get_perfil_param
            keys = ["version","ind_ini_per","sit_especial","ident","nome",
                    "logradouro","numero","complemento","bairro","uf",
                    "cod_mun","cep","telefone","email"]
            d = dict(zip(keys, row))
            return d.get(k, default)

        # inicialização dos campos:
        self.version     = QLineEdit(_val("version", "0013"))
        self.ind_ini_per = QComboBox(); self.ind_ini_per.addItems(["0 - Regular (início em 01/01)", "1 - Início fora de 01/01"])
        self.ind_ini_per.setCurrentIndex(0 if _val("ind_ini_per","0")=="0" else 1)

        self.sit_especial = QComboBox()
        self.sit_especial.addItems(["0 - Normal (sem ocorrência)","1 - Falecimento","2 - Espólio","3 - Saída definitiva do país"])
        se = _val("sit_especial","0"); self.sit_especial.setCurrentIndex(int(se) if se in {"0","1","2","3"} else 0)

        self.ident = QLineEdit(_val("ident","")); self.ident.setInputMask("000.000.000-00;_")
        self.nome  = QLineEdit(_val("nome",""))
        self.logradouro  = QLineEdit(_val("logradouro",""))
        self.numero      = QLineEdit(_val("numero",""))
        self.complemento = QLineEdit(_val("complemento",""))
        self.bairro      = QLineEdit(_val("bairro",""))
        self.uf          = QLineEdit(_val("uf",""))
        self.cod_mun     = QLineEdit(_val("cod_mun",""))
        self.cep         = QLineEdit(_val("cep",""))
        self.telefone    = QLineEdit(_val("telefone",""))
        self.email       = QLineEdit(_val("email",""))

        layout = QFormLayout(self)

        # Versão do leiaute
        self.version = QLineEdit(self.settings.value("param/version", "0013"))
        layout.addRow("Versão do Leiaute:", self.version)

        # Indicador de início do período (0000 campo 5)
        self.ind_ini_per = QComboBox()
        self.ind_ini_per.addItems(["0 - Regular (início em 01/01)", "1 - Início fora de 01/01"])
        iip = self.settings.value("param/ind_ini_per", "0")
        self.ind_ini_per.setCurrentText("0 - Regular (início em 01/01)" if iip=="0" else "1 - Início fora de 01/01")
        layout.addRow("Ind. início do período:", self.ind_ini_per)

        # Situação especial (0000 campo 6)
        self.sit_especial = QComboBox()
        self.sit_especial.addItems([
            "0 - Normal (sem ocorrência)",
            "1 - Falecimento",
            "2 - Espólio",
            "3 - Saída definitiva do país"
        ])
        se = self.settings.value("param/sit_especial", "0")
        idx = int(se) if se in {"0","1","2","3"} else 0
        self.sit_especial.setCurrentIndex(idx)
        layout.addRow("Situação especial:", self.sit_especial)

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
        p = {
            "version":     self.version.text().strip(),
            "ind_ini_per": self.ind_ini_per.currentText().split(" - ")[0],
            "sit_especial": self.sit_especial.currentText().split(" - ")[0],
            "ident":       self.ident.text().strip(),
            "nome":        self.nome.text().strip(),
            "logradouro":  self.logradouro.text().strip(),
            "numero":      self.numero.text().strip(),
            "complemento": self.complemento.text().strip(),
            "bairro":      self.bairro.text().strip(),
            "uf":          self.uf.text().strip(),
            "cod_mun":     self.cod_mun.text().strip(),
            "cep":         self.cep.text().strip(),
            "telefone":    self.telefone.text().strip(),
            "email":       self.email.text().strip(),
        }

        # 1) SQL (fonte de verdade por perfil)
        self.db.upsert_perfil_param(CURRENT_PROFILE, p)

        # 2) Espelho no QSettings (compatibilidade com partes da UI já prontas)
        s = self.settings
        for k, v in p.items():
            s.setValue(f"param/{k}", v)
        s.sync()

        QMessageBox.information(self, "Sucesso", "Parâmetros salvos no banco de dados.")
        self.accept()

# --- DIALOG DE RELATÓRIO POR PERÍODO ---
class RelatorioPeriodoDialog(QDialog):
    def __init__(self, titulo, parent=None, ini: QDate | None = None, fim: QDate | None = None):
        super().__init__(parent)
        self.setWindowTitle(titulo)
        self.setMinimumSize(360, 160)

        # defaults coerentes com os dados existentes
        if not ini or not fim:
            db = getattr(parent, "db", Database())
            row = db.fetch_one("SELECT MIN(data_ord), MAX(data_ord) FROM lancamento WHERE data_ord IS NOT NULL")
            if row and row[0] and row[1]:
                ini = QDate.fromString(str(row[0]), "yyyyMMdd")
                fim = QDate.fromString(str(row[1]), "yyyyMMdd")
            else:
                ini = QDate.currentDate().addMonths(-1)
                fim = QDate.currentDate()

        layout = QFormLayout(self)

        self.dt_ini = QDateEdit(ini); self.dt_ini.setCalendarPopup(True); self.dt_ini.setDisplayFormat("dd/MM/yyyy")
        self.dt_fim = QDateEdit(fim); self.dt_fim.setCalendarPopup(True); self.dt_fim.setDisplayFormat("dd/MM/yyyy")

        layout.addRow("Data inicial:", self.dt_ini)
        layout.addRow("Data final:",   self.dt_fim)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept); btns.rejected.connect(self.reject)
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
        self.settings = profile_settings()

        self.layout = QVBoxLayout(self)
        self._build_filter_ui()
        self._build_cards_ui()
        self._build_piechart_ui()
        self.load_data()

    def _build_filter_ui(self):
        hl = QHBoxLayout()
        hl.addWidget(QLabel("De:"))

        # pega menor/maior data existentes no banco
        row = self.db.fetch_one("SELECT MIN(data_ord), MAX(data_ord) FROM lancamento WHERE data_ord IS NOT NULL")
        if row and row[0] and row[1]:
            _ini = QDate.fromString(str(row[0]), "yyyyMMdd")
            _fim = QDate.fromString(str(row[1]), "yyyyMMdd")
        else:
            _ini = QDate.currentDate().addMonths(-1)
            _fim = QDate.currentDate()

        self.dt_dash_ini = QDateEdit(_ini); self.dt_dash_ini.setCalendarPopup(True); self.dt_dash_ini.setDisplayFormat("dd/MM/yyyy"); hl.addWidget(self.dt_dash_ini)
        hl.addWidget(QLabel("Até:"))
        self.dt_dash_fim = QDateEdit(_fim); self.dt_dash_fim.setCalendarPopup(True); self.dt_dash_fim.setDisplayFormat("dd/MM/yyyy"); hl.addWidget(self.dt_dash_fim)

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
        d1_ord = int(self.dt_dash_ini.date().toString("yyyyMMdd"))
        d2_ord = int(self.dt_dash_fim.date().toString("yyyyMMdd"))

        # Saldo total
        saldo = self.db.fetch_one("SELECT SUM(saldo_atual) FROM saldo_contas")[0] or 0
        s = f"{saldo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        self.saldo_card.findChild(QLabel, "value").setText(f"R$ {s}")

        # Receitas e Despesas no intervalo (indexado e cobrindo)
        rec = self.db.fetch_one(
            "SELECT SUM(valor_entrada) FROM lancamento WHERE data_ord BETWEEN ? AND ?",
            (d1_ord, d2_ord)
        )[0] or 0
        desp = self.db.fetch_one(
            "SELECT SUM(valor_saida)   FROM lancamento WHERE data_ord BETWEEN ? AND ?",
            (d1_ord, d2_ord)
        )[0] or 0

        r = f"{rec:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        self.receita_card.findChild(QLabel, "value").setText(f"R$ {r}")
        d = f"{desp:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        self.despesa_card.findChild(QLabel, "value").setText(f"R$ {d}")

        # Gráfico de pizza com %
        self.series.clear()
        self.series.append("Receitas", rec)
        self.series.append("Despesas", desp)
        for sl in self.series.slices():
            pct = sl.percentage() * 100
            sl.setLabelVisible(True)
            sl.setLabel(f"{sl.label()} ({pct:.1f}%)")

class ReportCenterDialog(QDialog):
    """
    Central de Relatórios – com filtros, opções e pré-visualização.
    Relatórios:
      1) Receitas x Despesas (Geral) [barras/mês]
      2) Comparativo entre Fazendas [barras/fazenda]
      3) DRE Simplificada mês a mês [tabela]
      4) Comparativo Anual [barras/ano]
      5) DRE por Fazenda (na mesma página) [tabela multi-seção]
    """
    def __init__(self, parent=None, d_ini=None, d_fim=None):
        super().__init__(parent)
        self.setWindowTitle("Central de Relatórios")
        self.resize(1100, 700)
        self.db = getattr(parent, "db", Database())

        # ====== LADO ESQUERDO: filtros/opções ======
        left = QWidget(); lf = QVBoxLayout(left)

        self.cmb_tipo = QComboBox()
        self.cmb_tipo.addItems([
            "Receitas x Despesas (Geral)",
            "Comparativo entre Fazendas",
            "DRE Simplificada (mês a mês)",
            "Comparativo Anual",
            "DRE por Fazenda (multi-seção)"
        ])

        lf.addWidget(QLabel("Tipo de relatório:"))
        lf.addWidget(self.cmb_tipo)

        row = self.db.fetch_one("SELECT MIN(data_ord), MAX(data_ord) FROM lancamento WHERE data_ord IS NOT NULL")
        if row and row[0] and row[1]:
            _ini = QDate.fromString(str(row[0]), "yyyyMMdd")
            _fim = QDate.fromString(str(row[1]), "yyyyMMdd")
        else:
            _ini = QDate.currentDate().addMonths(-1)
            _fim = QDate.currentDate()
        if isinstance(d_ini, QDate): _ini = d_ini
        if isinstance(d_fim, QDate): _fim = d_fim

        hl = QHBoxLayout()
        self.dt_ini = QDateEdit(_ini); self.dt_ini.setCalendarPopup(True); self.dt_ini.setDisplayFormat("dd/MM/yyyy")
        self.dt_fim = QDateEdit(_fim); self.dt_fim.setCalendarPopup(True); self.dt_fim.setDisplayFormat("dd/MM/yyyy")
        hl.addWidget(QLabel("De:"));  hl.addWidget(self.dt_ini)
        hl.addWidget(QLabel("Até:")); hl.addWidget(self.dt_fim)
        lf.addLayout(hl)

        self.chk_por_fazenda = QCheckBox("Separar por fazenda (quando aplicável)")
        lf.addWidget(self.chk_por_fazenda)

        btns = QHBoxLayout()
        self.btn_preview = QPushButton("Pré-visualizar")
        self.btn_export  = QPushButton("Salvar PDF/PNG")
        self.btn_fechar  = QPushButton("Fechar")
        btns.addWidget(self.btn_preview); btns.addStretch(); btns.addWidget(self.btn_export); btns.addWidget(self.btn_fechar)
        lf.addStretch(); lf.addLayout(btns)

        # ====== LADO DIREITO: Preview ======
        self.stack = QStackedWidget()
        self.chart_view = QChartView(); self.chart_view.setRenderHint(QPainter.Antialiasing)
        self.text_view  = QTextBrowser()
        self.text_view.setOpenExternalLinks(False)
        self.text_view.setStyleSheet("QTextBrowser { background:#1B1D1E; color:#E0E0E0; border:1px solid #11398a; }")

        self.stack.addWidget(self.chart_view)  # idx 0
        self.stack.addWidget(self.text_view)   # idx 1

        # ====== SPLITTER ======
        split = QSplitter()
        split.addWidget(left); split.addWidget(self.stack)
        split.setStretchFactor(0, 0); split.setStretchFactor(1, 1)

        root = QVBoxLayout(self); root.addWidget(split)

        # sinais
        self.btn_preview.clicked.connect(self.atualizar_preview)
        self.btn_export.clicked.connect(self.exportar)
        self.btn_fechar.clicked.connect(self.reject)
        self.cmb_tipo.currentIndexChanged.connect(self._tipo_changed)

        self._tipo_changed()

    # ------------------------- DATA HELPERS -------------------------
    def _rows(self, sql: str, params=()):
        return self.db.fetch_all(sql, list(params))

    def _mes_label(self, ym: str) -> str:
        # ym = 'YYYYMM' -> 'MM/AAAA'
        if len(ym) == 6:
            return f"{ym[4:6]}/{ym[0:4]}"
        return ym

    def _serie_bar_dupla(self, categorias: list[str], rec: list[float], des: list[float], titulo: str) -> QChart:
        s_rec = QBarSet("Receitas"); s_rec.append(rec)
        s_des = QBarSet("Despesas"); s_des.append(des)
        series = QBarSeries(); series.append(s_rec); series.append(s_des)

        chart = QChart(); chart.addSeries(series); chart.setTitle(titulo)
        axisX = QBarCategoryAxis(); axisX.append(categorias); chart.addAxis(axisX, Qt.AlignBottom); series.attachAxis(axisX)
        axisY = QValueAxis(); axisY.setTitleText("R$"); chart.addAxis(axisY, Qt.AlignLeft); series.attachAxis(axisY)
        chart.setAnimationOptions(QChart.SeriesAnimations)
        return chart

    # ------------------------- CONSULTAS -------------------------
    def _dados_mes_geral(self, d1_ord: int, d2_ord: int):
        sql = """
        SELECT substr(CAST(data_ord AS TEXT),1,6) AS ym,
               SUM(valor_entrada) AS rec, SUM(valor_saida) AS des
        FROM lancamento
        WHERE data_ord BETWEEN ? AND ?
        GROUP BY ym ORDER BY ym
        """
        rows = self._rows(sql, (d1_ord, d2_ord))
        cats, r, d = [], [], []
        for ym, rec, des in rows:
            cats.append(self._mes_label(ym)); r.append(rec or 0); d.append(des or 0)
        return cats, r, d

    def _dados_por_fazenda(self, d1_ord: int, d2_ord: int):
        sql = """
        SELECT i.nome_imovel,
               SUM(l.valor_entrada) AS rec,
               SUM(l.valor_saida)   AS des
        FROM lancamento l
        JOIN imovel_rural i ON i.id = l.cod_imovel
        WHERE l.data_ord BETWEEN ? AND ?
        GROUP BY i.nome_imovel
        ORDER BY (rec - des) DESC
        """
        rows = self._rows(sql, (d1_ord, d2_ord))
        cats, r, d = [], [], []
        for nome, rec, des in rows:
            cats.append(nome); r.append(rec or 0); d.append(des or 0)
        return cats, r, d

    def _dados_anual(self, d1_ord: int, d2_ord: int):
        sql = """
        SELECT substr(CAST(data_ord AS TEXT),1,4) AS ano,
               SUM(valor_entrada) AS rec, SUM(valor_saida) AS des
        FROM lancamento
        WHERE data_ord BETWEEN ? AND ?
        GROUP BY ano ORDER BY ano
        """
        rows = self._rows(sql, (d1_ord, d2_ord))
        cats, r, d = [], [], []
        for ano, rec, des in rows:
            cats.append(str(ano)); r.append(rec or 0); d.append(des or 0)
        return cats, r, d

    def _dre_mes_a_mes(self, d1_ord: int, d2_ord: int, por_fazenda: bool):
        if por_fazenda:
            sql = """
            SELECT i.nome_imovel,
                   substr(CAST(l.data_ord AS TEXT),1,6) AS ym,
                   SUM(l.valor_entrada) AS rec, SUM(l.valor_saida) AS des
            FROM lancamento l JOIN imovel_rural i ON i.id = l.cod_imovel
            WHERE l.data_ord BETWEEN ? AND ?
            GROUP BY i.nome_imovel, ym ORDER BY i.nome_imovel, ym
            """
            return self._rows(sql, (d1_ord, d2_ord))
        else:
            sql = """
            SELECT substr(CAST(data_ord AS TEXT),1,6) AS ym,
                   SUM(valor_entrada) AS rec, SUM(valor_saida) AS des
            FROM lancamento
            WHERE data_ord BETWEEN ? AND ?
            GROUP BY ym ORDER BY ym
            """
            return self._rows(sql, (d1_ord, d2_ord))

    def _dre_por_fazenda(self, d1_ord: int, d2_ord: int):
        sql = """
        SELECT i.nome_imovel,
               SUM(l.valor_entrada) AS rec,
               SUM(l.valor_saida)   AS des
        FROM lancamento l
        JOIN imovel_rural i ON i.id = l.cod_imovel
        WHERE l.data_ord BETWEEN ? AND ?
        GROUP BY i.nome_imovel
        ORDER BY i.nome_imovel
        """
        return self._rows(sql, (d1_ord, d2_ord))

    # ------------------------- RENDER -------------------------
    def _tipo_changed(self):
        # Para relatórios de tabela: mostrar texto; para comparativos: gráfico
        t = self.cmb_tipo.currentText()
        self.stack.setCurrentIndex(1 if "DRE" in t else 0)

    def atualizar_preview(self):
        d1_ord = qdate_to_ord(self.dt_ini.date())
        d2_ord = qdate_to_ord(self.dt_fim.date())
        t = self.cmb_tipo.currentText()

        if t == "Receitas x Despesas (Geral)":
            cats, r, d = self._dados_mes_geral(d1_ord, d2_ord)
            chart = self._serie_bar_dupla(cats, r, d, "Receitas x Despesas (Geral)")
            self.chart_view.setChart(chart); self.stack.setCurrentIndex(0)

        elif t == "Comparativo entre Fazendas":
            cats, r, d = self._dados_por_fazenda(d1_ord, d2_ord)
            chart = self._serie_bar_dupla(cats, r, d, "Comparativo entre Fazendas")
            self.chart_view.setChart(chart); self.stack.setCurrentIndex(0)

        elif t == "Comparativo Anual":
            cats, r, d = self._dados_anual(d1_ord, d2_ord)
            chart = self._serie_bar_dupla(cats, r, d, "Comparativo Anual")
            self.chart_view.setChart(chart); self.stack.setCurrentIndex(0)

        elif t == "DRE Simplificada (mês a mês)":
            por_faz = self.chk_por_fazenda.isChecked()
            rows = self._dre_mes_a_mes(d1_ord, d2_ord, por_faz)
            html = self._html_dre_mes(rows, por_faz)
            self.text_view.setHtml(html); self.stack.setCurrentIndex(1)

        elif t == "DRE por Fazenda (multi-seção)":
            rows = self._dre_por_fazenda(d1_ord, d2_ord)
            html = self._html_dre_por_fazenda(rows)
            self.text_view.setHtml(html); self.stack.setCurrentIndex(1)

    def _html_header(self, titulo: str) -> str:
        return f"""
    <html><head>
    <style>
    @page {{ margin: 0; }}
    html, body {{
      margin: 0;
      padding: 12mm;
      background:#1B1D1E;
      color:#E0E0E0;
      font-family:'Segoe UI', Arial, sans-serif;
      -webkit-print-color-adjust: exact;
      print-color-adjust: exact;
    }}
    /* Cartões / seções */
    .card {{
      border:1px solid #11398a;
      border-radius:10px;
      padding:16px;
      margin:14px 0;
      background:#0d1b3d;
      page-break-inside: avoid;
    }}
    /* Título */
    h1 {{
      margin:0 0 12px 0;
      font-size:18pt;
      font-weight:600;
    }}
    /* Tabelas */
    table {{ width:100%; border-collapse:collapse; margin-top:8px; }}
    th, td {{ padding:8px 10px; border-bottom:1px solid #11398a; text-align:center; }}
    th {{ background:#11398a; color:#fff; }}
    /* Resultado por sinal */
    .ok  {{ color:#27AE60; font-weight:700; }}   /* lucro → verde */
    .bad {{ color:#E74C3C; font-weight:700; }}   /* prejuízo → vermelho */
    .muted {{ color:#9aa1b1; font-size:11px; margin-top:6px; }}
    </style></head><body>
    <h1>{titulo}</h1>
    """    
    
    def _html_footer(self) -> str:
        return "</body></html>"

    def _html_dre_mes(self, rows, por_fazenda: bool) -> str:
        title = "DRE Simplificada (mês a mês)" + (" — por Fazenda" if por_fazenda else "")
        html = self._html_header(title)
        if por_fazenda:
            # rows: (fazenda, ym, rec, des)
            atual = None; total_r=total_d=0
            for faz, ym, rec, des in rows:
                if atual != faz:
                    if atual is not None:
                        resultado = total_r - total_d
                        html += (
                            f"<tr>"
                            f"<th>Total</th>"
                            f"<th>{fmt_money(total_r)}</th>"
                            f"<th>{fmt_money(total_d)}</th>"
                            f"<th class='{ 'ok' if resultado>=0 else 'bad' }'>{fmt_money(resultado)}</th>"
                            f"</tr></table></div>"
                        )
                    html += f"<div class='card'><h2>{faz}</h2><table><tr><th>Mês</th><th>Receitas</th><th>Despesas</th><th>Resultado</th></tr>"
                    atual = faz; total_r = total_d = 0
                r = rec or 0; d = des or 0; res = r - d
                total_r += r; total_d += d
                html += f"<tr><td>{self._mes_label(ym)}</td><td>{fmt_money(r)}</td><td>{fmt_money(d)}</td><td class='{ 'ok' if res>=0 else 'bad' }'>{fmt_money(res)}</td></tr>"
            if atual is not None:
                resultado = total_r - total_d
                html += (
                    f"<tr>"
                    f"<th>Total</th>"
                    f"<th>{fmt_money(total_r)}</th>"
                    f"<th>{fmt_money(total_d)}</th>"
                    f"<th class='{ 'ok' if resultado>=0 else 'bad' }'>{fmt_money(resultado)}</th>"
                    f"</tr></table></div>"
                )
        else:
            # rows: (ym, rec, des)
            html += "<div class='card'><table><tr><th>Mês</th><th>Receitas</th><th>Despesas</th><th>Resultado</th></tr>"
            total_r=total_d=0
            for ym, rec, des in rows:
                r = rec or 0; d = des or 0; res = r - d
                total_r += r; total_d += d
                html += f"<tr><td>{self._mes_label(ym)}</td><td>{fmt_money(r)}</td><td>{fmt_money(d)}</td><td class='{ 'ok' if res>=0 else 'bad' }'>{fmt_money(res)}</td></tr>"
            resultado = total_r - total_d
            html += (
                f"<tr>"
                f"<th>Total</th>"
                f"<th>{fmt_money(total_r)}</th>"
                f"<th>{fmt_money(total_d)}</th>"
                f"<th class='{ 'ok' if resultado>=0 else 'bad' }'>{fmt_money(resultado)}</th>"
                f"</tr></table></div>"
            )
        html += self._html_footer()
        return html

    def _html_dre_por_fazenda(self, rows) -> str:
        # rows: (fazenda, rec, des)
        html = self._html_header("DRE por Fazenda (multi-seção)")
        for faz, rec, des in rows:
            rec = rec or 0; des = des or 0; res = rec - des
            html += f"""
            <div class='card'>
              <h2>{faz}</h2>
              <table>
                <tr><th>Indicador</th><th>Valor</th></tr>
                <tr><td>Receitas</td><td>{fmt_money(rec)}</td></tr>
                <tr><td>Despesas</td><td>{fmt_money(des)}</td></tr>
                <tr><td><b>Resultado</b></td><td class='{ 'ok' if res>=0 else 'bad' }'><b>{fmt_money(res)}</b></td></tr>
              </table>
              <div class='muted'>Período: {self.dt_ini.date().toString("dd/MM/yyyy")} a {self.dt_fim.date().toString("dd/MM/yyyy")}</div>
            </div>
            """
        html += self._html_footer()
        return html

    # ------------------------- EXPORTAR -------------------------
    def exportar(self):
        idx = self.stack.currentIndex()
        if idx == 1:
            # Exporta HTML -> PDF
            path, _ = QFileDialog.getSaveFileName(self, "Salvar Relatório em PDF", "", "PDF (*.pdf)")
            if not path: return
            printer = QPrinter(QPrinter.HighResolution)
            printer.setOutputFormat(QPrinter.PdfFormat)
            printer.setOutputFileName(path)

            # ▼ NOVO: margens 0 e página inteira
            try:
                from PySide6.QtGui import QPageLayout, QPageSize
                from PySide6.QtCore import QMarginsF
                layout = printer.pageLayout()
                layout.setMode(QPageLayout.FullPage)
                layout.setMargins(QMarginsF(0, 0, 0, 0))
                printer.setPageLayout(layout)
            except Exception:
                try:
                    printer.setFullPage(True)  # fallback para algumas builds
                except Exception:
                    pass
                
            doc = self.text_view.document()
            try:
                doc.setDocumentMargin(0)  # ▼ NOVO: remove margem interna do QTextDocument
            except Exception:
                pass
            
            (getattr(doc, "print_", None) or getattr(doc, "print"))(printer)


            QMessageBox.information(self, "OK", "PDF gerado com sucesso.")
        else:
            # Exporta gráfico -> PNG
            path, _ = QFileDialog.getSaveFileName(self, "Salvar Gráfico em PNG", "", "PNG (*.png)")
            if not path: return
            img = self.chart_view.grab()
            img.save(path, "PNG")
            QMessageBox.information(self, "OK", "Imagem salva com sucesso.")

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
        # campo de data do lançamento
        self.data = QDateEdit(QDate.currentDate())
        self.data.setCalendarPopup(True)
        self.data.setDisplayFormat("dd/MM/yyyy")
        form.addRow("Data:", self.data)

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
        self.participante = QComboBox(self)
        self.participante.setEditable(True)
        self.participante.setInsertPolicy(QComboBox.NoInsert)

        self.participante.clear()
        self.participante.setCurrentIndex(-1)
        self.participante.setPlaceholderText("Selecione participante (nome ou CNPJ)")
        self.participante.lineEdit().setPlaceholderText("Selecione participante (nome ou CNPJ)")
        _opcoes = []
        for id_, nome, doc in self.db.fetch_all("SELECT id, nome, cpf_cnpj FROM participante ORDER BY nome"):
            txt = f"{nome} — {doc}"
            self.participante.addItem(txt, id_)
            _opcoes.append(txt)

        comp = QCompleter(_opcoes, self.participante)
        comp.setCaseSensitivity(Qt.CaseInsensitive)
        comp.setFilterMode(Qt.MatchContains)
        self.participante.setCompleter(comp)

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
        _d = QDate.fromString(data or "", "dd/MM/yyyy")
        if not _d.isValid():
            _d = QDate.currentDate()
        self.data.setDate(_d)
    
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
        self.valor_entrada.setText("" if ent is None else f"{float(ent):.2f}")
        self.valor_saida.setText("" if sai is None else f"{float(sai):.2f}")
    
    def salvar(self):
        try:
            # Campos obrigatórios
            if not (self.imovel.currentData() and self.conta.currentData() and self.historico.text().strip()):
                QMessageBox.warning(self, "Campos Obrigatórios", "Preencha todos os campos obrigatórios!")
                return

            # SUBSTITUA POR:
            raw_num = (self.num_doc.text() or '').strip()
            # normaliza removendo TUDO que não é dígito (ex.: "123/2025", "123-2025", "123 2025")
            norm_num = re.sub(r'\D+', '', raw_num)
            part = self.participante.currentData()


            # Verifica duplicata: mesmo número de documento + mesmo participante
            if norm_num:
                sql = """
                    SELECT id FROM lancamento
                    WHERE REPLACE(COALESCE(num_doc,''),' ','') = ? AND id_participante = ?
                """
                params = [norm_num, part]
                if self.lanc_id:
                    sql += " AND id != ?"
                    params.append(self.lanc_id)
                existente = self.db.fetch_one(sql, params)
                if existente:
                    QMessageBox.warning(
                        self, "Lançamento Duplicado",
                        f"Já existe um lançamento (ID {existente[0]})\n"
                        f"com nota nº {raw_num} para este participante."
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

            # Calcula o saldo anterior CORRETO (sem contar o próprio lançamento)
            conta_id = self.conta.currentData()

            # se estiver editando, pegue o saldo do lançamento ANTERIOR (id menor) da mesma conta
            if self.lanc_id:
                row_prev = self.db.fetch_one(
                    "SELECT (saldo_final * CASE natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) "
                    "FROM lancamento WHERE cod_conta=? AND id < ? ORDER BY id DESC LIMIT 1",
                    (conta_id, self.lanc_id),
                )
                saldo_ant = row_prev[0] if row_prev and row_prev[0] is not None else 0.0
            else:
                # inserção: usa o último saldo existente na conta
                row_prev = self.db.fetch_one(
                    "SELECT (saldo_final * CASE natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) "
                    "FROM lancamento WHERE cod_conta=? ORDER BY id DESC LIMIT 1",
                    (conta_id,),
                )
                saldo_ant = row_prev[0] if row_prev and row_prev[0] is not None else 0.0

            saldo_f = saldo_ant + ent - sai
            nat = 'P' if saldo_f >= 0 else 'N'


            # antes de tudo, capture data e hora atuais
            now = datetime.now().strftime("%d/%m/%Y %H:%M")
            usuario_ts = f"{CURRENT_USER} dia {now}"

            data_str = self.data.date().toString("dd/MM/yyyy")
            data_ord = int(self.data.date().toString("yyyyMMdd"))

            # Parâmetros para INSERT/UPDATE (sem categoria)
            params = [
                data_str,
                self.imovel.currentData(),
                self.conta.currentData(),
                raw_num or None,
                self.tipo_doc.currentIndex() + 1,
                self.historico.text().strip(),
                part,
                self.tipo_lanc.currentIndex() + 1,
                ent, sai, abs(saldo_f), nat, usuario_ts,
                data_ord
            ]
            
            if self.lanc_id:
                # 1) Atualiza o registro editado
                sql = """
                UPDATE lancamento SET
                    data = ?, cod_imovel = ?, cod_conta = ?, num_doc = ?, tipo_doc = ?,
                    historico = ?, id_participante = ?, tipo_lanc = ?,
                    valor_entrada = ?, valor_saida = ?, saldo_final = ?,
                    natureza_saldo = ?, usuario = ?, data_ord = ?
                WHERE id = ?
                """
                self.db.execute_query(sql, params + [self.lanc_id])

                # 2) Recalcula em CADEIA os lançamentos posteriores da mesma conta
                saldo_atual = saldo_f
                with self.db.bulk():
                    rows = self.db.fetch_all(
                        "SELECT id, valor_entrada, valor_saida "
                        "FROM lancamento WHERE cod_conta=? AND id > ? ORDER BY id",
                        (conta_id, self.lanc_id),
                    )
                    for rid, v_ent, v_sai in rows:
                        saldo_atual = saldo_atual + float(v_ent or 0) - float(v_sai or 0)
                        nat_r = 'P' if saldo_atual >= 0 else 'N'
                        self.db.execute_query(
                            "UPDATE lancamento SET saldo_final=?, natureza_saldo=? WHERE id=?",
                            (abs(saldo_atual), nat_r, rid),
                            autocommit=False
                        )

            else:
                sql = """
                INSERT INTO lancamento (
                    data, cod_imovel, cod_conta, num_doc, tipo_doc,
                    historico, id_participante, tipo_lanc,
                    valor_entrada, valor_saida, saldo_final,
                    natureza_saldo, usuario, data_ord
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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

            ListAccelerator.build_cache(self.tabela)
            ListCounter.refresh(self.tabela)

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
        resp = QMessageBox.question(self, "Confirmar Exclusão", "Deseja excluir 1 registro?", QMessageBox.Yes | QMessageBox.No)
        if resp == QMessageBox.Yes:
            try:
                self.db.execute_query("DELETE FROM imovel_rural WHERE id=?", (id_,))
                QMessageBox.information(self, "Sucesso", "Imóvel excluído!")
                self.carregar_imoveis()
            except Exception as e:
                QMessageBox.critical(self, "Erro", f"Erro ao excluir: {e}")

    def _reload_participantes(self):
        """Recarrega o combo de participantes mantendo a seleção atual."""
        cur_id = self.participante.currentData() if hasattr(self, "participante") else None
        self.participante.blockSignals(True)
        try:
            self.participante.clear()
            self.participante.addItem("Selecione...", None)
            for id_, nome in self.db.fetch_all("SELECT id, nome FROM participante ORDER BY nome"):
                self.participante.addItem(nome, id_)
            if cur_id is not None:
                idx = self.participante.findData(cur_id)
                if idx >= 0:
                    self.participante.setCurrentIndex(idx)
        finally:
            self.participante.blockSignals(False)

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
        self.tabela.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
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
        hdr.setSectionResizeMode(QHeaderView.Stretch)  # ocupa toda a largura (sem “sobras”)

        # ordenação cíclica
        hdr.sectionDoubleClicked.connect(self._toggle_sort)
        # clique ativa botões
        self.tabela.cellClicked.connect(self._select_row)

        self.layout.addWidget(self.tabela)

    def _filter_contas(self, text: str):
        ListAccelerator.filter(self.tabela, text, delay_ms=0)

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

        total = len(df.index)
        GlobalProgress.begin("Importando contas (Excel)…", maximo=total, parent=self.window())
        try:
            with self.db.bulk():
                for i, row in enumerate(df.itertuples(index=False), start=1):
                    self.db.execute_query(
                        """
                        INSERT OR REPLACE INTO conta_bancaria (
                            cod_conta, pais_cta, banco, nome_banco, agencia, num_conta, saldo_inicial
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            row.cod_conta, row.pais_cta, row.banco, row.nome_banco,
                            row.agencia, row.num_conta, float(row.saldo_inicial or 0)
                        ),
                        autocommit=False
                    )
                    if i % 100 == 0:
                        GlobalProgress.set_value(i)
            GlobalProgress.set_value(total)
        finally:
            GlobalProgress.end()

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

        ListAccelerator.build_cache(self.tabela)
        ListCounter.refresh(self.tabela)

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
        qtd = len(rows)
        if QMessageBox.question(self, "Confirmar Exclusão", f"Deseja excluir {qtd} registro(s)?", QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
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
        ListAccelerator.filter(self.tabela, text, delay_ms=0)

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
        # conta linhas para o progresso
        with open(path, encoding='utf-8') as _f:
            total = sum(1 for _ in _f)

        GlobalProgress.begin("Importando imóveis (TXT)…", maximo=total, parent=self.window())
        try:
            with self.db.bulk():
                with open(path, encoding='utf-8') as f:
                    for lineno, line in enumerate(f, 1):
                        parts = line.strip().split("|")
                        if len(parts) != 18:
                            raise ValueError(f"Linha {lineno}: esperado 18 campos, encontrou {len(parts)}")
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
                                cod_mun, cep, int(tipo_exploracao), float(participacao),
                                float(area_total), float(area_utilizada)
                            ],
                            autocommit=False
                        )
                        if lineno % 50 == 0:
                            GlobalProgress.set_value(lineno)
            GlobalProgress.set_value(total)
        finally:
            GlobalProgress.end()

    def _import_imoveis_excel(self, path: str):
        df = pd.read_excel(path, dtype=str)
        required = [
            'cod_imovel','pais','moeda','cad_itr','caepf','insc_estadual',
            'nome_imovel','endereco','num','compl','bairro','uf',
            'cod_mun','cep','tipo_exploracao','participacao','area_total','area_utilizada'
        ]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Colunas faltando no Excel: {', '.join(missing)}")
    
        df.fillna('', inplace=True)
        total = len(df.index)
        GlobalProgress.begin("Importando imóveis (Excel)…", maximo=total, parent=self.window())
        try:
            with self.db.bulk():
                for i, row in enumerate(df.itertuples(index=False), start=1):
                    self.db.execute_query(
                        """
                        INSERT OR REPLACE INTO imovel_rural (
                            cod_imovel, pais, moeda, cad_itr, caepf, insc_estadual,
                            nome_imovel, endereco, num, compl, bairro, uf,
                            cod_mun, cep, tipo_exploracao, participacao, area_total, area_utilizada
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        [
                            row.cod_imovel, row.pais, row.moeda,
                            (row.cad_itr or None), (row.caepf or None), (row.insc_estadual or None),
                            row.nome_imovel, row.endereco,
                            (row.num or None), (row.compl or None), row.bairro, row.uf,
                            row.cod_mun, row.cep, int(row.tipo_exploracao), float(row.participacao or 0),
                            float(row.area_total or 0), float(row.area_utilizada or 0)
                        ],
                        autocommit=False
                    )
                    if i % 50 == 0:
                        GlobalProgress.set_value(i)
            GlobalProgress.set_value(total)
        finally:
            GlobalProgress.end()

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
        qtd = len(indices)
        resp = QMessageBox.question(self, "Confirmar Exclusão", f"Deseja excluir {qtd} registro(s)?", QMessageBox.Yes | QMessageBox.No)

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
        self.layout = QVBoxLayout(self); self.layout.setContentsMargins(0,0,0,0)
        self._build_ui(); self._load_participantes_column_filter(); self.carregar_participantes()

    def _build_ui(self):
        tl = QHBoxLayout(); tl.setContentsMargins(12, 8, 12, 8)
        self.btn_novo = QPushButton("Novo Participante"); self.btn_novo.setIcon(QIcon.fromTheme("document-new")); self.btn_novo.clicked.connect(self.novo_participante); tl.addWidget(self.btn_novo)
        self.btn_editar = QPushButton("Editar"); self.btn_editar.setEnabled(False); self.btn_editar.setIcon(QIcon.fromTheme("document-edit")); self.btn_editar.clicked.connect(self.editar_participante); tl.addWidget(self.btn_editar)
        self.btn_excluir = QPushButton("Excluir"); self.btn_excluir.setEnabled(False); self.btn_excluir.setIcon(QIcon.fromTheme("edit-delete")); self.btn_excluir.clicked.connect(self.excluir_participante); tl.addWidget(self.btn_excluir)
        self.btn_importar = QPushButton("Importar"); self.btn_importar.setIcon(QIcon.fromTheme("document-import")); self.btn_importar.clicked.connect(self.importar_participantes); tl.addWidget(self.btn_importar)
        self.btn_exportar = QPushButton("Exportação"); self.btn_exportar.setIcon(QIcon.fromTheme("document-export")); self.btn_exportar.clicked.connect(self.exportar_participantes); tl.addWidget(self.btn_exportar)
        self.btn_cadastrar_novos = QPushButton("Cadastrar novos participantes"); self.btn_cadastrar_novos.setIcon(QIcon.fromTheme("list-add")); self.btn_cadastrar_novos.clicked.connect(self.cadastrar_novos_participantes); tl.addWidget(self.btn_cadastrar_novos)
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
        # usa cache por linha e aplica já (delay=0)
        ListAccelerator.filter(self.tabela, text, delay_ms=0)

    def importar_participantes(self):
        path, _ = QFileDialog.getOpenFileName(self, "Importar Participantes", "", "TXT (*.txt);Excel (*.xlsx *.xls)")
        if not path: return
        try:
            self._import_participantes_txt(path) if path.lower().endswith('.txt') else self._import_participantes_excel(path)
            self.carregar_participantes()
        except Exception:
            QMessageBox.warning(self, "Importação Falhou", "Arquivo não segue o layout esperado e não foi importado.")

    def exportar_participantes(self):
        # Janela com campo de pasta + "..."
        dlg = QDialog(self)
        dlg.setWindowTitle("Exportar participantes")
        form = QFormLayout(dlg)

        w = QWidget()
        hl = QHBoxLayout(w)
        hl.setContentsMargins(0, 0, 0, 0)

        ed_dir = QLineEdit()
        btn_browse = QToolButton()
        btn_browse.setText("…")

        def _pick_dir():
            p = QFileDialog.getExistingDirectory(self, "Selecionar pasta de exportação", "")
            if p:
                ed_dir.setText(p)

        btn_browse.clicked.connect(_pick_dir)
        hl.addWidget(ed_dir)
        hl.addWidget(btn_browse)
        form.addRow("Pasta de exportação:", w)

        # Carrega pasta padrão de um JSON (se existir)
        try:
            cfg_dir = os.path.join(PROJECT_DIR, "layout importacao", "participantes")
            os.makedirs(cfg_dir, exist_ok=True)
            cfg_file = os.path.join(cfg_dir, "export_participantes_path.json")
            if os.path.exists(cfg_file):
                with open(cfg_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    ed_dir.setText(data.get("export_folder", ""))
        except Exception:
            pass

        # Botões OK/Cancelar
        bb = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        form.addWidget(bb)
        bb.accepted.connect(dlg.accept)
        bb.rejected.connect(dlg.reject)

        if not dlg.exec():
            return

        out_dir = ed_dir.text().strip()
        if not out_dir:
            QMessageBox.warning(self, "Caminho inválido", "Informe a pasta de exportação.")
            return

        # Salva a pasta escolhida no JSON
        try:
            with open(cfg_file, "w", encoding="utf-8") as f:
                json.dump({"export_folder": out_dir}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            QMessageBox.warning(self, "Aviso", f"Não foi possível salvar a pasta padrão:\n{e}")

        # Monta o nome do arquivo
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        out_path = os.path.join(out_dir, f"participantes_{ts}.txt")

        # Busca participantes e escreve no layout de importação: cpf_cnpj|nome|tipo
        try:
            rows = self.db.fetch_all(
                "SELECT cpf_cnpj, nome, tipo_contraparte FROM participante ORDER BY nome"
            )
            if not rows:
                QMessageBox.information(self, "Sem dados", "Não há participantes para exportar.")
                return

            with open(out_path, "w", encoding="utf-8", newline="") as f:
                for cpf, nome, tipo in rows:
                    f.write(f"{(cpf or '').strip()}|{(nome or '').strip()}|{int(tipo)}\n")

        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Falha ao exportar arquivo:\n{e}")
            return

        QMessageBox.information(self, "Exportação concluída", f"Arquivo gerado:\n{out_path}")

    def cadastrar_novos_participantes(self):
        import importlib.util, json, os
        dlg = QDialog(self)
        dlg.setWindowTitle("Cadastrar novos participantes")
        form = QFormLayout(dlg)

        # Arquivo participantes (TXT)
        w1 = QWidget(); h1 = QHBoxLayout(w1); h1.setContentsMargins(0,0,0,0)

        # Arquivo participantes (TXT)
        w1 = QWidget(); h1 = QHBoxLayout(w1); h1.setContentsMargins(0,0,0,0)
        ed_part = QLineEdit(); btn1 = QToolButton(); btn1.setText("…")
        def _pick_part():
            p, _ = QFileDialog.getOpenFileName(self, "Selecionar lista de participantes (TXT)", "", "TXT (*.txt)")
            if p: ed_part.setText(p)
        btn1.clicked.connect(_pick_part)
        h1.addWidget(ed_part); h1.addWidget(btn1)
        form.addRow("Lista de participantes (TXT):", w1)
        
        # Arquivo PAGAMENTOS (TXT)
        w2 = QWidget(); h2 = QHBoxLayout(w2); h2.setContentsMargins(0,0,0,0)
        ed_pag = QLineEdit(); btn2 = QToolButton(); btn2.setText("…")
        def _pick_pag():
            p, _ = QFileDialog.getOpenFileName(self, "Selecionar PAGAMENTOS.txt", "", "TXT (*.txt)")
            if p: ed_pag.setText(p)
        btn2.clicked.connect(_pick_pag)
        h2.addWidget(ed_pag); h2.addWidget(btn2)
        form.addRow("PAGAMENTOS.txt:", w2)
        
        # 🔹 Agora sim: carrega valores salvos em JSON (se existirem)
        try:
            cfg_dir  = os.path.join(PROJECT_DIR, "layout importacao", "participantes")
            cfg_file = os.path.join(cfg_dir, "novos_participantes_paths.json")
            if os.path.exists(cfg_file):
                import json
                with open(cfg_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if "participantes_path" in data:
                        ed_part.setText(data["participantes_path"])
                    if "pagamentos_path" in data:
                        ed_pag.setText(data["pagamentos_path"])
        except Exception:
            pass

        # 🔹 Ajusta a largura dos campos de acordo com o texto
        font_metrics = ed_part.fontMetrics()
        ed_part.setMinimumWidth(font_metrics.horizontalAdvance(ed_part.text()) + 50)
        ed_pag.setMinimumWidth(font_metrics.horizontalAdvance(ed_pag.text()) + 50)
        
        # 🔹 Redimensiona automaticamente a janela para caber nos campos
        dlg.adjustSize()
        
        # Botões OK/Cancelar
        bb = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        form.addWidget(bb); bb.accepted.connect(dlg.accept); bb.rejected.connect(dlg.reject)

        if not dlg.exec():
            return

        part_path = ed_part.text().strip()
        pag_path  = ed_pag.text().strip()
        if not part_path or not pag_path:
            QMessageBox.warning(self, "Caminhos inválidos", "Preencha os dois caminhos (TXT).")
            return

        # Salva os caminhos escolhidos em JSON dentro de layout importacao/participantes
        try:
            cfg_dir  = os.path.join(PROJECT_DIR, "layout importacao", "participantes")
            os.makedirs(cfg_dir, exist_ok=True)
            cfg_file = os.path.join(cfg_dir, "novos_participantes_paths.json")
            with open(cfg_file, "w", encoding="utf-8") as f:
                json.dump({"participantes_path": part_path, "pagamentos_path": pag_path}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            QMessageBox.warning(self, "Aviso", f"Não foi possível salvar o JSON de caminhos:\n{e}")

        # Executa o gerador de novos participantes com os caminhos escolhidos
        try:
            mod_path = os.path.join(PROJECT_DIR, "layout importacao", "participantes", "novos_participantes.py")
            spec = importlib.util.spec_from_file_location("novos_participantes", mod_path)
            np = importlib.util.module_from_spec(spec); spec.loader.exec_module(np)
            np.main(part_path, pag_path)  # gera/atualiza o TXT de participantes a partir do PAGAMENTOS
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Falha ao executar novos_participantes.py:\n{e}")
            return

        # Importa APENAS o TXT de participantes (igual ao botão Importar)
        try:
            self._import_participantes_txt(part_path)
            self.carregar_participantes()
            QMessageBox.information(self, "Concluído", "Participantes atualizados com sucesso.")
        except Exception as e:
            QMessageBox.warning(self, "Importação Falhou", f"Não foi possível importar o TXT informado.\n{e}")

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

        ListAccelerator.build_cache(self.tabela)
        ListCounter.refresh(self.tabela)

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
        self._create_profile_banner()

        install_counters_for_all_tables(self)

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

        row = self.db.fetch_one("SELECT MIN(data_ord), MAX(data_ord) FROM lancamento WHERE data_ord IS NOT NULL")
        if row and row[0] and row[1]:
            _ini = QDate.fromString(str(row[0]), "yyyyMMdd")
            _fim = QDate.fromString(str(row[1]), "yyyyMMdd")
        else:
            _ini = QDate.currentDate().addMonths(-1)
            _fim = QDate.currentDate()

        self.dt_ini = QDateEdit(_ini); self.dt_ini.setCalendarPopup(True); self.dt_ini.setDisplayFormat("dd/MM/yyyy")
        self.lanc_filter_layout.addWidget(self.dt_ini)
        self.lanc_filter_layout.addWidget(QLabel("Até:"))
        self.dt_fim = QDateEdit(_fim); self.dt_fim.setCalendarPopup(True); self.dt_fim.setDisplayFormat("dd/MM/yyyy")
        self.lanc_filter_layout.addWidget(self.dt_fim)

        btn_filtrar = QPushButton("Filtrar"); btn_filtrar.clicked.connect(self.carregar_lancamentos); self.lanc_filter_layout.addWidget(btn_filtrar)
        self.btn_edit_lanc = QPushButton("Editar Lançamento"); self.btn_edit_lanc.setEnabled(False); self.btn_edit_lanc.clicked.connect(self.editar_lancamento)
        self.lanc_filter_layout.addWidget(self.btn_edit_lanc)
        self.btn_del_lanc = QPushButton("Excluir Lançamento"); self.btn_del_lanc.setEnabled(False); self.btn_del_lanc.clicked.connect(self.excluir_lancamento)
        self.lanc_filter_layout.addWidget(self.btn_del_lanc)
        self.btn_importacao = QPushButton("Importação"); 
        self.btn_importacao.setIcon(QIcon.fromTheme("document-import"))
        self.btn_importacao.clicked.connect(self.show_import_dialog)
        self.lanc_filter_layout.addWidget(self.btn_importacao)
        
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

        hdr = self.tab_lanc.horizontalHeader()
        hdr.setSectionsMovable(True)
        hdr.setStretchLastSection(False)

        # Não quebra linha nem corta texto
        self.tab_lanc.setWordWrap(False)
        self.tab_lanc.setTextElideMode(Qt.ElideNone)

        # Scroll horizontal se passar do limite
        self.tab_lanc.setHorizontalScrollMode(QTableWidget.ScrollPerPixel)
        self.tab_lanc.setSizeAdjustPolicy(QTableWidget.AdjustToContents)

        self.tab_lanc.setSelectionBehavior(QTableWidget.SelectRows); self.tab_lanc.setEditTriggers(QTableWidget.NoEditTriggers)
        self.tab_lanc.cellClicked.connect(lambda r,_: (self.btn_edit_lanc.setEnabled(True), self.btn_del_lanc.setEnabled(True)))
        l_l.addWidget(self.tab_lanc)
        
        # aplica filtro inicial ao abrir
        self.carregar_lancamentos()
        self.dashboard.load_data()
        # contador no layout (sem sobrepor a barra de filtros)
        attach_counter_in_layout(self.tab_lanc, self.lanc_filter_layout)

        config_file = os.path.join(CACHE_FOLDER, 'Cleuber Marcos', 'json', 'lanc_columns.json')
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                vis = json.load(f)
            for i, label in enumerate(self.tab_lanc.horizontalHeaderLabels()):
                self.tab_lanc.setColumnHidden(i, not vis.get(label, True))

        hdr = self.tab_lanc.horizontalHeader()
        hdr.sectionDoubleClicked.connect(self._sort_lanc_by_column)
        hdr.setSortIndicatorShown(True)  # opcional (mostra a setinha)

        for i, _ in enumerate(self._lanc_labels):
            hdr.setSectionResizeMode(i, QHeaderView.ResizeToContents)

        # Reajusta sempre que o modelo mudar (ocultar/exibir colunas etc.)
        m = self.tab_lanc.model()
        m.layoutChanged.connect(lambda *_: self._ajustar_colunas_lanc())
        m.modelReset.connect(lambda *_: self._ajustar_colunas_lanc())

        # Ativa stretch na última coluna para ocupar sobra
        hdr.setStretchLastSection(True)

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

        # >>> Habilita ordenação por duplo clique em TODAS as tabelas desta janela
        install_sorting_for_all_tables(self)
        
        # NOVO: indexação/cache + filtro rápido universal
        ListAccelerator.install(self)

        # Dados iniciais
        self.carregar_lancamentos(); self.profile_selector.setCurrentText("Cleuber Marcos")
        self.carregar_planejamento(); self._load_lanc_filter_settings()

    def _ajustar_colunas_lanc(self):
        """Cada coluna respeita seu conteúdo mínimo, e a sobra é dividida igualmente entre todas as visíveis."""
        try:
            hdr = self.tab_lanc.horizontalHeader()
            self.tab_lanc.resizeColumnsToContents()

            total_width = self.tab_lanc.viewport().width()
            visiveis = [i for i in range(self.tab_lanc.columnCount()) if not self.tab_lanc.isColumnHidden(i)]
            if not visiveis:
                return

            # largura mínima exigida pelo conteúdo
            min_widths = {i: self.tab_lanc.columnWidth(i) for i in visiveis}
            used_width = sum(min_widths.values())

            sobra = total_width - used_width
            if sobra > 0:
                extra = sobra // len(visiveis)
                for i in visiveis:
                    self.tab_lanc.setColumnWidth(i, min_widths[i] + extra)

            # garante que nunca sobre espaço vazio na direita
            hdr.setStretchLastSection(True)

        except Exception as e:
            print("Erro ao ajustar colunas:", e)

    
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
        ListAccelerator.filter(self.tab_lanc, text, delay_ms=0)

    def _create_menu(self):
        mb = self.menuBar(); m1 = mb.addMenu("&Arquivo")
        m1.addAction(QAction("Novo Lançamento", self, triggered=self.novo_lancamento))
        m1.addAction(QAction("Sair", self, triggered=self.close))
        m2 = mb.addMenu("&Cadastros")
        for txt, fn in [("Imóvel Rural", lambda: self.cad_imovel()),
                        ("Conta Bancária", lambda: self.cad_conta()),
                        ("Participante", lambda: self.cad_participante()),
                        ("Cultura", lambda: QMessageBox.information(self, "Cultura", "Em desenvolvimento"))]:
            m2.addAction(QAction(txt, self, triggered=fn))
        m2.addAction(QAction("Parâmetros", self, triggered=self.abrir_parametros))
        m3 = mb.addMenu("&Relatórios")
        m3.addAction(QAction("Central de Relatórios…", self, triggered=self.abrir_central_relatorios))
        m3.addSeparator()
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
        tb.addAction(QAction(QIcon(os.path.join(ICONS_DIR, "relatorio.png")), "Relatórios", self, triggered=self.abrir_central_relatorios))
        tb.addAction(QAction(QIcon(os.path.join(ICONS_DIR, "report.png")), "Arquivo LCDPR", self, triggered=self.arquivo_lcdpr))
        tb.addSeparator(); tb.addWidget(QLabel("Perfil:"))
        self.profile_selector = QComboBox()
        self.profile_selector.addItems(["Cleuber Marcos", "Gilson Oliveira", "Adriana Lucia", "Lucas Laignier"])
        self.profile_selector.setCurrentText(CURRENT_PROFILE)
        self.profile_selector.currentTextChanged.connect(self.switch_profile)
        tb.addWidget(self.profile_selector)

    def _create_profile_banner(self):
        tb = QToolBar("Topo")
        tb.setMovable(False)
        tb.setIconSize(QSize(1, 1))  # sem ícones
        tb.setStyleSheet("QToolBar{background:#1B1D1E;border:0px;}")

        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        tb.addWidget(spacer)

        self._profile_banner = QLabel()
        self._profile_banner.setStyleSheet("color:#E0E0E0; font-weight:bold; padding:4px 10px;")
        self._profile_banner.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        tb.addWidget(self._profile_banner)

        self.addToolBar(Qt.TopToolBarArea, tb)
        self._update_profile_banner()

    def _update_profile_banner(self):
        user = CURRENT_USER or "—"
        self._profile_banner.setText(f"Perfil: {CURRENT_PROFILE}  |  Usuário: {user}")

    def switch_profile(self, profile: str):
        global CURRENT_PROFILE
        if profile == CURRENT_PROFILE:
            return
        CURRENT_PROFILE = profile
        self._update_profile_banner()

        # reabrir conexões
        self.db.conn.close(); self.db = Database()
        self.dashboard.db.conn.close(); self.dashboard.db = Database()

        # menor/maior data do novo perfil
        row = self.db.fetch_one("SELECT MIN(data_ord), MAX(data_ord) FROM lancamento WHERE data_ord IS NOT NULL")
        if row and row[0] and row[1]:
            _ini = QDate.fromString(str(row[0]), "yyyyMMdd")
            _fim = QDate.fromString(str(row[1]), "yyyyMMdd")
        else:
            _ini = QDate.currentDate().addMonths(-1)
            _fim = QDate.currentDate()

        # aplica datas nos filtros do dashboard e lançamentos
        self.dashboard.dt_dash_ini.setDate(_ini); self.dashboard.dt_dash_fim.setDate(_fim)
        self.dt_ini.setDate(_ini); self.dt_fim.setDate(_fim)

        # recarrega telas
        self.dashboard.load_data()
        self.carregar_lancamentos()
        self.carregar_planejamento()

        # reatualiza cadastros
        im_w = self.cadw.widget(0); im_w.db.conn.close(); im_w.db = Database(); im_w.carregar_imoveis()
        ct_w = self.cadw.widget(1); ct_w.db.conn.close(); ct_w.db = Database(); ct_w.carregar_contas()

        QMessageBox.information(self, "Perfil alterado", f"Perfil Trocado para: '{profile}'.")

    def arquivo_lcdpr(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("Arquivo LCDPR")
        dlg.setMinimumSize(400, 200)
        layout = QVBoxLayout(dlg)
    
        # Apenas opções de EXPORTAÇÃO
        btn_export_txt = QPushButton("Exportar TXT LCDPR")
        btn_export_plan = QPushButton("Exportar Planilha LCDPR")
        for btn in (btn_export_txt, btn_export_plan):
            layout.addWidget(btn)
    
        btn_export_txt.clicked.connect(lambda: self.show_export_dialog(dlg))
        btn_export_plan.clicked.connect(lambda: (dlg.accept(), self._exportar_planilha_lcdpr()))
    
        dlg.exec()
    
    def carregar_lancamentos(self):
        # 1) Consulta (rápida, indexada)
        d1_ord = int(self.dt_ini.date().toString("yyyyMMdd"))
        d2_ord = int(self.dt_fim.date().toString("yyyyMMdd"))
        q = """
        SELECT
            l.id,
            CASE
                WHEN instr(l.data, '/') > 0
                    THEN substr(l.data, 1, 2) || '/' || substr(l.data, 4, 2) || '/' || substr(l.data, 7, 4)
                ELSE strftime('%d/%m/%Y', l.data)
            END AS data,
            i.nome_imovel,
            l.num_doc,
            p.nome,
            l.historico,
            CASE l.tipo_lanc WHEN 1 THEN 'Receita' WHEN 2 THEN 'Despesa' ELSE 'Adiantamento' END AS tipo,
            l.valor_entrada,
            l.valor_saida,
            (l.saldo_final * CASE l.natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) AS saldo,
            l.usuario
        FROM lancamento l
        JOIN imovel_rural i       ON l.cod_imovel = i.id
        LEFT JOIN participante p  ON l.id_participante = p.id
        WHERE l.data_ord BETWEEN ? AND ?
        ORDER BY l.data_ord DESC, l.id DESC
        """
        rows = self.db.fetch_all(q, [d1_ord, d2_ord])
    
        # 2) Prepara a tabela sem travar a UI
        self.tab_lanc.setSortingEnabled(False)
        self.tab_lanc.setUpdatesEnabled(False)
        self.tab_lanc.clearContents()
        self.tab_lanc.setRowCount(len(rows))
        self.tab_lanc.setUpdatesEnabled(True)
    
        # 3) Estado para carga assíncrona
        self._lanc_rows = rows
        self._lanc_fill_pos = 0
    
        # 4) Pinta um primeiro pedaço já (feedback instantâneo)
        self._fill_lanc_chunk(size=300)
    
        # 5) Agenda o resto em background (sem travar)
        QTimer.singleShot(0, self._fill_lanc_async)
    
    def _fill_lanc_async(self):
        # Ajuste o tamanho do chunk conforme a sua máquina
        self._fill_lanc_chunk(size=400)
    
    def _fill_lanc_chunk(self, size=400):
        tbl = self.tab_lanc
        rows = getattr(self, "_lanc_rows", [])
        start = getattr(self, "_lanc_fill_pos", 0)
        end = min(start + size, len(rows))
        if start >= end:
            # terminou: reativa ordenação e faz o resize de coluna no próximo tick
            tbl.setSortingEnabled(True)
            try:
                idx = self._lanc_labels.index("Usuário")
                QTimer.singleShot(0, lambda: tbl.resizeColumnToContents(idx))
            except Exception:
                pass
            ListCounter.refresh(tbl)  # <<< COLOQUE ESTA LINHA AQUI
            QTimer.singleShot(0, self.tab_lanc.resizeRowsToContents)
            return

        # Evita sinais e repaints desnecessários
        model = tbl.model()
        blocker = QSignalBlocker(model)
        tbl.setUpdatesEnabled(False)
        try:
            for r in range(start, end):
                row = rows[r]
                for c, val in enumerate(row):
                    if c == 0:
                        item = NumericItem(int(val))
                    elif c == 1:
                        item = DateItem(str(val))
                    elif c in (7, 8, 9):  # valores
                        num = float(val or 0)
                        br = f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                        item = NumericItem(num, f"R$ {br}")
                    else:
                        item = QTableWidgetItem("" if val is None else str(val))
                    item.setTextAlignment(Qt.AlignCenter)
                    if c == 7:
                        item.setForeground(QColor("#27ae60"))
                    elif c == 8:
                        item.setForeground(QColor("#e74c3c"))
                    elif c == 9:
                        v = float(val or 0)
                        item.setForeground(QColor("#27ae60" if v >= 0 else "#e74c3c"))
                    tbl.setItem(r, c, item)
    
                # já constroi cache de filtro desta linha → busca instantânea
                ListAccelerator._ensure_row_cache(tbl, r)
        finally:
            tbl.setUpdatesEnabled(True)
    
        self._lanc_fill_pos = end
        # Continua no próximo “tick” até completar tudo
        QTimer.singleShot(0, self._fill_lanc_async)
    
    def _warm_lanc_cache_async(self, chunk=400):
        tbl = self.tab_lanc
        total = tbl.rowCount()
        if total == 0:
            return
        tbl._cache_pos = 0

        def step():
            start = tbl._cache_pos
            end = min(start + chunk, total)
            tbl.setUpdatesEnabled(False)
            try:
                for r in range(start, end):
                    ListAccelerator._ensure_row_cache(tbl, r)
            finally:
                tbl.setUpdatesEnabled(True)
            tbl._cache_pos = end
            if end < total:
                QTimer.singleShot(0, step)

        QTimer.singleShot(0, step)

    def _sort_lanc_by_column(self, col: int):
        # Alterna entre asc/desc por coluna
        order = self._lanc_sort_state.get(col, Qt.DescendingOrder)
        order = Qt.AscendingOrder if order == Qt.DescendingOrder else Qt.DescendingOrder
        self._lanc_sort_state[col] = order
        self.tab_lanc.sortItems(col, order)
        self.tab_lanc.horizontalHeader().setSortIndicator(col, order)

    def editar_lancamento(self):
        row = self.tab_lanc.currentRow(); lanc_id = int(self.tab_lanc.item(row,0).text())
        dlg = LancamentoDialog(self, lanc_id)
        if dlg.exec(): self.carregar_lancamentos(); self.dashboard.load_data()

    def excluir_lancamento(self):
        indices = self.tab_lanc.selectionModel().selectedRows()
        if not indices: return
        ids = [int(self.tab_lanc.item(idx.row(), 0).text()) for idx in indices]
        qtd = len(ids)
        resp = QMessageBox.question(self, "Confirmar Exclusão", f"Deseja excluir {qtd} lançamento(s)?", QMessageBox.Yes | QMessageBox.No)
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

    def gerar_txt(self, path: str):
        import os, re, unicodedata
        from decimal import Decimal, ROUND_HALF_UP
        from PySide6.QtCore import QSettings
        from PySide6.QtWidgets import QMessageBox

        settings = profile_settings()  # helper abaixo

        # ===== helpers =====
        NL = "\r\n"  # CRLF exigido pelo manual

        def _digits(s): return re.sub(r"\D", "", str(s or ""))

        def _ddmmyyyy(qdate_or_str):
            s = str(qdate_or_str or "")
            if hasattr(qdate_or_str, "toString"):
                return qdate_or_str.toString("ddMMyyyy")
            m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
            if m: return f"{m.group(3)}{m.group(2)}{m.group(1)}"
            m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
            if m: return f"{m.group(1)}{m.group(2)}{m.group(3)}"
            return ""

        def _cents(val):
            # N 19,2 sem separador; para zero, emitir "000" (aceito pelo validador)
            q = Decimal(str(val or 0)).quantize(Decimal("0.01"))
            v = int(q * 100)
            return "000" if v == 0 else str(v)

        def _clean(s):
            s = re.sub(r"[|\r\n]+", " ", str(s or "")).strip()
            s = s.replace("—","-").replace("º","o").replace("ª","a")
            return unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")

        def _perc5(v):
            try:
                d = Decimal(str(v).replace(",", "."))
            except Exception:
                d = Decimal("0")
            return f"{int((d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)*100).to_integral_value()):05d}"

        def _num112(v):
            q = Decimal(str(v or 0)).quantize(Decimal("0.01"))
            return f"{int(q*100):011d}"

        # ===== cabeçalho =====
        versao = settings.value("param/version", "0013")
        ident  = _digits(settings.value("param/ident", ""))

        if not ident:
            row = self.db.fetch_one(
                "SELECT cpf_cnpj FROM participante "
                "WHERE LENGTH(REPLACE(REPLACE(REPLACE(cpf_cnpj,'.',''),'-',''),'/',''))=11 "
                "ORDER BY id LIMIT 1"
            )
            if row: ident = _digits(row[0])

        nome         = _clean(settings.value("param/nome", ""))
        ind_ini_per  = settings.value("param/ind_ini_per", "0")
        sit_especial = settings.value("param/sit_especial", "0")

        dt_ini_txt = self.dt_ini.date().toString("ddMMyyyy")
        dt_fim_txt = self.dt_fim.date().toString("ddMMyyyy")
        if ind_ini_per == "0":  # Regular deve iniciar em 01/01/AAAA
            ano = self.dt_ini.date().toString("yyyy")
            dt_ini_txt = f"0101{ano}"

        if not ident:
            QMessageBox.warning(self, "LCDPR",
                "Informe o CPF do declarante em Configurações > Declarante ou cadastre um participante Pessoa Física.")
            return

        with open(path, "w", encoding="utf-8", newline="") as f:
            # 0000
            f.write("0000|" + "|".join([
                "LCDPR", versao, _digits(ident), _clean(nome),
                ind_ini_per, sit_especial, "", dt_ini_txt, dt_fim_txt
            ]) + NL)

            # 0010
            f.write("0010|1" + NL)

            # 0030 – endereço (IBGE 7, CEP 8)
            logradouro  = _clean(settings.value("param/logradouro", ""))
            numero      = _clean(settings.value("param/numero", ""))
            complemento = _clean(settings.value("param/complemento", ""))
            bairro      = _clean(settings.value("param/bairro", ""))
            uf          = (_clean(settings.value("param/uf", "")) or "")[:2].upper()
            cod_mun     = _digits(settings.value("param/cod_mun", "")).zfill(7)
            cep         = _digits(settings.value("param/cep", "")).zfill(8)
            telefone    = _digits(settings.value("param/telefone", ""))
            email       = _clean(settings.value("param/email", ""))

            if not logradouro:
                raise ValueError("Endereço (logradouro) obrigatório para o registro 0030.")

            f.write("0030|" + "|".join([
                logradouro, _digits(numero), complemento, bairro,
                uf, cod_mun, cep, telefone, email
            ]) + NL)

            # 0040 – imóveis (EXATOS 17 campos, sem áreas)
            for (cod, pais, moeda, cad_itr, caepf, ie, nome_imovel, end, num, comp,
                 bai, uf_, mun, cep_, tipo_expl, part, _area_tot, _area_uti) in self.db.fetch_all("""
                SELECT cod_imovel,pais,moeda,cad_itr,caepf,insc_estadual,
                       nome_imovel,endereco,num,compl,bairro,uf,cod_mun,cep,
                       tipo_exploracao,participacao,area_total,area_utilizada
                  FROM imovel_rural
            """):
                f.write("0040|" + "|".join([
                    _digits(cod).zfill(3),
                    _clean(pais),
                    _clean(moeda),
                    _digits(cad_itr).zfill(8),     # CAD_ITR N=8
                    _digits(caepf).zfill(14),      # CAEPF N=14
                    _digits(ie),                   # IE (opcional)
                    _clean(nome_imovel),
                    _clean(end),
                    _clean(num),
                    _clean(comp),
                    _clean(bai),
                    _clean(uf_).upper()[:2],
                    _digits(mun).zfill(7),
                    _digits(cep_).zfill(8),
                    str(tipo_expl or ""),
                    _perc5(part or 0)
                ]) + NL)

            # 0050 – contas (AG=4, CONTA=16)
            for cod, pais_cta, banco, nome_bco, ag, num_cta in self.db.fetch_all(
                "SELECT cod_conta,pais_cta,banco,nome_banco,agencia,num_conta FROM conta_bancaria"):
                ag_d  = _digits(ag)
                cta_d = _digits(num_cta)
                if not ag_d or not cta_d:
                    continue
                f.write("0050|" + "|".join([
                    _digits(cod).zfill(3),
                    _clean(pais_cta),
                    (_digits(banco).zfill(3) if banco else ""),
                    _clean(nome_bco),
                    ag_d.zfill(4),
                    cta_d.zfill(16)
                ]) + NL)

            # Q100 – lançamentos
            for (data, cod_im, cod_ct, doc, td, hist, cpf_cnpj, tl, ent, sai, sf, nat) in self.db.fetch_all("""
                SELECT l.data,
                       im.cod_imovel,
                       ct.cod_conta,
                       l.num_doc, l.tipo_doc, l.historico, p.cpf_cnpj, l.tipo_lanc,
                       l.valor_entrada, l.valor_saida, l.saldo_final, l.natureza_saldo
                  FROM lancamento l
                  JOIN imovel_rural  im ON im.id = l.cod_imovel
                  JOIN conta_bancaria ct ON ct.id = l.cod_conta
             LEFT JOIN participante      p ON p.id = l.id_participante
              ORDER BY l.data_ord, l.id
            """):
                f.write("Q100|" + "|".join([
                    _ddmmyyyy(data),
                    _digits(cod_im).zfill(3),
                    _digits(cod_ct).zfill(3),
                    _clean(doc),
                    str(td or ""),
                    _clean(hist),
                    _digits(cpf_cnpj or ident),
                    str(tl or ""),
                    _cents(ent or 0),
                    _cents(sai or 0),
                    _cents(sf or 0),      # aqui pode ser informado >=0; natureza no campo seguinte
                    str(nat or "")
                ]) + NL)

            # Q200 – resumo mensal (mmaaaa) com saldo ACUMULADO **ABSOLUTO** + NAT_SLD_FIN
            d1 = int(self.dt_ini.date().toString("yyyyMMdd"))
            d2 = int(self.dt_fim.date().toString("yyyyMMdd"))
            resumo = self.db.fetch_all("""
                SELECT substr(CAST(data_ord AS TEXT),1,6) AS ym,
                       SUM(valor_entrada), SUM(valor_saida)
                  FROM lancamento
                 WHERE data_ord BETWEEN ? AND ?
              GROUP BY ym ORDER BY ym
            """, (d1, d2))

            saldo_acum = Decimal("0")
            for ym, tot_ent, tot_sai in resumo:
                mesano  = ym[4:6] + ym[0:4]                 # mmaaaa
                tot_ent = Decimal(str(tot_ent or 0))
                tot_sai = Decimal(str(tot_sai or 0))
                saldo_acum += (tot_ent - tot_sai)
                nat = 'P' if saldo_acum >= 0 else 'N'
                f.write(
                    f"Q200|{mesano}|{_cents(tot_ent)}|{_cents(tot_sai)}|{_cents(abs(saldo_acum))}|{nat}{NL}"
                )

        # 9999 – **7 campos**: REG, IDENT_NOM, IDENT_CPF_CNPJ, IND_CRC, EMAIL, FONE, QTD_LIN
        total = sum(1 for _ in open(path, "r", encoding="utf-8")) + 1
        with open(path, "a", encoding="utf-8", newline="") as f:
            f.write(f"9999||||||{total}{NL}")

        try:
            save_last_txt_path(path)
        except Exception:
            pass

        QMessageBox.information(self, "Sucesso", f"Arquivo {os.path.basename(path)} gerado!")

    def _exportar_planilha_lcdpr(self):
        """
        Exporta TODOS os lançamentos do período selecionado em uma planilha Excel,
        com as colunas solicitadas e mapeamentos legíveis de tipo de doc e tipo de lançamento.
        Layout:
        DATA | IMOVEL RURAL | CONTA BANCARIA | CODIGO DA CONTA | PARTICIPANTE | CPF/CNPJ |
        NUMERO DO DOCUMENTO | TIPO | HISTORICO | TIPO DE LANÇAMENTO | VALOR ENTRADA | VALOR SAIDA
        """
        from PySide6.QtWidgets import QFileDialog, QMessageBox
        import pandas as pd

        # 1) Pergunta onde salvar
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Salvar Planilha LCDPR",
            load_last_txt_path(),
            "Excel (*.xlsx *.xls)"
        )
        if not path:
            return
        if not path.lower().endswith(('.xlsx', '.xls')):
            path += '.xlsx'

        # 2) Intervalo de datas → **usar data_ord** (indexado e consistente)
        d1_ord = int(self.dt_ini.date().toString("yyyyMMdd"))
        d2_ord = int(self.dt_fim.date().toString("yyyyMMdd"))

        # 3) Mapeamentos legíveis
        map_tipo_doc = {
            1: "Nota Fiscal",
            2: "Recibo",
            3: "Boleto",
            4: "Fatura",
            5: "Folha",
            6: "Outros",
        }
        map_tipo_lanc = {
            1: "Receita",
            2: "Despesa",
            3: "Adiantamento",
        }

        # 4) Busca no banco – agora filtra por data_ord
        rows = self.db.fetch_all("""
            SELECT
                CASE
                    WHEN instr(l.data,'/') > 0
                        THEN substr(l.data,1,2) || '/' || substr(l.data,4,2) || '/' || substr(l.data,7,4)
                    ELSE strftime('%d/%m/%Y', l.data)
                END                                   AS data_fmt,
                ir.cod_imovel                         AS cod_imovel,
                COALESCE(cb.nome_banco, '')           AS conta_bancaria,
                COALESCE(cb.cod_conta, '')            AS cod_conta,
                COALESCE(p.nome, '')                  AS participante,
                COALESCE(p.cpf_cnpj, '')              AS cpf_cnpj,
                COALESCE(l.num_doc, '')               AS num_doc,
                l.tipo_doc                            AS tipo_doc,
                COALESCE(l.historico, '')             AS historico,
                l.tipo_lanc                           AS tipo_lanc,
                COALESCE(l.valor_entrada, 0)          AS valor_entrada,
                COALESCE(l.valor_saida, 0)            AS valor_saida
            FROM lancamento l
            LEFT JOIN imovel_rural    ir ON ir.id = l.cod_imovel
            LEFT JOIN conta_bancaria  cb ON cb.id = l.cod_conta
            LEFT JOIN participante    p  ON p.id  = l.id_participante
            WHERE l.data_ord BETWEEN ? AND ?
            ORDER BY l.data_ord, l.id
        """, [d1_ord, d2_ord])

        if not rows:
            QMessageBox.information(self, "Exportar Planilha LCDPR",
                                    "Nenhum lançamento encontrado no período selecionado.")
            return

        # 5) Converte para DataFrame com os cabeçalhos pedidos
        data = []
        for (data_fmt, cod_imovel, conta_bancaria, cod_conta, participante, cpf_cnpj,
             num_doc, tipo_doc, historico, tipo_lanc, valor_entrada, valor_saida) in rows:

            tipo_doc_desc  = map_tipo_doc.get(int(tipo_doc) if tipo_doc is not None else 0, "")
            tipo_lanc_desc = map_tipo_lanc.get(int(tipo_lanc) if tipo_lanc is not None else 0, "")

            data.append({
                "DATA":                data_fmt or "",
                "IMOVEL RURAL":        cod_imovel or "",
                "CONTA BANCARIA":      conta_bancaria,
                "CODIGO DA CONTA":     cod_conta,
                "PARTICIPANTE":        participante,
                "CPF/CNPJ":            cpf_cnpj,
                "NUMERO DO DOCUMENTO": num_doc,
                "TIPO":                tipo_doc_desc,
                "HISTORICO":           historico,
                "TIPO DE LANÇAMENTO":  tipo_lanc_desc,
                "VALOR ENTRADA":       float(valor_entrada or 0),
                "VALOR SAIDA":         float(valor_saida or 0),
            })

        df = pd.DataFrame(data, columns=[
            "DATA",
            "IMOVEL RURAL",
            "CONTA BANCARIA",
            "CODIGO DA CONTA",
            "PARTICIPANTE",
            "CPF/CNPJ",
            "NUMERO DO DOCUMENTO",
            "TIPO",
            "HISTORICO",
            "TIPO DE LANÇAMENTO",
            "VALOR ENTRADA",
            "VALOR SAIDA",
        ])

        # 6) Salva no Excel com largura razoável
        try:
            with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name="LCDPR")
                ws = writer.sheets["LCDPR"]
                for idx, col in enumerate(df.columns, start=1):
                    col_width = max(12, min(60, int(df[col].astype(str).str.len().quantile(0.90)) + 4))
                    ws.set_column(idx-1, idx-1, col_width)
        except Exception as e:
            QMessageBox.critical(self, "Exportar Planilha LCDPR", f"Erro ao salvar planilha:\n{e}")
            return

        QMessageBox.information(self, "Exportar Planilha LCDPR",
                                f"Planilha gerada com sucesso em:\n{path}")

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
            "Sistema AgroContábil - LCDPR\n\nVersão: 1.0\n© 2025 Automatize Tech\n\n"
            "Funcionalidades:\n- Gestão de propriedades rurais\n- Controle financeiro completo\n"
            "- Planejamento de safras\n- Gerenciamento de estoque\n- Geração do LCDPR")

    def abrir_central_relatorios(self):
        dlg = ReportCenterDialog(self, d_ini=self.dt_ini.date(), d_fim=self.dt_fim.date())
        dlg.exec()

    def importar_lancamentos(self):
        path, _ = QFileDialog.getOpenFileName(self, "Importar Lançamentos", "", "TXT (*.txt);;Excel (*.xlsx *.xls)")
        if not path: return
        try:
            self._import_lancamentos_txt(path) if path.lower().endswith('.txt') else self._import_lancamentos_excel(path)
            self.carregar_lancamentos(); self.dashboard.load_data()
        except Exception as e:
            QMessageBox.warning(self, "Importação Falhou", f"Arquivo não segue o layout esperado:\n{e}")

    def _extract_name_from_historico(historico: str) -> str:
        """Retorna o texto dentro do último parêntese no histórico, ou ''."""
        import re
        if not historico:
            return ""
        m = re.findall(r"\(([^)]+)\)", historico)
        return (m[-1].strip() if m else "")

    def _ensure_participante(self, digits: str, historico: str = "") -> int:
        """
        Garante que o participante (CPF/CNPJ) exista.
        - CNPJ: consulta Receita e usa razão/fantasia como nome.
        - CPF: usa o nome entre parênteses do histórico; se vazio, tenta Receita.
        Retorna id do participante.
        """
        # já existe?
        row = self.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj=?", (digits,))
        if row:
            return row[0]

        is_pf = (len(digits) == 11)
        tipo_contraparte = 2 if is_pf else 1  # 1=Pessoa Jurídica, 2=Pessoa Física
        nome = ""

        if is_pf:
            # 1) tenta pegar do histórico (RENATA ... dentro de parênteses)
            nome = _extract_name_from_historico(historico)
            # 2) se não tiver no histórico, tenta Receita (se disponível)
            if not nome:
                try:
                    info = consulta_receita(digits, tipo='cpf')
                    nome = (info.get('nome') or "").strip()
                except Exception:
                    pass
            if not nome:
                nome = f"CPF {digits}"
        else:
            # CNPJ -> sempre tenta Receita para vir com razão/fantasia
            try:
                info = consulta_receita(digits, tipo='cnpj')
                nome = _nome_cnpj_from_receita(info)
            except Exception:
                pass
            if not nome:
                # Fallback (só se a API falhar)
                nome = f"CNPJ {digits}"

        cur = self.db.execute_query(
            "INSERT INTO participante (cpf_cnpj, nome, tipo_contraparte) VALUES (?,?,?)",
            [digits, nome, tipo_contraparte]
        )
        return cur.lastrowid

    def _broadcast_participantes_changed(self):
        """Pede para todas as janelas/diálogos recarregarem a lista de participantes."""
        try:
            from PySide6.QtWidgets import QApplication, QDialog
            for top in QApplication.topLevelWidgets():
                for dlg in top.findChildren(QDialog):
                    if hasattr(dlg, "_reload_participantes"):
                        try:
                            dlg._reload_participantes()
                        except Exception:
                            pass
        except Exception:
            pass

    def _import_lancamentos_txt(self, path):
        import re
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        usuario_ts = f"{CURRENT_USER} dia {now}"

        def _parse_cent(v: str) -> float:
            s = re.sub(r'\D', '', (v or ''))
            return (int(s) / 100.0) if s else 0.0

        def _norms(code: str):
            s = (code or '').strip()
            if not s:
                return []
            out = [s]
            if s.isdigit():
                out += [s.zfill(3), (s.lstrip('0') or '0')]
            return list(dict.fromkeys(out))

        # ---- contagem de linhas para configurar o progresso ----
        with open(path, encoding='utf-8') as _f:
            total = sum(1 for _ in _f)

        # ---- caches para acelerar lookups de FK/participante/saldo por conta ----
        im_cache = {}      # cod_imovel_normalizado -> id_imovel
        ct_cache = {}      # cod_conta_normalizado  -> id_conta
        part_cache = {}    # cpf_cnpj_digits        -> id_participante
        saldos = {}        # id_conta -> saldo atual (considerando natureza)

        GlobalProgress.begin("Importando lançamentos (TXT)…", maximo=total, parent=self.window())
        try:
            with self.db.bulk():
                with open(path, encoding='utf-8') as f:
                    for lineno, line in enumerate(f, 1):
                        parts = line.strip().split("|")

                        # Layout 1 (11 colunas) -> YYYY-MM-DD | ... | participante | ...
                        if len(parts) == 11 and re.match(r"\d{4}-\d{2}-\d{2}$", parts[0]):
                            (data_iso, cod_imovel, cod_conta, num_doc, raw_tipo_doc, historico,
                             participante_raw, tipo_lanc_raw, raw_ent, raw_sai, _) = parts

                            y, m, d = data_iso.split("-")
                            data_str = f"{d}/{m}/{y}"
                            data_ord = int(f"{y}{m}{d}")  # AAAAMMDD
                            tipo_doc = int(raw_tipo_doc) if (raw_tipo_doc or "").strip().isdigit() else 4
                            ent = float(raw_ent.replace(",", ".")) if raw_ent else 0.0
                            sai = float(raw_sai.replace(",", ".")) if raw_sai else 0.0

                            # Participante: CPF/CNPJ ou ID
                            id_participante = None
                            digits = re.sub(r"\D", "", participante_raw or "")
                            if digits and len(digits) in (11, 14):
                                if digits in part_cache:
                                    id_participante = part_cache[digits]
                                else:
                                    row = self.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj=?", (digits,))
                                    id_participante = row[0] if row else self._ensure_participante(digits, historico)
                                    part_cache[digits] = id_participante
                            elif (participante_raw or "").isdigit():
                                id_participante = int(participante_raw)

                            tipo_lanc = int(tipo_lanc_raw) if (tipo_lanc_raw or "").isdigit() else (1 if sai > 0 else 2)

                        # Layout 2 (12 colunas) -> DD-MM-AAAA | ... | cpf/cnpj | ...
                        elif len(parts) == 12 and re.match(r"\d{2}-\d{2}-\d{4}$", parts[0]):
                            (data_br, cod_imovel, cod_conta, num_doc, raw_tipo_doc, historico,
                             cpf_cnpj_raw, tipo_lanc_raw, cent_ent, cent_sai, _cent_saldo, _nat_raw) = parts

                            d, m, y = data_br.split("-")
                            data_str = f"{d}/{m}/{y}"
                            data_ord = int(f"{y}{m}{d}")  # AAAAMMDD
                            tipo_doc = int(raw_tipo_doc) if (raw_tipo_doc or "").strip().isdigit() else 4
                            ent = _parse_cent(cent_ent)
                            sai = _parse_cent(cent_sai)

                            id_participante = None
                            digits = re.sub(r"\D", "", cpf_cnpj_raw or "")
                            if digits and len(digits) in (11, 14):
                                if digits in part_cache:
                                    id_participante = part_cache[digits]
                                else:
                                    row = self.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj=?", (digits,))
                                    id_participante = row[0] if row else self._ensure_participante(digits, historico)
                                    part_cache[digits] = id_participante

                            tipo_lanc = int(tipo_lanc_raw) if (tipo_lanc_raw or "").isdigit() else (1 if sai > 0 else 2)

                        else:
                            raise ValueError(f"Linha {lineno}: formato não reconhecido ({len(parts)} colunas)")

                        # Heurísticas de tipo_doc/categoria
                        categoria = None
                        desc = (historico or "").upper()
                        if any(k in desc for k in ("FOLHA DE PAGAMENTO", "IRRF", "FGTS", "INSS", "FOLHA")):
                            tipo_doc = 5; categoria = "Folha"
                        elif any(k in desc for k in ("TALAO", "TALÃO", "ENERGIA")):
                            tipo_doc = 4; categoria = "Fatura"

                        # FK imóvel (normalização 1/01/001) com cache
                        id_imovel = None
                        for c in _norms(cod_imovel):
                            if c in im_cache:
                                id_imovel = im_cache[c]
                                break
                            row = self.db.fetch_one("SELECT id FROM imovel_rural WHERE cod_imovel=?", (c,))
                            if row:
                                id_imovel = row[0]
                                # guarda no cache para todas as variantes normalizadas
                                for alt in _norms(cod_imovel):
                                    im_cache[alt] = id_imovel
                                break
                        if not id_imovel:
                            raise ValueError(f"Linha {lineno}: imóvel '{cod_imovel}' não encontrado")

                        # FK conta (normalização 1/01/001) com cache
                        id_conta = None
                        for c in _norms(cod_conta):
                            if c in ct_cache:
                                id_conta = ct_cache[c]
                                break
                            row = self.db.fetch_one("SELECT id FROM conta_bancaria WHERE cod_conta=?", (c,))
                            if row:
                                id_conta = row[0]
                                for alt in _norms(cod_conta):
                                    ct_cache[alt] = id_conta
                                break
                        if not id_conta:
                            raise ValueError(f"Linha {lineno}: conta '{cod_conta}' não encontrada")

                        # Saldo/natureza por conta (consulta 1x e mantém acumulado)
                        if id_conta not in saldos:
                            last = self.db.fetch_one(
                                "SELECT (saldo_final * CASE natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) "
                                "FROM lancamento WHERE cod_conta=? ORDER BY id DESC LIMIT 1",
                                (id_conta,)
                            )
                            saldos[id_conta] = last[0] if last and last[0] is not None else 0.0

                        saldo_ant = saldos[id_conta]
                        saldo_f = saldo_ant + ent - sai
                        saldos[id_conta] = saldo_f  # atualiza para próxima linha dessa conta
                        nat = 'P' if saldo_f >= 0 else 'N'

                        self.db.execute_query(
                            """INSERT INTO lancamento (
                                   data, cod_imovel, cod_conta, num_doc, tipo_doc, historico,
                                   id_participante, tipo_lanc, valor_entrada, valor_saida,
                                   saldo_final, natureza_saldo, usuario, categoria, data_ord
                               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            [data_str, id_imovel, id_conta, ((num_doc or '').strip() or None), tipo_doc, historico,
                             id_participante, int(tipo_lanc), ent, sai, abs(saldo_f), nat, usuario_ts, categoria, data_ord]
                        )
                        if lineno % 200 == 0:
                            GlobalProgress.set_value(lineno)

                GlobalProgress.set_value(total)
        finally:
            GlobalProgress.end()

        # terminou: atualiza listas/combos de participantes nas janelas abertas
        self._broadcast_participantes_changed()

    def _import_lancamentos_excel(self, path):
        import re
        df = pd.read_excel(path, dtype=str)

        required = ['data','cod_imovel','cod_conta','num_doc','tipo_doc','historico','tipo_lanc','valor_entrada','valor_saida','categoria']
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Colunas faltando no Excel: {', '.join(missing)}")

        has_pid = 'id_participante' in df.columns
        has_doc = 'cpf_cnpj' in df.columns
        if not (has_pid or has_doc):
            raise ValueError("Planilha deve ter 'id_participante' ou 'cpf_cnpj'.")

        df.fillna('', inplace=True)

        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        usuario_ts = f"{CURRENT_USER} dia {now}"

        def _norms(code: str):
            s = (code or '').strip()
            if not s:
                return []
            out = [s]
            if s.isdigit():
                out += [s.zfill(3), (s.lstrip('0') or '0')]
            return list(dict.fromkeys(out))

        total = len(df.index)

        # ---- caches e saldos por conta (mesma lógica do TXT) ----
        im_cache = {}
        ct_cache = {}
        part_cache = {}
        saldos = {}

        GlobalProgress.begin("Importando lançamentos (Excel)…", maximo=total, parent=self.window())
        try:
            with self.db.bulk():
                for lineno, row in enumerate(df.itertuples(index=False), start=2):
                    # FK Imóvel (com cache)
                    id_imovel = None
                    for c in _norms(getattr(row, 'cod_imovel', '')):
                        if c in im_cache:
                            id_imovel = im_cache[c]; break
                        r = self.db.fetch_one("SELECT id FROM imovel_rural WHERE cod_imovel=?", (c,))
                        if r:
                            id_imovel = r[0]
                            for alt in _norms(getattr(row, 'cod_imovel', '')):
                                im_cache[alt] = id_imovel
                            break
                    if not id_imovel:
                        raise ValueError(f"Linha {lineno}: imóvel '{row.cod_imovel}' não encontrado")

                    # FK Conta (com cache)
                    id_conta = None
                    for c in _norms(getattr(row, 'cod_conta', '')):
                        if c in ct_cache:
                            id_conta = ct_cache[c]; break
                        r = self.db.fetch_one("SELECT id FROM conta_bancaria WHERE cod_conta=?", (c,))
                        if r:
                            id_conta = r[0]
                            for alt in _norms(getattr(row, 'cod_conta', '')):
                                ct_cache[alt] = id_conta
                            break
                    if not id_conta:
                        raise ValueError(f"Linha {lineno}: conta '{row.cod_conta}' não encontrada")

                    # Participante
                    pid = None
                    if has_pid and str(getattr(row, 'id_participante', '')).strip().isdigit():
                        pid = int(getattr(row, 'id_participante'))
                    elif has_doc:
                        digits = re.sub(r'\D', '', str(getattr(row, 'cpf_cnpj', '')))
                        if digits:
                            if digits in part_cache:
                                pid = part_cache[digits]
                            else:
                                r = self.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj=?", (digits,))
                                pid = r[0] if r else self._ensure_participante(digits, getattr(row, 'historico', ''))
                                part_cache[digits] = pid

                    # Valores
                    ent = float(row.valor_entrada or 0)
                    sai = float(row.valor_saida or 0)
                    tipo_doc = int(row.tipo_doc) if str(row.tipo_doc).strip().isdigit() else 4
                    tipo_lanc = int(row.tipo_lanc) if str(row.tipo_lanc).strip().isdigit() else (1 if sai > 0 else 2)

                    # Saldo/natureza por conta (consulta 1x e mantém acumulado)
                    if id_conta not in saldos:
                        last = self.db.fetch_one(
                            "SELECT (saldo_final * CASE natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) "
                            "FROM lancamento WHERE cod_conta=? ORDER BY id DESC LIMIT 1",
                            (id_conta,)
                        )
                        saldos[id_conta] = last[0] if last and last[0] is not None else 0.0

                    saldo_ant = saldos[id_conta]
                    saldo_f = saldo_ant + ent - sai
                    saldos[id_conta] = saldo_f
                    nat = 'P' if saldo_f >= 0 else 'N'
                    # row.data no formato DD/MM/AAAA
                    dd, mm, yyyy = str(row.data).split("/")
                    data_ord = int(f"{yyyy}{mm}{dd}")  # AAAAMMDD

                    self.db.execute_query(
                        """INSERT INTO lancamento (
                               data, cod_imovel, cod_conta, num_doc, tipo_doc, historico,
                               id_participante, tipo_lanc, valor_entrada, valor_saida,
                               saldo_final, natureza_saldo, usuario, categoria, data_ord
                           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        [data_str, id_imovel, id_conta, ((num_doc or '').strip() or None), tipo_doc, historico,
                         id_participante, int(tipo_lanc), ent, sai, abs(saldo_f), nat, usuario_ts, categoria, data_ord]
                    )
                    if (lineno - 1) % 200 == 0:
                        GlobalProgress.set_value(lineno - 1)

            GlobalProgress.set_value(total)
        finally:
            GlobalProgress.end()

        # terminou: atualiza listas/combos de participantes nas janelas abertas
        self._broadcast_participantes_changed()

    # =====================
    # Importação (modal)
    # =====================
    def show_import_dialog(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("Importação")
        lay = QVBoxLayout(dlg)

        # Botões
        btn_danfe = QPushButton("Importar DANFE (Fiscal.io)")
        btn_folha = QPushButton("Importar folha de pagamento")
        btn_cte   = QPushButton("Importar CTe")
        btn_talao = QPushButton("Importar Talão")
        btn_scan  = QPushButton("Importar notas digitalizadas")

        # Ações
        def _open_danfe():
            dlg.accept()
            self.open_importador_danfe_tab()

        def _open_talao():
            dlg.accept()
            self.open_automacao_energia_tab()

        def _placeholder(msg):
            QMessageBox.information(self, "Em breve", f"{msg} — funcionalidade em desenvolvimento.")

        btn_danfe.clicked.connect(_open_danfe)
        btn_folha.clicked.connect(lambda: (dlg.accept(), self.open_automacao_folha_tab()))
        btn_cte.clicked.connect(lambda: (dlg.accept(), self.open_importador_cte_tab()))
        btn_talao.clicked.connect(_open_talao)
        btn_scan.clicked.connect(lambda: (dlg.accept(), self.open_nfs_digitalizadas_tab()))

        for b in (btn_danfe, btn_folha, btn_cte, btn_talao, btn_scan):
            b.setFixedHeight(34)
            lay.addWidget(b)

        dlg.exec()

    def open_importador_danfe_tab(self):
        # Evita duplicar a aba
        for i in range(self.tabs.count()):
            w = self.tabs.widget(i)
            if w and getattr(w, 'objectName', lambda: '')() == 'tab_import_danfe':
                self.tabs.setCurrentIndex(i)
                return

        try:
            mod = self._load_importador_danfe_module()
            importer_widget = mod.RuralXmlImporter()
            importer_widget.setObjectName('tab_import_danfe')
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Falha ao carregar Importador DANFE:\n{e}")
            return

        self.tabs.addTab(importer_widget, "Importar DANFE (Fiscal.io)")
        self.tabs.setCurrentWidget(importer_widget)

    def open_importador_cte_tab(self):
        # evita duplicar a aba
        for i in range(self.tabs.count()):
            w = self.tabs.widget(i)
            if w and getattr(w, 'objectName', lambda: '')() == 'tab_import_cte':
                self.tabs.setCurrentIndex(i)
                return
        try:
            mod = self._load_importador_cte_module()
            importer_widget = mod.ImportadorCTe(self)
            importer_widget.setObjectName('tab_import_cte')
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Falha ao carregar Importador CT-e:\n{e}")
            return
        self.tabs.addTab(importer_widget, "Importar CTe")
        self.tabs.setCurrentWidget(importer_widget)
    
    def open_automacao_folha_tab(self):
        # Evita duplicar a aba
        for i in range(self.tabs.count()):
            w = self.tabs.widget(i)
            if w and getattr(w, 'objectName', lambda: '')() == 'tab_automacao_folha':
                self.tabs.setCurrentIndex(i)
                return
        try:
            mod = self._load_automacao_folha_module()
            # Garanta que o arquivo automacao_folha.py exponha esta classe:
            folha_widget = mod.AutomacaoFolhaUI(self)
            folha_widget.setObjectName('tab_automacao_folha')
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Falha ao carregar Importação Folha:\n{e}")
            return
    
        self.tabs.addTab(folha_widget, "Folha de Pagamento")
        self.tabs.setCurrentWidget(folha_widget)
    
    def _load_importador_danfe_module(self):
        import importlib.util, os
        # Caminho padrão solicitado: ./Importação DANFE/Importador XML.py
        base = PROJECT_DIR
        preferred = os.path.join(base, "Importação DANFE", "Importador XML.py")
        fallback = os.path.join(base, "Importador XML.py")

        if not os.path.exists(preferred) and not os.path.exists(fallback):
            raise FileNotFoundError("Não encontrei o arquivo 'Importador XML.py' (ou 'Importador XML.py').")

        filepath = preferred if os.path.exists(preferred) else fallback
        spec = importlib.util.spec_from_file_location("importador_xml", filepath)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    # ISSO (inexistente) — vamos ADICIONAR:
    def _load_importador_cte_module(self):
        import importlib.util, os
        base = PROJECT_DIR
        preferred = os.path.join(base, "Importação CTe", "Importador CTe.py")
        fallback  = os.path.join(base, "Importador CTe.py")

        if not os.path.exists(preferred) and not os.path.exists(fallback):
            raise FileNotFoundError("Não encontrei o arquivo 'Importador CTe.py'.")

        filepath = preferred if os.path.exists(preferred) else fallback
        spec = importlib.util.spec_from_file_location("importador_cte", filepath)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def _load_automacao_folha_module(self):
        import importlib.util, os
        base = PROJECT_DIR
        # você pode ajustar as pastas conforme sua organização:
        preferred = os.path.join(base, "Importação Folha", "automacao_folha.py")
        fallback  = os.path.join(base, "automacao_folha.py")

        if not os.path.exists(preferred) and not os.path.exists(fallback):
            raise FileNotFoundError("Não encontrei o arquivo 'automacao_folha.py'.")

        filepath = preferred if os.path.exists(preferred) else fallback
        spec = importlib.util.spec_from_file_location("automacao_folha", filepath)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    # INSIRA DEPOIS DE open_importador_danfe_tab():
    def _load_automacao_energia_module(self):
        """
        Carrega o módulo da Automação de Energia (como fazemos com o Importador DANFE).
        Ajuste o caminho abaixo conforme onde você salvar o arquivo automacao_energia.py.
        """
        import importlib.util, os
        # Sugestão de caminho no seu projeto:
        mod_path = os.path.join(os.path.dirname(__file__), "Importação Energia", "automacao_energia.py")
        spec = importlib.util.spec_from_file_location("automacao_energia", mod_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def _load_automacao_nfs_digitalizadas_module(self):
        import importlib.util, os, sys
        mod_path = os.path.join(os.path.dirname(__file__), "Importação NFs Digitalizadas", "automacao_NFS Digitalizada.py")
        if not os.path.exists(mod_path):
            raise FileNotFoundError("Não encontrei 'Importação NFs Digitalizadas/automacao_NFS Digitalizada.py'.")
        spec = importlib.util.spec_from_file_location("automacao_nfs_digitalizadas", mod_path)
        mod = importlib.util.module_from_spec(spec)
        # 👇 REGISTRA antes de exec_module — evita o erro do dataclasses
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod

    def open_automacao_energia_tab(self):
        # Evita duplicar a aba
        for i in range(self.tabs.count()):
            w = self.tabs.widget(i)
            if w and getattr(w, 'objectName', lambda: '')() == 'tab_automacao_energia':
                self.tabs.setCurrentIndex(i)
                return

        try:
            mod = self._load_automacao_energia_module()
            energia_widget = mod.AutomacaoEnergiaUI()
            energia_widget.setObjectName('tab_automacao_energia')
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Falha ao carregar Automação de Energia:\n{e}")
            return

        self.tabs.addTab(energia_widget, "Automação de Energia")
        self.tabs.setCurrentWidget(energia_widget)

    def open_nfs_digitalizadas_tab(self):
        TAB_OBJ_NAME = 'tab_automacao_nfs_digitalizadas'
        TAB_TITLE    = 'NFS-e Digitalizadas'

        # Foca a aba se já existir
        idx = next(
            (i for i in range(self.tabs.count())
             if getattr(self.tabs.widget(i), 'objectName', lambda: '')() == TAB_OBJ_NAME),
            -1
        )
        if idx != -1:
            self.tabs.setCurrentIndex(idx)
            return

        # Carrega o módulo e cria o widget
        try:
            mod = self._load_automacao_nfs_digitalizadas_module()
            widget = mod.AutomacaoNFSDigitalizadasUI()
            widget.setObjectName(TAB_OBJ_NAME)
        except Exception as e:
            from PySide6.QtWidgets import QMessageBox
            QMessageBox.critical(self, "Erro", f"Falha ao carregar Automação NFS-e Digitalizadas:\n{e}")
            return

        self.tabs.addTab(widget, TAB_TITLE)
        self.tabs.setCurrentWidget(widget)

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
    window.showMaximized()
    sys.exit(app.exec())