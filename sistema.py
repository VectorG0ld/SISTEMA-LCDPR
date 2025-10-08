import os
import sys
import re
import json
import csv
from supabase import create_client, Client, create_async_client, AsyncClient
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
    QStackedWidget, QTextBrowser, QSplitter, QAbstractItemView
)

from PySide6.QtCore import Qt, QDate, QSize, QCoreApplication, QTimer, QSignalBlocker, QObject, QEvent, QPoint, QSettings
from PySide6.QtGui import QFont, QIcon, QColor, QPainter, QAction
from PySide6.QtCharts import QChart, QChartView, QPieSeries, QBarSeries, QBarSet, QBarCategoryAxis, QValueAxis
from PySide6.QtPrintSupport import QPrinter

from contextlib import contextmanager
import shiboken6

from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP
import threading
import contextlib
# --- .env / chaves Supabase ---

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).with_name(".env"))
except Exception:
    pass

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA", "public")

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    raise RuntimeError("Defina SUPABASE_URL e SUPABASE_ANON_KEY no arquivo .env ou variáveis de ambiente.")

# --- cliente global Supabase (sb) ---
_SB_CLIENT: Client | None = None
def sb() -> Client:
    global _SB_CLIENT
    if _SB_CLIENT is None:
        _SB_CLIENT = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    return _SB_CLIENT

# --- Compat: sb().table(...) via HTTP direto (sem postgrest) -------------------
import requests as _rq

class _RestResp:
    def __init__(self, data): self.data = data

class _RestTable:
    def __init__(self, name: str):
        self.name = name
        self.base = f"{SUPABASE_URL}/rest/v1/{name}"
        self.headers = {
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }
        if SUPABASE_SCHEMA:
            self.headers["Accept-Profile"]  = SUPABASE_SCHEMA
            self.headers["Content-Profile"] = SUPABASE_SCHEMA
        self.params = {}
        self._select = "*"
        self._method = "GET"
        self._payload = None
        self._filters = []           # <-- NOVO: armazena (col, "op.valor")

    # ----- builders -----
    def select(self, cols: str):
        self._select = cols or "*"; return self
    def eq(self, col, val):
        self._filters.append((col, f"eq.{val}")); return self
    def gte(self, col, val):
        self._filters.append((col, f"gte.{val}")); return self
    def lte(self, col, val):
        self._filters.append((col, f"lte.{val}")); return self
    def in_(self, col, values):
        if isinstance(values, (list, tuple, set)):
            vals = ",".join(str(v) for v in values if v is not None)
        else:
            vals = str(values)
        self._filters.append((col, f"in.({vals})"))
        return self
    def lt(self, col, val): self._filters.append((col, f"lt.{val}")); return self
    def gt(self, col, val): self._filters.append((col, f"gt.{val}")); return self
    def like(self, col: str, pattern: str):
        self._filters.append((col, f"like.{pattern}"))
        return self
    def order(self, col, desc=False):
        self.params["order"] = f"{col}.{'desc' if desc else 'asc'}"; return self
    def limit(self, n: int):
        self.params["limit"] = int(n); return self
    def insert(self, payload):
        self._method = "POST"; self._payload = payload; return self
    def update(self, payload):
        self._method = "PATCH"; self._payload = payload; return self
    def upsert(self, payload, on_conflict=None):
        self._method = "POST"; self._payload = payload
        if on_conflict: self.params["on_conflict"] = on_conflict
        self.headers["Prefer"] = "resolution=merge-duplicates,return=representation"
        return self
    def delete(self):
        self._method = "DELETE"; self._payload = None; return self

    # ----- executor -----
    def execute(self):
        # começa com os params "únicos" (order, limit, on_conflict, etc.)
        params_list = list(self.params.items())
        # em GET, incluir o select
        if self._method == "GET":
            params_list.append(("select", self._select))
        # adiciona filtros repetíveis (eq/gte/lte/in)
        params_list.extend(self._filters)
    
        if   self._method == "GET":
            r = _rq.get(self.base, headers=self.headers, params=params_list, timeout=20)
        elif self._method == "POST":
            r = _rq.post(self.base, headers=self.headers, params=params_list, json=self._payload, timeout=20)
        elif self._method == "PATCH":
            r = _rq.patch(self.base, headers=self.headers, params=params_list, json=self._payload, timeout=20)
        else:
            r = _rq.request(self._method, self.base, headers=self.headers, params=params_list, json=self._payload, timeout=20)
        if r.status_code >= 400:
            raise Exception(f"PostgREST error {r.status_code}: {r.text}")
        try:
            data = r.json()
        except Exception:
            data = None
        return _RestResp(data)

def _table_proxy(self, name: str): return _RestTable(name)

Client.table = _table_proxy
Client.from_ = _table_proxy


def valida_cnpj(cnpj: str) -> bool:
    import re
    nums = re.sub(r'\D', '', cnpj or '')
    if len(nums) != 14 or nums == nums[0] * 14:
        return False
    def calc_dig(base: str, pesos: list[int]) -> str:
        soma = sum(int(d) * p for d, p in zip(base, pesos))
        resto = soma % 11
        return '0' if resto < 2 else str(11 - resto)
    d1 = calc_dig(nums[:12], [5,4,3,2,9,8,7,6,5,4,3,2])
    d2 = calc_dig(nums[:12] + d1, [6,5,4,3,2,9,8,7,6,5,4,3,2])
    return nums.endswith(d1 + d2)

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

# —————— Cache/Prefs 100% Supabase ——————
# Tabelas:
#   public.meta_kv (key text primary key, val jsonb, updated_at timestamptz default now())
# Chaves usadas:
#   receita_cache::<PROFILE>      -> json (dict)
#   last_txt_path::<PROFILE>      -> string

def kv_get(key: str, default=None):
    row = sb().table("meta_kv").select("val").eq("key", key).limit(1).execute().data
    if row:
        return row[0]["val"]
    return default

def kv_set(key: str, val):
    sb().table("meta_kv").upsert({"key": key, "val": val}).execute()

def load_last_txt_path() -> str:
    return kv_get(f"last_txt_path::{CURRENT_PROFILE}", "") or ""

def save_last_txt_path(path: str):
    kv_set(f"last_txt_path::{CURRENT_PROFILE}", path or "")

RATE_LIMIT_RETRIES = 4
RATE_LIMIT_BASE_DELAY = 2.0
MIN_INTERVAL_BETWEEN_CALLS = 1.0
_RECEITA_LAST_HIT_TS = 0.0

API_URL_CNPJ = 'https://www.receitaws.com.br/v1/cnpj/'
API_URL_CPF  = 'https://www.receitaws.com.br/v1/cpf/'

def load_cache() -> dict:
    return kv_get(f"receita_cache::{CURRENT_PROFILE}", {}) or {}

def save_cache(cache: dict):
    kv_set(f"receita_cache::{CURRENT_PROFILE}", cache or {})

def consulta_receita(cpf_cnpj: str, tipo: str = 'cnpj') -> dict:
    import time, requests
    cache = load_cache()
    key = f"{tipo}:{cpf_cnpj}"
    if key in cache:
        return cache[key]

    url = (API_URL_CPF if tipo == 'cpf' else API_URL_CNPJ) + cpf_cnpj
    global _RECEITA_LAST_HIT_TS
    for attempt in range(RATE_LIMIT_RETRIES):
        elapsed = time.time() - _RECEITA_LAST_HIT_TS
        if elapsed < MIN_INTERVAL_BETWEEN_CALLS:
            time.sleep(MIN_INTERVAL_BETWEEN_CALLS - elapsed)
        try:
            res = requests.get(url, timeout=8)
            _RECEITA_LAST_HIT_TS = time.time()
            if res.status_code == 429:
                time.sleep(RATE_LIMIT_BASE_DELAY * (2 ** attempt)); continue
            res.raise_for_status()
            data = res.json()
            cache[key] = data; save_cache(cache); return data
        except requests.RequestException:
            time.sleep(RATE_LIMIT_BASE_DELAY * (2 ** attempt)); continue
    return {"status": "ERROR", "message": "RATE_LIMIT_OR_NETWORK"}

def _nome_cnpj_from_receita(data: dict) -> str:
    if not isinstance(data, dict): return ""
    for k in ("nome","razao_social","razaosocial","razaoSocial","fantasia","nome_fantasia"):
        v = data.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
ICONS_DIR = os.path.join(PROJECT_DIR, 'banco_de_dados', 'icons')

APP_ICON    = os.path.join(ICONS_DIR, 'agro_icon.png')
LOCK_ICON = os.path.join(ICONS_DIR, 'lock.png')

# Perfil dinâmico
CURRENT_PROFILE = "Cleuber Marcos"

# Usuário que fez login
CURRENT_USER = None

def get_profile_db_filename():
    # legado removido (não há mais SQLite local)
    return ""

# ── (1) Login 100% Supabase ─────────────────────────────────────────
# Tabela esperada: public.app_users (username text PK, password text, role text)
# role: 'admin' | 'user'

def valida_usuario(username: str, password: str) -> bool:
    data = (sb().table("app_users")
              .select("password")
              .eq("username", username)
              .limit(1).execute().data)
    return bool(data and data[0]["password"] == password)

def is_admin_password(pw: str) -> bool:
    row = (sb().table("app_users")
             .select("password, role")
             .eq("username", "admin")
             .limit(1).execute().data)
    return bool(row and row[0]["role"] == "admin" and row[0]["password"] == pw)

def registrar_usuario(username: str, password: str) -> None:
    sb().table("app_users").upsert({"username": username, "password": password, "role": "user"}).execute()


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
        # valida duplicidade no Supabase
        exists = sb().table("app_users").select("username").eq("username", u).limit(1).execute().data
        if exists:
            QMessageBox.warning(self, "Erro", "Usuário já existe."); return
        # apenas fecha; inserção será feita pelo LoginDialog.try_register
        self.accept()

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
        # pede a senha de admin (validada no Supabase)
        senha, ok = QInputDialog.getText(self, "Senha de Administrador", "Digite a senha de administrador:", QLineEdit.Password)
        if not ok:
            return
        if not is_admin_password(senha):
            QMessageBox.warning(self, "Acesso negado", "Senha de administrador incorreta.")
            return

        # diálogo de registro
        dlg = RegisterUserDialog(self)
        dlg.setStyleSheet(STYLE_SHEET)
        if dlg.exec() == QDialog.Accepted:
            u = dlg.user_edit.text().strip()
            p = dlg.pw_edit.text()
            registrar_usuario(u, p)
            QMessageBox.information(self, "Sucesso", f"Usuário '{u}' cadastrado no Supabase.")


def _tuplize(rows, cols):
    """Converte list[dict] -> list[tuple] na ordem de 'cols'."""
    out = []
    for r in rows:
        out.append(tuple(r.get(c) for c in cols))
    return out

class Database:
    """
    Substituto compatível com a API antiga, porém lendo do Supabase.
    Somente leitura por enquanto (fetch_one/fetch_all).
    """
    def __init__(self, perfil_slug: str | None = None):
        self.sb: Client = sb()
        self.perfil_id: str | None = None
        # mantém compat com seu uso atual de CURRENT_PROFILE
        try:
            self.set_profile(perfil_slug or CURRENT_PROFILE)
        except NameError:
            self.set_profile(perfil_slug)

    # -------- perfis ----------
    def set_profile(self, perfil_slug: str | None):
        """Ajusta o perfil (produtor rural) em uso. Usa tabela public.perfil(slug)."""
        self.perfil_slug = perfil_slug
        if not perfil_slug:
            self.perfil_id = None
            return

        # tenta achar pelo slug
        res = (self.sb.table("perfil")
                   .select("id")
                   .eq("slug", perfil_slug)
                   .limit(1).execute()).data

        # se não existir, cria e já retorna o id
        if not res:
            res = (self.sb.table("perfil")
                       .insert({"slug": perfil_slug, "nome": perfil_slug})
                       .select("id")
                       .execute()).data

        self.perfil_id = res[0]["id"] if res else None


    # -------- API compatível ----------
    def close(self):  # compat
        return

    def _next_id(self, table: str, per_profile: bool = False) -> int:
        """
        Retorna MAX(id)+1 da tabela. Se não houver linhas, retorna 1.
        Se per_profile=True e existir self.perfil_id, calcula por perfil.
        """
        q = self.sb.table(table).select("id").order("id", desc=True).limit(1)
        if per_profile and getattr(self, "perfil_id", None):
            q = q.eq("perfil_id", self.perfil_id)
        rows = q.execute().data or []
        max_id = int(rows[0]["id"]) if rows else 0
        return max_id + 1

    def insert_with_return(self, table: str, payload: dict) -> dict:
        """
        Insere no Supabase e retorna o primeiro registro criado (id incluído).
        """
        resp = self.sb.table(table).insert(payload).execute()
        return (resp.data or [{}])[0]

    def execute_query(self, sql: str, params: list | tuple | None = None, autocommit: bool = True):
        s = (sql or "").strip()
        s_low = s.lower()

        # --- SELECTs ---
        if s_low.startswith("select"):
            data = self._run_select(sql, params or [])
            class _Cur:
                def __init__(self, data): self._data = data
                def fetchall(self): return data
                def fetchone(self): return data[0] if data else None
            return _Cur(data)

        # --- INSERT/UPDATE mínimos suportados (sem SQLite) ---
        # participante
        if s_low.startswith("insert into lancamento"):
            cols15 = ["data","cod_imovel","cod_conta","num_doc","tipo_doc","historico",
                      "id_participante","tipo_lanc","valor_entrada","valor_saida",
                      "saldo_final","natureza_saldo","usuario","categoria","data_ord"]
            cols14 = ["data","cod_imovel","cod_conta","num_doc","tipo_doc","historico",
                      "id_participante","tipo_lanc","valor_entrada","valor_saida",
                      "saldo_final","natureza_saldo","usuario","data_ord"]
            cols = cols15 if len(params) == 15 else cols14
            payload = dict(zip(cols, params))
        
            def _to_iso(d):
                if isinstance(d, str) and "/" in d:
                    d = d.strip(); dd, mm, yyyy = d.split("/")
                    return f"{yyyy}-{int(mm):02d}-{int(dd):02d}"
                return d
            payload["data"] = _to_iso(payload.get("data"))
        
            # >>> NOVO: gravar perfil_id no lançamento
            if getattr(self, "perfil_id", None):
                payload["perfil_id"] = self.perfil_id
        
            # >>> NOVO: MAX(id)+1 POR PERFIL (per_profile=True)
            payload.pop("id", None)

            if not payload.get("perfil_id"):
                inferred = None
                cid = payload.get("cod_conta")
                if cid:
                    crow = (self.sb.table("conta_bancaria")
                                .select("perfil_id")
                                .eq("id", cid).limit(1).execute().data)
                    if crow and crow[0].get("perfil_id"):
                        inferred = crow[0]["perfil_id"]
                if not inferred:
                    iid = payload.get("cod_imovel")
                    if iid:
                        irow = (self.sb.table("imovel_rural")
                                    .select("perfil_id")
                                    .eq("id", iid).limit(1).execute().data)
                        if irow and irow[0].get("perfil_id"):
                            inferred = irow[0]["perfil_id"]
                if inferred:
                    payload["perfil_id"] = inferred
                        
            novo = self.insert_with_return("lancamento", payload)
            return type("Cur", (), {"fetchall": lambda *_: [], "fetchone": lambda *_: None})()

        if s_low.startswith("update participante set"):
            cpf, nome, tipo, pid = params
            self.sb.table("participante").update({"cpf_cnpj": cpf, "nome": nome, "tipo_contraparte": tipo}).eq("id", pid).execute()
            return type("Cur", (), {"fetchall": lambda *_: [], "fetchone": lambda *_: None})()

        # imovel_rural
        if (s_low.startswith("insert into imovel_rural")
            or s_low.startswith("insert or replace into imovel_rural")):

            cols = ["cod_imovel","pais","moeda","cad_itr","caepf","insc_estadual","nome_imovel",
                    "endereco","num","compl","bairro","uf","cod_mun","cep",
                    "tipo_exploracao","participacao","area_total","area_utilizada"]
            payload = dict(zip(cols, params))
            if self.perfil_id: payload["perfil_id"] = self.perfil_id
            
            # menor ID livre
            ids = (self.sb.table("imovel_rural").select("id").order("id", desc=False).execute().data) or []
            taken = {r["id"] for r in ids if r.get("id") is not None and int(r["id"]) > 0}
            nxt = 1
            while nxt in taken:
                nxt += 1
            payload["id"] = self._next_id("imovel_rural", per_profile=False)
            
            novo = self.insert_with_return("imovel_rural", payload)
            return type("Cur", (), {"fetchall": lambda *_: [], "fetchone": lambda *_: None})()

        if s_low.startswith("update imovel_rural set"):
            cols = ["cod_imovel","pais","moeda","cad_itr","caepf","insc_estadual","nome_imovel","endereco","num","compl","bairro","uf","cod_mun","cep","tipo_exploracao","participacao","area_total","area_utilizada"]
            payload = dict(zip(cols, params[:-1])); imovel_id = params[-1]
            self.sb.table("imovel_rural").update(payload).eq("id", imovel_id).execute()
            return type("Cur", (), {"fetchall": lambda *_: [], "fetchone": lambda *_: None})()

        # conta_bancaria
        if (s_low.startswith("insert into conta_bancaria")
            or s_low.startswith("insert or replace into conta_bancaria")):
            cols = ["cod_conta","pais_cta","banco","nome_banco","agencia","num_conta","saldo_inicial"]
            payload = dict(zip(cols, params))
            if self.perfil_id: payload["perfil_id"] = self.perfil_id

            # menor ID livre
            ids = (self.sb.table("conta_bancaria").select("id").order("id", desc=False).execute().data) or []
            taken = {r["id"] for r in ids if r.get("id") is not None and int(r["id"]) > 0}
            nxt = 1
            while nxt in taken:
                nxt += 1
            payload["id"] = self._next_id("conta_bancaria", per_profile=False)

            novo = self.insert_with_return("conta_bancaria", payload)
            return type("Cur", (), {"fetchall": lambda *_: [], "fetchone": lambda *_: None})()
        
        def _to_iso(d):
            if isinstance(d, str) and "/" in d:
                d = d.strip()
                dd, mm, yyyy = d.split("/")
                return f"{yyyy}-{int(mm):02d}-{int(dd):02d}"
            return d

        # UPDATE completo (edição da tela)
        m = re.match(r"update\s+lancamento\s+set\s+data\s*=\s*\?,\s*cod_imovel\s*=\s*\?,\s*cod_conta\s*=\s*\?,\s*num_doc\s*=\s*\?,\s*tipo_doc\s*=\s*\?,\s*historico\s*=\s*\?,\s*id_participante\s*=\s*\?,\s*tipo_lanc\s*=\s*\?,\s*valor_entrada\s*=\s*\?,\s*valor_saida\s*=\s*\?,\s*saldo_final\s*=\s*\?,\s*natureza_saldo\s*=\s*\?,\s*usuario\s*=\s*\?,\s*data_ord\s*=\s*\?\s*where\s*id\s*=\s*\?", s_low)
        if m:
            cols = ["data","cod_imovel","cod_conta","num_doc","tipo_doc","historico",
                    "id_participante","tipo_lanc","valor_entrada","valor_saida",
                    "saldo_final","natureza_saldo","usuario","data_ord"]
            payload = dict(zip(cols, params[:-1]))
            payload["data"] = _to_iso(payload.get("data"))
            self.sb.table("lancamento").update(payload).eq("id", params[-1]).execute()
            return type("Cur", (), {"fetchall": lambda *_: [], "fetchone": lambda *_: None})()

        # UPDATE só de saldo/natureza (recalcular cadeia)
        if s_low.startswith("update lancamento set saldo_final=?, natureza_saldo=? where id=?"):
            sf, nat, rid = params
            self.sb.table("lancamento").update({"saldo_final": sf, "natureza_saldo": nat}).eq("id", rid).execute()
            return type("Cur", (), {"fetchall": lambda *_: [], "fetchone": lambda *_: None})()

        # --- DELETE genérico: DELETE FROM <tabela> WHERE id = ? ---
        m = re.match(r"delete\s+from\s+(\w+)\s+where\s+id\s*=\s*\?", s_low)
        if m:
            table = m.group(1)
            rec_id = params[0] if params else None
            if rec_id is None:
                raise ValueError("DELETE sem id.")
            self.sb.table(table).delete().eq("id", rec_id).execute()
            return type("Cur", (), {"fetchall": lambda *_: [], "fetchone": lambda *_: None})()

        # Compat: "INSERT OR REPLACE INTO participante (cpf_cnpj, nome, tipo_contraparte) VALUES (?,?,?)"
        if s_low.startswith("insert or replace into participante"):
            cpf, nome, tipo = params
            self.upsert_participante(cpf, nome, int(tipo))
            return type("Cur", (), {"fetchall": lambda *_: [], "fetchone": lambda *_: None})()


        # CT-e: checagem de duplicidade por (num_doc normalizado + cpf/cnpj)
        m = re.match(
            r"select\s+1\s+from\s+lancamento\s+l\s+join\s+participante\s+p\s+on\s+p\.id\s*=\s*l\.id_participante.*where.*limit\s+1",
            s_low
        )
        if m:
            numdoc_raw = (params[0] or "")
            cpfcnpj_raw = (params[1] or "")
            # normalizações equivalentes ao SQL REPLACE(...)
            numdoc = numdoc_raw.replace(" ", "")
            digits = re.sub(r"\D", "", cpfcnpj_raw)

            # resolve participante pelo cpf_cnpj já sem máscara
            prow = (self.sb.table("participante")
                        .select("id")
                        .eq("cpf_cnpj", digits)
                        .limit(1).execute().data) or []
            if not prow:
                return []  # sem participante -> não há duplicado

            pid = prow[0]["id"]
            # verifica lançamento com mesmo num_doc e mesmo participante
            lrow = (self.sb.table("lancamento")
                        .select("id")
                        .eq("id_participante", pid)
                        .eq("num_doc", numdoc)
                        .limit(1).execute().data) or []
            return [(1,)] if lrow else []

        # Relatório: receitas/despesas agrupadas por ano-mês
        m = re.match(r"select\s+substr\(cast\(data_ord as text\),1,6\)\s+as\s+ym.*from\s+lancamento.*group\s+by\s+ym\s+order\s+by\s+ym", s_low, re.S)
        if m:
            rows = (self.sb.table("lancamento")
                        .select("data_ord, valor_entrada, valor_saida")
                        .gte("data_ord", params[0])
                        .lte("data_ord", params[1])
                        .execute().data) or []
            # agrega manualmente
            agg = {}
            for r in rows:
                ym = str(r["data_ord"])[:6]
                agg.setdefault(ym, {"rec":0,"des":0})
                agg[ym]["rec"] += r.get("valor_entrada",0) or 0
                agg[ym]["des"] += r.get("valor_saida",0) or 0
            return [(ym, v["rec"], v["des"]) for ym,v in sorted(agg.items())]

        # outros ainda não migrados
        raise NotImplementedError("Comando não suportado nesta fase (somente leitura + cadastros básicos via Supabase).")

    @contextlib.contextmanager
    def bulk(self):
        # No-op: não existe transação via PostgREST; manter compat até migrarmos escrita.
        yield

    def fetch_all(self, sql: str, params: list | tuple | None = None):
        return self.execute_query(sql, params).fetchall()

    def fetch_one(self, sql: str, params: list | tuple | None = None):
        return self.execute_query(sql, params).fetchone()

    # -- perfil_param -----------------------------------------------------------
    def get_perfil_param(self, profile_slug: str):
        row = (self.sb.table("perfil_param")
                  .select("version,ind_ini_per,sit_especial,ident,nome,logradouro,numero,complemento,bairro,uf,cod_mun,cep,telefone,email")
                  .eq("profile", profile_slug).limit(1).execute().data)
        if not row: return None
        r = row[0]
        return (r["version"], r["ind_ini_per"], r["sit_especial"], r["ident"], r["nome"],
                r["logradouro"], r["numero"], r["complemento"], r["bairro"], r["uf"],
                r["cod_mun"], r["cep"], r["telefone"], r["email"])

    def upsert_participante(self, cpf_cnpj: str, nome: str, tipo: int) -> int | None:
        """Upsert de participante pela chave de negócio cpf_cnpj, com MAX(id)+1."""
        cpf = (cpf_cnpj or "").strip()
        nm = (nome or "").strip()
        tp = int(tipo)
    
        # 1) Já existe? -> atualiza e retorna o id
        exist = (self.sb.table("participante")
                    .select("id")
                    .eq("cpf_cnpj", cpf)
                    .limit(1).execute().data)
        if exist:
            pid = exist[0]["id"]
            self.sb.table("participante").update({
                "nome": nm,
                "tipo_contraparte": tp
            }).eq("id", pid).execute()
            return pid
    
        # 2) Não existe -> cria com MAX(id)+1 (global, como sua tabela está definida)
        payload = {
            "id": self._next_id("participante", per_profile=False),
            "cpf_cnpj": cpf,
            "nome": nm,
            "tipo_contraparte": tp
        }
        # IMPORTANTE: NÃO gravar perfil_id aqui (a tabela não tem essa coluna)
    
        novo = self.insert_with_return("participante", payload)
        return novo.get("id")

    def upsert_perfil_param(self, profile_slug: str, p: dict):
        payload = {
            "profile": profile_slug,
            "version": p.get("version"),
            "ind_ini_per": p.get("ind_ini_per"),
            "sit_especial": p.get("sit_especial"),
            "ident": p.get("ident"),
            "nome": p.get("nome"),
            "logradouro": p.get("logradouro"),
            "numero": p.get("numero"),
            "complemento": p.get("complemento"),
            "bairro": p.get("bairro"),
            "uf": p.get("uf"),
            "cod_mun": p.get("cod_mun"),
            "cep": p.get("cep"),
            "telefone": p.get("telefone"),
            "email": p.get("email"),
        }
        # upsert pela PK "profile"
        self.sb.table("perfil_param").upsert(payload, on_conflict="profile").execute()

    # --------- SELECT dispatcher ----------
    def _run_select(self, sql: str, params: list | tuple):
        s_norm = " ".join((sql or "").strip().split())
        s_low = s_norm.lower()

        # 0) Helpers
        def _fmt_date_iso_to_br(iso_txt: str) -> str:
            if not iso_txt: return ""
            if "-" in iso_txt and len(iso_txt) >= 10:
                y, m, d = iso_txt[:10].split("-")
                return f"{int(d):02d}/{int(m):02d}/{y}"
            return iso_txt

        # 1) SUM(saldo_atual) FROM saldo_contas  -> computa via conta_bancaria + último lancamento
        if s_low.startswith("select sum(saldo_atual) from saldo_contas"):
            qcontas = self.sb.table("conta_bancaria").select("id,saldo_inicial")
            if getattr(self, "perfil_id", None):
                qcontas = qcontas.eq("perfil_id", self.perfil_id)
            contas = qcontas.execute().data or []
        
            total = 0
            for c in contas:
                cid = c.get("id"); si = c.get("saldo_inicial") or 0
                last = (self.sb.table("lancamento")
                            .select("saldo_final,natureza_saldo")
                            .eq("cod_conta", cid)
                            .order("id", desc=True)
                            .limit(1).execute().data)
                if last:
                    sfin = last[0].get("saldo_final") or 0
                    nature = (last[0].get("natureza_saldo") or "P").upper()
                    sign = 1 if nature == "P" else -1
                    total += (sfin * sign)
                else:
                    total += si
            return [(total,)]
        

        # 2) MIN/MAX(data_ord) FROM lancamento
        if s_low.startswith("select min(data_ord), max(data_ord) from lancamento"):
            qmin = self.sb.table("lancamento").select("data_ord")
            qmax = self.sb.table("lancamento").select("data_ord")

            if getattr(self, "perfil_id", None):
                contas = (self.sb.table("conta_bancaria").select("id").eq("perfil_id", self.perfil_id).execute().data) or []
                conta_ids = [r["id"] for r in contas]
                if not conta_ids:
                    return [(None, None)]
                qmin = qmin.in_("cod_conta", conta_ids)
                qmax = qmax.in_("cod_conta", conta_ids)

            rmin = qmin.order("data_ord", desc=False).limit(1).execute().data
            rmax = qmax.order("data_ord", desc=True ).limit(1).execute().data
            vmin = rmin[0]["data_ord"] if rmin and rmin[0]["data_ord"] is not None else None
            vmax = rmax[0]["data_ord"] if rmax and rmax[0]["data_ord"] is not None else None
            return [(vmin, vmax)]

        # 3) SUM(valor_entrada|valor_saida) BETWEEN datas (data_ord)
        m = re.match(r"select sum\((valor_entrada|valor_saida)\) from lancamento where data_ord between \? and \?", s_low)
        if m:
            col = m.group(1)  # valor_entrada -> Receita ; valor_saida -> Despesa
            d1, d2 = params

            # Buscamos colunas suficientes para pós-filtrar por perfil e tipo
            rows = (self.sb.table("lancamento")
                        .select(f"{col},cod_conta,cod_imovel,tipo_lanc")
                        .gte("data_ord", d1)
                        .lte("data_ord", d2)
                        .execute().data) or []

            # (1) filtro por perfil: aceita por CONTA ou por IMÓVEL (para não perder lançamentos sem conta)
            if getattr(self, "perfil_id", None):
                contas = (self.sb.table("conta_bancaria")
                             .select("id")
                             .eq("perfil_id", self.perfil_id)
                             .execute().data) or []
                imoveis = (self.sb.table("imovel_rural")
                              .select("id")
                              .eq("perfil_id", self.perfil_id)
                              .execute().data) or []
                conta_ids  = {r["id"] for r in contas}
                imovel_ids = {r["id"] for r in imoveis}

                def _match_perfil(r):
                    return (r.get("cod_conta") in conta_ids) or (r.get("cod_imovel") in imovel_ids)

                rows = [r for r in rows if _match_perfil(r)]

            # (2) filtro por TIPO (garante que receita não “some” saídas e vice-versa)
            tipo_desejado = 1 if col == "valor_entrada" else 2
            rows = [r for r in rows if (r.get("tipo_lanc") == tipo_desejado)]

            total = sum((r.get(col) or 0) for r in rows)
            return [(total,)]


        # 3.1) SALDO ANTERIOR (edição) -> "... cod_conta=? AND id < ? ORDER BY id DESC LIMIT 1"
        m = re.match(r"select \(saldo_final \* case natureza_saldo when 'p' then 1 else -1 end\) from lancamento where cod_conta=\? and id < \? order by id desc limit 1", s_low)
        if m:
            conta_id, lim_id = params
            rows = (self.sb.table("lancamento")
                      .select("saldo_final,natureza_saldo,id")
                      .eq("cod_conta", conta_id).lt("id", lim_id)
                      .order("id", desc=True).limit(1).execute().data) or []
            if not rows: return [(None,)]
            sfin = rows[0].get("saldo_final") or 0
            nat  = (rows[0].get("natureza_saldo") or "P").upper()
            return [(sfin * (1 if nat == "P" else -1),)]

        # 3.2) SALDO ANTERIOR (inserção) -> "... cod_conta=? ORDER BY id DESC LIMIT 1"
        m = re.match(r"select \(saldo_final \* case natureza_saldo when 'p' then 1 else -1 end\) from lancamento where cod_conta=\? order by id desc limit 1", s_low)
        if m:
            (conta_id,) = params
            rows = (self.sb.table("lancamento")
                      .select("saldo_final,natureza_saldo")
                      .eq("cod_conta", conta_id)
                      .order("id", desc=True).limit(1).execute().data) or []
            if not rows: return [(None,)]
            sfin = rows[0].get("saldo_final") or 0
            nat  = (rows[0].get("natureza_saldo") or "P").upper()
            return [(sfin * (1 if nat == "P" else -1),)]

        # 3.3) DUPLICIDADE por num_doc + participante
        # "SELECT id FROM lancamento WHERE REPLACE(COALESCE(num_doc,''),' ','') = ? AND id_participante = ? [AND id != ?]"
        if s_low.startswith("select id from lancamento where replace(coalesce(num_doc,''),' ','') = ? and id_participante = ?"):
            norm, pid = params[0], params[1]
            skip = params[2] if len(params) > 2 else None
            rows = (self.sb.table("lancamento")
                      .select("id,num_doc,id_participante")
                      .eq("id_participante", pid).execute().data) or []
            import re as _re
            def _norm(x): return _re.sub(r"\D+", "", (x or ""))  # normaliza como na tela
            for r in rows:
                if _norm(r.get("num_doc")) == norm and (skip is None or r.get("id") != skip):
                    return [(r["id"],)]
            return []


        # 3.4) Próximos lançamentos da conta (recalcular cadeia)
        # "SELECT id, valor_entrada, valor_saida FROM lancamento WHERE cod_conta=? AND id > ? ORDER BY id"
        m = re.match(r"select id, valor_entrada, valor_saida from lancamento where cod_conta=\? and id > \? order by id", s_low)
        if m:
            conta_id, from_id = params
            rows = (self.sb.table("lancamento")
                      .select("id,valor_entrada,valor_saida")
                      .eq("cod_conta", conta_id).gt("id", from_id)
                      .order("id", desc=False).execute().data) or []
            return _tuplize(rows, ["id", "valor_entrada", "valor_saida"])

        # 4) SELECT de lançamentos com JOIN/alias (listagem principal da UI)
        #    (reconhece o padrão do SQL original)
        if "from lancamento l" in s_low and "where l.data_ord between ? and ?" in s_low:
            d1_ord, d2_ord = params

            # 4.1 busca lançamentos no intervalo
            rows = (self.sb.table("lancamento")
                        .select("id,data,cod_imovel,num_doc,id_participante,historico,tipo_lanc,valor_entrada,valor_saida,saldo_final,natureza_saldo,usuario,data_ord,cod_conta")
                        .gte("data_ord", d1_ord)
                        .lte("data_ord", d2_ord)
                        .execute().data) or []

            # 4.2 filtra por perfil ativo (via imovel.perfil_id)
            if self.perfil_id:
                imoveis = (self.sb.table("imovel_rural")
                               .select("id")
                               .eq("perfil_id", self.perfil_id)
                               .execute().data) or []
                imovel_ids = {r["id"] for r in imoveis}
                rows = [r for r in rows if r.get("cod_imovel") in imovel_ids]

            # 4.3 mapas auxiliares (imóvel e participante)
            ids_imovel = sorted({r.get("cod_imovel") for r in rows if r.get("cod_imovel") is not None})
            ids_part   = sorted({r.get("id_participante") for r in rows if r.get("id_participante") is not None})

            imap = {}
            if ids_imovel:
                im_data = (self.sb.table("imovel_rural")
                               .select("id,nome_imovel")
                               .execute().data) or []
                imap = {r["id"]: (r.get("nome_imovel") or "") for r in im_data}

            pmap = {}
            if ids_part:
                p_data = (self.sb.table("participante")
                               .select("id,nome")
                               .execute().data) or []
                pmap = {r["id"]: (r.get("nome") or "") for r in p_data}

            # 4.4 monta saída conforme colunas originais e ordena (data_ord desc, id desc)
            def _key(r):
                do = r.get("data_ord")
                return (-(do if isinstance(do, int) else -1), -(r.get("id") or 0))

            rows_sorted = sorted(rows, key=_key)

            out = []
            for r in rows_sorted:
                dtxt = _fmt_date_iso_to_br(str(r.get("data") or ""))
                tl = int(r.get("tipo_lanc") or 0)
                tipo = "Receita" if tl == 1 else ("Despesa" if tl == 2 else "Adiantamento")
                nature = (r.get("natureza_saldo") or "P").upper()
                sign = 1 if nature == "P" else -1
                saldo = (r.get("saldo_final") or 0) * sign
                out.append((
                    r.get("id"),
                    dtxt,
                    imap.get(r.get("cod_imovel"), ""),
                    r.get("num_doc"),
                    pmap.get(r.get("id_participante"), ""),
                    r.get("historico"),
                    tipo,
                    r.get("valor_entrada") or 0,
                    r.get("valor_saida") or 0,
                    saldo,
                    r.get("usuario") or ""
                ))
            return out

        # 5) PARTICIPANTE: id por cpf_cnpj
        m = re.match(r"select id from participante where cpf_cnpj\s*=\s*\?", s_low)
        if m:
            digits = params[0]
            data = (self.sb.table("participante")
                        .select("id")
                        .eq("cpf_cnpj", digits)
                        .limit(1).execute().data)
            return ([(data[0]["id"],)] if data else [None])

        # 6) PARTICIPANTE: dados por id
        m = re.match(r"select cpf_cnpj, nome, tipo_contraparte from participante where id\s*=\s*\?", s_low)
        if m:
            pid = params[0]
            data = (self.sb.table("participante")
                        .select("cpf_cnpj,nome,tipo_contraparte")
                        .eq("id", pid)
                        .limit(1).execute().data)
            if not data: return [None]
            d = data[0]
            return [(d.get("cpf_cnpj"), d.get("nome"), d.get("tipo_contraparte"))]

        # 7) IMÓVEL: id por cod_imovel
        m = re.match(r"select id from imovel_rural where cod_imovel\s*=\s*\?", s_low)
        if m:
            cod = params[0]
            q = (self.sb.table("imovel_rural")
                        .select("id")
                        .eq("cod_imovel", cod))
            if self.perfil_id: q = q.eq("perfil_id", self.perfil_id)
            data = q.limit(1).execute().data
            return ([(data[0]["id"],)] if data else [None])

        # 8) CONTA: id por cod_conta
        m = re.match(r"select id from conta_bancaria where cod_conta\s*=\s*\?", s_low)
        if m:
            cod = params[0]
            q = (self.sb.table("conta_bancaria")
                        .select("id")
                        .eq("cod_conta", cod))
            if self.perfil_id: q = q.eq("perfil_id", self.perfil_id)
            data = q.limit(1).execute().data
            return ([(data[0]["id"],)] if data else [None])

        # 9) CONTA: select ... where id = ?
        m = re.match(
            r"select\s+(?P<cols>[\w\*,\s,]+)\s+from\s+conta_bancaria\s+where\s+id\s*=\s*\?",
            s_norm, re.IGNORECASE
        )
        if m:
            cols_txt = m.group("cols").strip().replace(" ", "")
            if not params:
                return [None]  # segurança extra
            cid = params[0]
            data = (self.sb.table("conta_bancaria")
                        .select(cols_txt)
                        .eq("id", cid)
                        .limit(1).execute().data)
            if not data:
                return [None]
            cols_list = [c.strip() for c in cols_txt.split(",")]
            return _tuplize(data, cols_list)

        # 10) PARTICIPANTE: listas para combos
        if s_low.startswith("select id, nome, cpf_cnpj from participante order by nome"):
            rows = (self.sb.table("participante")
                        .select("id,nome,cpf_cnpj")
                        .order("nome").execute().data)
            return _tuplize(rows, ["id","nome","cpf_cnpj"])

        if s_low.startswith("select id, nome from participante order by nome"):
            rows = (self.sb.table("participante")
                        .select("id,nome")
                        .order("nome").execute().data)
            return _tuplize(rows, ["id","nome"])

        if s_low.startswith("select id,cpf_cnpj,nome,tipo_contraparte,data_cadastro from participante order by data_cadastro desc"):
            rows = (self.sb.table("participante")
                        .select("id,cpf_cnpj,nome,tipo_contraparte,data_cadastro")
                        .order("data_cadastro", desc=True).execute().data)
            return _tuplize(rows, ["id","cpf_cnpj","nome","tipo_contraparte","data_cadastro"])

        # 11) IMÓVEL / CONTA: listas simples
        if s_low.startswith("select id, nome_imovel from imovel_rural"):
            q = self.sb.table("imovel_rural").select("id,nome_imovel")
            if self.perfil_id: q = q.eq("perfil_id", self.perfil_id)
            rows = q.order("nome_imovel").execute().data
            return _tuplize(rows, ["id","nome_imovel"])

        if s_low.startswith("select id, nome_banco from conta_bancaria"):
            q = self.sb.table("conta_bancaria").select("id,nome_banco")
            if self.perfil_id: q = q.eq("perfil_id", self.perfil_id)
            rows = q.order("nome_banco").execute().data
            return _tuplize(rows, ["id","nome_banco"])

        # IMÓVEL: select ... where id=?
        m = re.match(r"select\s+(?P<cols>[\w\*,\s]+)\s+from\s+imovel_rural\s+where\s+id\s*=\s*\?", s_low)
        if m:
            cols = m.group("cols").replace(" ", "")
            imovel_id = params[0]
            data = (self.sb.table("imovel_rural")
                        .select(cols)
                        .eq("id", imovel_id)
                        .limit(1).execute().data)
            if not data:
                return [None]
            return _tuplize(data, [c for c in cols.split(",")])

        # --- LANCAMENTO: select <cols> from lancamento where id = ?
        m = re.match(r"select\s+(?P<cols>[\w\*,\s]+)\s+from\s+lancamento\s+where\s+id\s*=\s*\?", s_low)
        if m:
            # re-captura em s_norm para preservar a lista de colunas
            m2 = re.match(r"select\s+(?P<cols>[\w\*,\s]+)\s+from\s+lancamento\s+where\s+id\s*=\s*\?", s_norm, re.IGNORECASE)
            cols_txt = m2.group("cols").strip().replace(" ", "")
            rid = params[0]
            rows = (self.sb.table("lancamento")
                        .select(cols_txt)
                        .eq("id", rid)
                        .limit(1).execute().data) or []
            if not rows:
                return [None]
            cols_list = [c.strip() for c in cols_txt.split(",")]
            return _tuplize(rows, cols_list)

        # 12) Fallback: SELECT simples sem WHERE/ORDER complexos
        m = re.match(r"select\s+(?P<cols>[\w\*,\s]+)\s+from\s+(?P<table>\w+)$", s_low)
        if m:
            cols = m.group("cols").replace(" ", "")
            table = m.group("table")
            rows = self.sb.table(table).select(cols).execute().data
            return _tuplize(rows, [c for c in cols.split(",")])

        # SELECT da grade de "Gerenciamento de Imóveis"
        if s_low.startswith(
            "select id, cod_imovel, nome_imovel, uf, area_total, area_utilizada, participacao from imovel_rural order by nome_imovel"
        ):
            q = (self.sb.table("imovel_rural")
                    .select("id,cod_imovel,nome_imovel,uf,area_total,area_utilizada,participacao"))
            if getattr(self, "perfil_id", None):
                q = q.eq("perfil_id", self.perfil_id)
            rows = q.order("nome_imovel").execute().data or []
            return [
                (
                    r.get("id"),
                    r.get("cod_imovel"),
                    r.get("nome_imovel"),
                    r.get("uf"),
                    r.get("area_total"),
                    r.get("area_utilizada"),
                    r.get("participacao"),
                )
                for r in rows
            ]

        if s_low.startswith("select id, nome_imovel from imovel_rural order by nome_imovel"):
            q = self.sb.table("imovel_rural").select("id,nome_imovel")
            if getattr(self, "perfil_id", None):
                q = q.eq("perfil_id", self.perfil_id)
            rows = q.order("nome_imovel").execute().data or []
            return [(r.get("id"), r.get("nome_imovel")) for r in rows]

        # SELECT da grade de "Gerenciamento de Contas"
        if re.match(
            r"select\s+id\s*,\s*cod_conta\s*,\s*nome_banco\s*,\s*agencia\s*,\s*num_conta\s*,\s*saldo_inicial\s*from\s+conta_bancaria\s+order\s+by\s+nome_banco",
            s_low
        ):
            q = (self.sb.table("conta_bancaria")
                    .select("id,cod_conta,nome_banco,agencia,num_conta,saldo_inicial"))
            if getattr(self, "perfil_id", None):
                q = q.eq("perfil_id", self.perfil_id)
            rows = q.order("nome_banco").execute().data or []
            return [
                (
                    r.get("id"),
                    r.get("cod_conta"),
                    r.get("nome_banco"),
                    r.get("agencia"),
                    r.get("num_conta"),
                    r.get("saldo_inicial"),
                )
                for r in rows
            ]

        # Imóvel rural por nome
        m = re.match(r"select\s+cod_imovel\s+from\s+imovel_rural\s+where\s+upper\(nome_imovel\)=\?", s_low)
        if m:
            nome = (params[0] or "").upper()
            rows = (self.sb.table("imovel_rural")
                        .select("cod_imovel")
                        .eq("nome_imovel", nome)
                        .limit(1)
                        .execute().data) or []
            return _tuplize(rows, ["cod_imovel"])

        # Imóvel rural por nome (LIKE)
        m = re.match(r"select\s+cod_imovel\s+from\s+imovel_rural\s+where\s+upper\(nome_imovel\)\s+like\s+\?", s_low)
        if m:
            like = (params[0] or "").upper()
            rows = (self.sb.table("imovel_rural")
                        .select("cod_imovel")
                        .like("nome_imovel", like)
                        .limit(1)
                        .execute().data) or []
            return _tuplize(rows, ["cod_imovel"])

        # Agrupamento mensal (ym = AAAAMM) de receitas e despesas no período
        m = re.match(r"select\s+substr\(cast\(data_ord\s+as\s+text\),1,6\)\s+as\s+ym.*where\s+data_ord\s+between\s+\?\s+and\s+\?\s+group\s+by\s+ym\s+order\s+by\s+ym", s_low, re.S)
        if m:
            d1, d2 = params
            rows = (self.sb.table("lancamento")
                        .select("data_ord,valor_entrada,valor_saida")
                        .gte("data_ord", int(d1))
                        .lte("data_ord", int(d2))
                        .order("data_ord")
                        .execute().data) or []
            agg = {}
            for r in rows:
                ym = str(r.get("data_ord") or "")[:6]
                if not ym:
                    continue
                agg.setdefault(ym, {"rec": 0.0, "des": 0.0})
                agg[ym]["rec"] += r.get("valor_entrada", 0) or 0
                agg[ym]["des"] += r.get("valor_saida", 0) or 0
            return [(ym, v["rec"], v["des"]) for ym, v in sorted(agg.items())]

        # [NEW] SELECT cod_imovel FROM imovel_rural WHERE UPPER(nome_imovel) LIKE ?
        m = re.match(r"select\s+cod_imovel\s+from\s+imovel_rural\s+where\s+upper\(nome_imovel\)\s+like\s+\?", s_low)
        if m:
            pat = str(params[0] or "")
            # remove 'UPPER(...)' e usa ilike no Supabase
            # ex: '%FAZENDA ESTRELA%' => ilike('%fazenda estrela%')
            pat_ilike = pat.replace("%", "%")
            r = self.sb.table("imovel_rural").select("cod_imovel").ilike("nome_imovel", pat_ilike).limit(1).execute()
            data = [(row.get("cod_imovel"),) for row in (r.data or [])]
            return _Rows(data)

        elif re.search(
            r"select\s+1\s+from\s+lancamento\s+l\s+join\s+participante\s+p\s+on\s+p\.id\s*=\s*l\.id_participante\s+where\s+replace\(\s*coalesce\(\s*l\.num_doc\s*,\s*''\s*\)\s*,\s*'\s*'\s*,\s*''\s*\)\s*=\s*\?\s+and\s+replace\(\s*coalesce\(\s*p\.cpf_cnpj\s*,\s*''\s*\)\s*,\s*'\s*'\s*,\s*''\s*\)\s*=\s*\?\s+limit\s+1",
            s_low
        ):
            # params: [norm_numdoc, digits]
            norm_numdoc, digits = params[0], params[1]

            # 1) pega ids do participante pelo cpf_cnpj
            presp = self.sb("participante").select("id").eq("cpf_cnpj", digits).limit(50).execute()
            p_ids = [r["id"] for r in (presp.data or [])] if presp.ok else []

            # 2) busca lançamento por num_doc e participante
            found = False
            if p_ids:
                lresp = (
                    self.sb("lancamento")
                    .select("id")
                    .eq("num_doc", norm_numdoc)
                    .in_("id_participante", p_ids)
                    .limit(1)
                    .execute()
                )
                found = bool(lresp.ok and lresp.data)

            return _FakeCursor([(1,)] if found else [])

        # --- imovel_rural por cod_imovel (+ opcional perfil_id) ---
        if "from imovel_rural" in s_low and "where cod_imovel" in s_low:
            cod = params[0]
            pid = params[1] if len(params) > 1 else None
            q = self.sb.table("imovel_rural").select("id").eq("cod_imovel", cod)
            if pid:
                q = q.eq("perfil_id", pid)
            data = q.limit(1).execute().data or []
            return _Rows([[r["id"]] for r in data])

        
        # --- conta_bancaria por cod_conta (+ opcional perfil_id) ---
        if "from conta_bancaria" in s_low and "where cod_conta" in s_low:
            cod = params[0]
            pid = params[1] if len(params) > 1 else None
            q = self.sb.table("conta_bancaria").select("id").eq("cod_conta", cod)
            if pid:
                q = q.eq("perfil_id", pid)
            data = q.limit(1).execute().data or []
            return _Rows([[r["id"]] for r in data])

        # Participante: pegar 1 CPF (11 dígitos) — tolera WHERE vazio, ordem LIMIT/ORDER trocada e "ORDER BYid"
        if re.match(
            r"select\s+cpf_cnpj\s+from\s+participante(?:\s+where\b.*)?\s+(?:order\s+by\s*id\s+limit\s+1|limit\s+1\s+order\s+by\s*id)\b",
            s_low
        ):
            rows = (
                self.sb.table("participante")
                .select("id,cpf_cnpj")
                .order("id")
                .limit(100)
                .execute().data
            ) or []
            for r in rows:
                digits = re.sub(r"\D", "", r.get("cpf_cnpj") or "")
                if len(digits) == 11:
                    return [(r.get("cpf_cnpj"),)]
            # fallback: se não houver CPF puro, devolve o primeiro que houver
            return ([(rows[0].get("cpf_cnpj"),)] if rows else [])

        
        # 13) Não mapeado
        raise NotImplementedError(f"SELECT não mapeado para Supabase:\n{sql}")

# --- Realtime bridge (Supabase -> UI) -----------------------------------------
class RealtimeBridge:
    def __init__(self, sb: Client, tables: list[str], schema: str = SUPABASE_SCHEMA, channel: str = os.getenv("SUPABASE_RT_CHANNEL", "agroapp-rt")):
        # mantém assinatura p/ não mudar chamadas existentes
        self.tables = tables
        self.schema = schema
        self.channel = channel
        self._thread = None
        self._stop = False

    def start(self, on_change):
        import asyncio

        def _runner():
            async def main():
                self._stop = False
                asb: AsyncClient = await create_async_client(SUPABASE_URL, SUPABASE_ANON_KEY)
                ch = asb.channel(self.channel)
                for t in self.tables:
                    ch.on_postgres_changes(event="*", schema=self.schema, table=t, callback=on_change)
                await ch.subscribe()
                # loop de vida do canal
                while not self._stop:
                    await asyncio.sleep(0.5)
                try:
                    await ch.unsubscribe()
                except Exception:
                    pass
            asyncio.run(main())

        self._thread = threading.Thread(target=_runner, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop = True

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

# === Setup unificado para todas as QTableWidget =================================
def setup_interactive_table(
    table: QTableWidget,
    *, 
    header_movable: bool = True,
    select_rows: bool = True,
    extended_selection: bool = True,
    resize_mode: QHeaderView.ResizeMode | None = None,
    stretch_last_section: bool | None = None,
    build_cache: bool = True,
    enable_sort_on_header_double_click: bool = True,
):
    """Aplica a 'mesma experiência' de interação em qualquer QTableWidget."""
    hdr = table.horizontalHeader()
    if header_movable:
        hdr.setSectionsMovable(True)               # mover colunas com o mouse
    table.setWordWrap(False)
    table.setTextElideMode(Qt.ElideNone)
    table.setHorizontalScrollMode(QTableWidget.ScrollPerPixel)
    table.setSizeAdjustPolicy(QTableWidget.AdjustToContents)
    table.setEditTriggers(QTableWidget.NoEditTriggers)

    if select_rows:
        table.setSelectionBehavior(QTableWidget.SelectRows)     # seleção por LINHA
    if extended_selection:
        table.setSelectionMode(QTableWidget.ExtendedSelection)  # Ctrl/Shift

    table.setAlternatingRowColors(True)
    table.setShowGrid(False)
    table.verticalHeader().setVisible(False)

    hdr.setHighlightSections(False)
    hdr.setDefaultAlignment(Qt.AlignCenter)

    # Largura/ajuste das colunas
    if resize_mode is not None:
        hdr.setSectionResizeMode(resize_mode)
    else:
        # padrão: comportar-se como Lançamentos (auto + sem sobra)
        hdr.setSectionResizeMode(QHeaderView.ResizeToContents)

    if stretch_last_section is not None:
        hdr.setStretchLastSection(stretch_last_section)

    # Ordenação por duplo clique no header
    if enable_sort_on_header_double_click:
        try:
            _install_header_double_click_sort(table)  # já existe no seu código
        except Exception:
            pass

    # Acelera filtro + badge
    if build_cache:
        try:
            ListAccelerator.build_cache(table)
        except Exception:
            pass
    try:
        ListCounter.refresh(table)
    except Exception:
        pass
# ================================================================================

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
        """
        CPF: valida localmente, sem consulta na Receita.
        CNPJ: valida localmente, consulta Receita, preenche 'nome' e avança o foco.
        """
        import re
        from PySide6.QtWidgets import QMessageBox
        txt = (self.cpf_cnpj.text() or '').strip()
        digits = re.sub(r'\D+', '', txt)

        # --- CPF (11) ---
        if len(digits) == 11:
            # valida local
            if not valida_cpf(digits):
                QMessageBox.warning(self, "CPF inválido", "Informe um CPF válido.")
                self.cpf_cnpj.setFocus()
                return
            # formatação (opcional)
            self.cpf_cnpj.setText(f"{digits[0:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:11]}")
            # NÃO consulta Receita; foca próximo campo
            self.focusNextChild()
            return

        # --- CNPJ (14) ---
        if len(digits) == 14:
            # valida CNPJ local (veja função valida_cnpj abaixo)
            if not valida_cnpj(digits):
                QMessageBox.warning(self, "CNPJ inválido", "Informe um CNPJ válido.")
                self.cpf_cnpj.setFocus()
                return

            # formatação (opcional)
            self.cpf_cnpj.setText(f"{digits[0:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:14]}")

            # consulta Receita — sua consulta_receita RETORNA UM DICT
            data = consulta_receita(digits, tipo='cnpj')  # <<< NÃO faça 'ok, dados = ...'
            # tenta extrair um nome útil
            nome_receita = _nome_cnpj_from_receita(data) if isinstance(data, dict) else ""
            if nome_receita and hasattr(self, "nome"):
                if not (self.nome.text() or "").strip():
                    self.nome.setText(nome_receita)

            # foca o próximo campo da tela
            self.focusNextChild()
            return

        # --- Tamanho inesperado: só normaliza/avisa ---
        if digits:
            QMessageBox.warning(self, "Documento inválido", "CPF deve ter 11 dígitos ou CNPJ 14 dígitos.")
        self.cpf_cnpj.setText(digits)
        self.cpf_cnpj.setFocus()


    def salvar(self):
        """
        Salva participante…
        """
        from PySide6.QtWidgets import QMessageBox, QApplication
        import re

        parent = self.parent()
        db = getattr(parent, "db", None)
        if db is None:
            QMessageBox.warning(self, "Erro", "Conexão com o banco não encontrada.")
            return

        cpf_widget  = getattr(self, "cpf_cnpj", None)
        nome_widget = getattr(self, "nome", None)

        cpf_raw = (cpf_widget.text() if cpf_widget else "").strip()
        nome    = (nome_widget.text() if nome_widget else "").strip()
        digits  = re.sub(r"\D+", "", cpf_raw)

        if not digits or not nome:
            QMessageBox.warning(self, "Campos obrigatórios", "Informe CPF/CNPJ e Nome.")
            return

        idx = self.tipo.currentIndex() if hasattr(self, "tipo") else -1
        tipo = [1,2,3,4][idx] if idx in (0,1,2,3) else (2 if len(digits) == 11 else 1)

        # ✅ NOVO: impedir duplicado quando for inclusão (ou edição trocando p/ um já existente)
        exist = (
            db.sb.table("participante")
                .select("id,nome")
                .eq("cpf_cnpj", digits)
                .limit(1)
                .execute()
                .data
        )
        if exist:
            exist_id   = exist[0]["id"]
            exist_nome = exist[0]["nome"]
            # se for inclusão OU se estiver tentando trocar o doc para um que já pertence a outro id
            if not getattr(self, "participante_id", None) or int(exist_id) != int(self.participante_id):
                QMessageBox.warning(
                    self,
                    "CPF/CNPJ já cadastrado",
                    f"Este documento já está cadastrado para: {exist_nome} (ID {exist_id})."
                )
                if cpf_widget: cpf_widget.setFocus()
                return
        
        # Checagem de duplicidade direto no Supabase
        try:
            exist = (
                db.sb.table("participante")
                    .select("id,nome")
                    .eq("cpf_cnpj", digits)
                    .limit(1)
                    .execute()
                    .data
            )
        except Exception as e:
            QMessageBox.critical(self, "Erro ao consultar duplicidade", str(e))
            return
        
        if exist:
            exist_id   = exist[0].get("id")
            exist_nome = exist[0].get("nome") or ""
            # bloqueia inclusão ou troca para documento que já pertence a outro ID
            if not getattr(self, "participante_id", None) or int(exist_id) != int(self.participante_id):
                QMessageBox.warning(
                    self,
                    "CPF/CNPJ já cadastrado",
                    f"Este documento já está cadastrado para: {exist_nome} (ID {exist_id})."
                )
                if cpf_widget:
                    cpf_widget.setFocus()
                return

        # Validação de CPF (não chama Receita)
        if len(digits) == 11:
            if not valida_cpf(digits):
                QMessageBox.warning(self, "CPF inválido", "Informe um CPF válido.")
                if cpf_widget: cpf_widget.setFocus()
                return

        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            pid = db.upsert_participante(digits, nome, tipo)
        except Exception as e:
            QApplication.restoreOverrideCursor()
            QMessageBox.critical(self, "Erro ao salvar", f"{e}")
            return
        finally:
            QApplication.restoreOverrideCursor()

        self.novo_id = pid

        # Atualiza todas as listas/combos abertas
        try:
            if parent and hasattr(parent, "_broadcast_participantes_changed"):
                parent._broadcast_participantes_changed()
            if parent and hasattr(parent, "_reload_participantes"):
                parent._reload_participantes()
        except Exception:
            pass

        QMessageBox.information(self, "Sucesso", "Participante salvo com sucesso.")
        self.accept()

class ParametrosDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Parâmetros do Contribuinte")
        self.setMinimumSize(400, 500)
        self.db = Database()
        row = self.db.get_perfil_param(CURRENT_PROFILE)
        def _val(k, default=""):
            if not row:  # sem registro no SQL ainda → usa QSettings como fallback
                return default
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

        # monta o formulário usando os widgets já criados acima (vindos do Supabase)
        layout.addRow("Versão do Leiaute:", self.version)
        layout.addRow("Ind. início do período:", self.ind_ini_per)
        layout.addRow("Situação especial:", self.sit_especial)
        layout.addRow("CPF:", self.ident)
        layout.addRow("Nome / Razão Social:", self.nome)
        
        # Endereço
        layout.addRow("Logradouro:", self.logradouro)
        layout.addRow("Número:", self.numero)
        layout.addRow("Complemento:", self.complemento)
        layout.addRow("Bairro:", self.bairro)
        
        # Localização
        layout.addRow("UF:", self.uf)
        layout.addRow("Cód. Município:", self.cod_mun)
        layout.addRow("CEP:", self.cep)
        
        # Contato
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
        self.settings = QSettings("AgroLCDPR", "UI")

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

        ini = self.settings.value("dashFilterIni", None)
        fim = self.settings.value("dashFilterFim", None)
        if isinstance(ini, QDate): self.dt_dash_ini.setDate(ini)
        if isinstance(fim, QDate): self.dt_dash_fim.setDate(fim)

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
        """
        Retorna (categorias, receitas[], despesas[]) por fazenda usando Supabase,
        evitando colunas extras (ex.: 'usuario') caírem em posições erradas.
        """
        sb = self.db.sb
        pid = self.db.perfil_id

        # 1) contas do perfil
        contas = (sb.table("conta_bancaria")
                    .select("id")
                    .eq("perfil_id", pid)
                    .execute().data) or []
        conta_ids = [c["id"] for c in contas]
        if not conta_ids:
            return [], [], []

        # 2) lançamentos no período, apenas campos necessários
        #    (filtra por contas do perfil)
        lans = (sb.table("lancamento")
                  .select("cod_imovel,valor_entrada,valor_saida,cod_conta,data_ord")
                  .gte("data_ord", d1_ord)
                  .lte("data_ord", d2_ord)
                  .in_("cod_conta", conta_ids)
                  .execute().data) or []

        # 3) mapa id_imovel -> nome
        imvs = (sb.table("imovel_rural")
                  .select("id,nome_imovel")
                  .eq("perfil_id", pid)
                  .execute().data) or []
        nome_by_id = {r["id"]: (r.get("nome_imovel") or f"IMÓVEL {r['id']}") for r in imvs}

        # 4) agrega
        from collections import defaultdict
        soma_r = defaultdict(float)
        soma_d = defaultdict(float)
        for r in lans:
            fid = r.get("cod_imovel")
            if fid is None:
                continue
            key = nome_by_id.get(fid, f"IMÓVEL {fid}")
            try:
                soma_r[key] += float(r.get("valor_entrada") or 0)
                soma_d[key] += float(r.get("valor_saida") or 0)
            except Exception:
                # Se vier string por engano, ignora aquela linha
                continue

        # 5) ordena por nome de fazenda
        cats = sorted(soma_r.keys())
        receitas = [soma_r[c] for c in cats]
        despesas = [soma_d[c] for c in cats]
        return cats, receitas, despesas

    def _dados_anual(self, d1_ord: int, d2_ord: int):
        """
        Retorna (categorias, receitas[], despesas[]) agregando por ANO.
        Se estiver usando Supabase (self.db.sb), agrupa em Python para evitar SELECT não mapeado.
        """
        sb = getattr(self.db, "sb", None)
        if sb:
            pid = getattr(self.db, "perfil_id", None)

            # Busca os lançamentos necessários
            rows = (sb.table("lancamento")
                        .select("data_ord,valor_entrada,valor_saida,cod_conta,cod_imovel,tipo_lanc")
                        .gte("data_ord", d1_ord)
                        .lte("data_ord", d2_ord)
                        .execute().data) or []

            # Filtro por perfil: aceita por CONTA ou por IMÓVEL (coerente com _run_select)
            if pid:
                contas = (sb.table("conta_bancaria").select("id").eq("perfil_id", pid).execute().data) or []
                imoveis = (sb.table("imovel_rural").select("id").eq("perfil_id", pid).execute().data) or []
                cset = {c["id"] for c in contas}
                iset = {i["id"] for i in imoveis}
                rows = [r for r in rows if (r.get("cod_conta") in cset) or (r.get("cod_imovel") in iset)]

            # Agrega por ANO
            agg = {}  # ano -> [rec, des]
            for r in rows:
                ano = str(r.get("data_ord") or "")[:4]
                if len(ano) != 4:
                    continue
                rec = float(r.get("valor_entrada") or 0.0)
                des = float(r.get("valor_saida") or 0.0)
                tot = agg.setdefault(ano, [0.0, 0.0])
                tot[0] += rec
                tot[1] += des

            cats = sorted(agg.keys())
            r = [agg[a][0] for a in cats]
            d = [agg[a][1] for a in cats]
            return cats, r, d

        # Caminho SQLite/local (SQL original)
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
        sb = getattr(self.db, "sb", None)
        pid = getattr(self.db, "perfil_id", None)

        if por_fazenda:
            if sb:
                # Imóveis do perfil (p/ mapear id -> nome)
                im_rows = (sb.table("imovel_rural")
                              .select("id,nome_imovel" + (",perfil_id" if pid else ""))
                              .eq("perfil_id", pid).execute().data) if pid else \
                          (sb.table("imovel_rural").select("id,nome_imovel").execute().data)
                im_rows = im_rows or []
                imap = {i["id"]: (i.get("nome_imovel") or f"IMÓVEL {i['id']}") for i in im_rows}

                # Lançamentos no período
                rows = (sb.table("lancamento")
                            .select("data_ord,valor_entrada,valor_saida,cod_imovel,cod_conta")
                            .gte("data_ord", d1_ord)
                            .lte("data_ord", d2_ord)
                            .execute().data) or []

                # Filtro por perfil via CONTA ou IMÓVEL
                if pid:
                    contas = (sb.table("conta_bancaria").select("id").eq("perfil_id", pid).execute().data) or []
                    cset = {c["id"] for c in contas}
                    rows = [r for r in rows if (r.get("cod_imovel") in imap) or (r.get("cod_conta") in cset)]

                # Agrega por (fazenda, AAAAMM)
                agg = {}  # (faz, ym) -> [rec, des]
                for r in rows:
                    ym = str(r.get("data_ord") or "")[:6]
                    if len(ym) != 6:
                        continue
                    fid = r.get("cod_imovel")
                    faz = imap.get(fid, f"IMÓVEL {fid}" if fid is not None else "—")
                    rec = float(r.get("valor_entrada") or 0.0)
                    des = float(r.get("valor_saida") or 0.0)
                    key = (faz, ym)
                    tot = agg.setdefault(key, [0.0, 0.0])
                    tot[0] += rec
                    tot[1] += des

                out = [ (faz, ym, vals[0], vals[1]) for (faz, ym), vals in agg.items() ]
                out.sort(key=lambda x: (str(x[0]), str(x[1])))
                return out

            # Caminho SQL (local)
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
            if sb:
                # Lançamentos e filtro perfil
                rows = (sb.table("lancamento")
                            .select("data_ord,valor_entrada,valor_saida,cod_conta,cod_imovel")
                            .gte("data_ord", d1_ord)
                            .lte("data_ord", d2_ord)
                            .execute().data) or []
                if pid:
                    contas = (sb.table("conta_bancaria").select("id").eq("perfil_id", pid).execute().data) or []
                    imoveis = (sb.table("imovel_rural").select("id").eq("perfil_id", pid).execute().data) or []
                    cset = {c["id"] for c in contas}
                    iset = {i["id"] for i in imoveis}
                    rows = [r for r in rows if (r.get("cod_conta") in cset) or (r.get("cod_imovel") in iset)]

                # Agrega por AAAAMM
                agg = {}  # ym -> [rec, des]
                for r in rows:
                    ym = str(r.get("data_ord") or "")[:6]
                    if len(ym) != 6:
                        continue
                    rec = float(r.get("valor_entrada") or 0.0)
                    des = float(r.get("valor_saida") or 0.0)
                    tot = agg.setdefault(ym, [0.0, 0.0])
                    tot[0] += rec
                    tot[1] += des

                out = [ (ym, vals[0], vals[1]) for ym, vals in agg.items() ]
                out.sort(key=lambda x: str(x[0]))
                return out

            # Caminho SQL (local)
            sql = """
            SELECT substr(CAST(data_ord AS TEXT),1,6) AS ym,
                   SUM(valor_entrada) AS rec, SUM(valor_saida) AS des
            FROM lancamento
            WHERE data_ord BETWEEN ? AND ?
            GROUP BY ym ORDER BY ym
            """
            return self._rows(sql, (d1_ord, d2_ord))

    def _dre_por_fazenda(self, d1_ord: int, d2_ord: int):
        """
        Retorna [(nome_imovel, rec, des)] por fazenda.
        Implementa caminho Supabase (agregação no cliente) e fallback SQL local.
        """
        sb = getattr(self.db, "sb", None)
        pid = getattr(self.db, "perfil_id", None)

        if sb:
            # Mapa id -> nome_imovel
            if pid:
                im_rows = (sb.table("imovel_rural").select("id,nome_imovel").eq("perfil_id", pid).execute().data) or []
            else:
                im_rows = (sb.table("imovel_rural").select("id,nome_imovel").execute().data) or []
            imap = {i["id"]: (i.get("nome_imovel") or f"IMÓVEL {i['id']}") for i in im_rows}

            # Lançamentos
            rows = (sb.table("lancamento")
                        .select("cod_imovel,cod_conta,valor_entrada,valor_saida,data_ord")
                        .gte("data_ord", d1_ord)
                        .lte("data_ord", d2_ord)
                        .execute().data) or []

            # Filtro por perfil: CONTA ou IMÓVEL
            if pid:
                contas = (sb.table("conta_bancaria").select("id").eq("perfil_id", pid).execute().data) or []
                cset = {c["id"] for c in contas}
                rows = [r for r in rows if (r.get("cod_imovel") in imap) or (r.get("cod_conta") in cset)]

            # Agrega por nome_imovel
            agg = {}  # nome -> [rec, des]
            for r in rows:
                fid = r.get("cod_imovel")
                nome = imap.get(fid, f"IMÓVEL {fid}" if fid is not None else "—")
                rec = float(r.get("valor_entrada") or 0.0)
                des = float(r.get("valor_saida") or 0.0)
                tot = agg.setdefault(nome, [0.0, 0.0])
                tot[0] += rec
                tot[1] += des

            out = [ (nome, vals[0], vals[1]) for nome, vals in agg.items() ]
            out.sort(key=lambda x: str(x[0]))
            return out

        # Caminho SQL (local)
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
    .card {{ border:1px solid #11398a; border-radius:10px; padding:16px; margin:14px 0; background:#0d1b3d; page-break-inside: avoid; }}
    h1 {{ margin:0 0 12px 0; font-size:18pt; font-weight:600; }}
    table {{ width:100%; border-collapse:collapse; margin-top:8px; }}
    th, td {{ padding:8px 10px; border-bottom:1px solid #11398a; text-align:center; }}
    th {{ background:#11398a; color:#fff; }}
    .ok  {{ color:#27AE60; font-weight:700; }}
    .bad {{ color:#E74C3C; font-weight:700; }}
    .muted {{ color:#9aa1b1; font-size:11px; margin-top:6px; }}
    </style></head><body>
    <h1>{titulo}</h1>
    """

    def _html_dre_mes(self, rows, por_fazenda: bool) -> str:
        title = "DRE Simplificada (mês a mês)" + (" — por Fazenda" if por_fazenda else "")
        html = self._html_header(title)

        def _lab(ym):
            try:
                return self._mes_label(str(ym))
                
            except Exception:
                return str(ym)

        if por_fazenda:
            # Aceita linhas nos formatos: (faz, ym, rec, des) | (ym, faz, rec, des) | (faz, rec, des) | etc.
            norm = []
            for r in (rows or []):
                if not isinstance(r, (list, tuple)) or not r:
                    continue
                
                faz = ""
                ym  = ""
                nums = []

                for x in r:
                    xs = str(x or "").strip()

                    # ym = sequência de 6 dígitos (ex.: 202501)
                    if xs.isdigit() and len(xs) == 6:
                        ym = xs
                        continue
                    
                    # número (aceita vírgula ou ponto)
                    xs_num = xs.replace('.', '', 1).replace(',', '.', 1)
                    if isinstance(x, (int, float)) or xs_num.replace('.', '', 1).isdigit():
                        try:
                            nums.append(float(xs_num))
                        except Exception:
                            pass
                        continue
                    
                    # texto: provável nome da fazenda
                    if not faz and xs:
                        faz = xs

                # completa faltantes
                rec = nums[0] if len(nums) > 0 else 0.0
                des = nums[1] if len(nums) > 1 else 0.0
                norm.append((faz, ym, rec, des))

            # ordena e renderiza (mantém sua lógica de totalização/tabela)
            norm.sort(key=lambda x: (str(x[0]), str(x[1])))

            atual = None
            total_r = total_d = 0.0
            for faz, ym, rec, des in norm:
                if atual != faz:
                    if atual is not None:
                        resultado = total_r - total_d
                        html += (
                            f"<tr>"
                            f"<th>Total</th>"
                            f"<th>{fmt_money(total_r)}</th>"
                            f"<th>{fmt_money(total_d)}</th>"
                            f"<th class='{'ok' if resultado>=0 else 'bad'}'>{fmt_money(resultado)}</th>"
                            f"</tr></table></div>"
                        )
                    atual = faz
                    total_r = total_d = 0.0
                    html += f"<div class='card'><h2>{faz or '—'}</h2><table><tr><th>Mês</th><th>Receitas</th><th>Despesas</th><th>Resultado</th></tr>"

                r_ = float(rec or 0.0)
                d_ = float(des or 0.0)
                res = r_ - d_
                total_r += r_
                total_d += d_
                html += (
                    f"<tr>"
                    f"<td>{_lab(ym)}</td>"
                    f"<td>{fmt_money(r_)}</td>"
                    f"<td>{fmt_money(d_)}</td>"
                    f"<td class='{'ok' if res>=0 else 'bad'}'>{fmt_money(res)}</td>"
                    f"</tr>"
                )

            if atual is not None:
                resultado = total_r - total_d
                html += (
                    f"<tr>"
                    f"<th>Total</th>"
                    f"<th>{fmt_money(total_r)}</th>"
                    f"<th>{fmt_money(total_d)}</th>"
                    f"<th class='{'ok' if resultado>=0 else 'bad'}'>{fmt_money(resultado)}</th>"
                    f"</tr></table></div>"
                )


        html += self._html_footer()
        return html

    def _html_footer(self) -> str:
        return "</body></html>"

    def _html_dre_mes(self, rows, por_fazenda: bool) -> str:
        title = "DRE Simplificada (mês a mês)" + (" — por Fazenda" if por_fazenda else "")
        html = self._html_header(title)

        def _lab(ym):
            try:
                return self._mes_label(str(ym))
            except Exception:
                return str(ym)

        if por_fazenda:
            import re
        
            def _is_number_like(x):
                if isinstance(x, (int, float)):
                    return True
                s = str(x or "").strip()
                return bool(re.fullmatch(r"[-+]?[\d.,]+", s))
        
            def _parse_num(x):
                if isinstance(x, (int, float)):
                    return float(x)
                s = str(x or "").strip()
                # remove quaisquer símbolos estranhos
                s = re.sub(r"[^\d,.\-+]", "", s)
                # heurística decimal: vírgula prevalece sobre ponto se vier por último
                if "," in s and "." in s:
                    if s.rfind(",") > s.rfind("."):
                        s = s.replace(".", "").replace(",", ".")
                    else:
                        s = s.replace(",", "")
                elif "," in s:
                    s = s.replace(".", "").replace(",", ".")
                else:
                    s = s.replace(",", "")
                try:
                    return float(s)
                except Exception:
                    return 0.0
        
            # cache local id -> nome_imovel (quando só vier o código)
            _nome_cache = {}
            def _nome_por_id(v):
                try:
                    i = int(str(v))
                except Exception:
                    return None
                # evita confundir com AAAAMM
                if len(str(v)) == 6:
                    return None
                if i in _nome_cache:
                    return _nome_cache[i]
                try:
                    r = self.db.fetch_all("SELECT nome_imovel FROM imovel_rural WHERE id = ?", (i,))
                    nm = (r[0][0] if r and r[0] else f"IMÓVEL {i}")
                except Exception:
                    nm = f"IMÓVEL {i}"
                _nome_cache[i] = nm
                return nm
        
            norm = []
            for row in (rows or []):
                if not isinstance(row, (list, tuple)) or not row:
                    continue
                
                # dois últimos números da linha = rec/des (robusto p/ (faz, ym, rec, des) | (id, ym, rec, des) | (ym, rec, des))
                num_idxs = [(idx, _parse_num(val)) for idx, val in enumerate(row) if _is_number_like(val)]
                rec = des = 0.0
                if len(num_idxs) >= 2:
                    (i1, v1), (i2, v2) = num_idxs[-2], num_idxs[-1]
                    if i1 < i2:
                        rec, des = v1, v2
                    else:
                        rec, des = v2, v1
        
                # ym (AAAAMM)
                ym = ""
                for x in row:
                    xs = str(x or "").strip()
                    if xs.isdigit() and len(xs) == 6:
                        ym = xs
                        break
                    
                # nome da fazenda (qualquer texto com letras). se não houver, tenta id->nome
                faz = ""
                for x in row:
                    xs = str(x or "").strip()
                    if any(ch.isalpha() for ch in xs):
                        faz = xs
                        break
                if not faz:
                    for x in row:
                        nome = _nome_por_id(x)
                        if nome:
                            faz = nome
                            break
                        
                norm.append((faz, ym, rec, des))
        
            norm.sort(key=lambda x: (str(x[0]), str(x[1])))
        
            atual = None
            total_r = total_d = 0.0
            for faz, ym, rec, des in norm:
                if atual != faz:
                    if atual is not None:
                        resultado = total_r - total_d
                        html += (
                            f"<tr>"
                            f"<th>Total</th>"
                            f"<th>{fmt_money(total_r)}</th>"
                            f"<th>{fmt_money(total_d)}</th>"
                            f"<th class='{'ok' if resultado>=0 else 'bad'}'>{fmt_money(resultado)}</th>"
                            f"</tr></table></div>"
                        )
                    atual = faz
                    total_r = total_d = 0.0
                    html += (
                        f"<div class='card'><h2>{faz or '—'}</h2>"
                        f"<table><tr><th>Mês</th><th>Receitas</th><th>Despesas</th><th>Resultado</th></tr>"
                    )
        
                r_ = float(rec or 0.0)
                d_ = float(des or 0.0)
                res = r_ - d_
                total_r += r_
                total_d += d_
                html += (
                    f"<tr>"
                    f"<td>{_lab(ym)}</td>"
                    f"<td>{fmt_money(r_)}</td>"
                    f"<td>{fmt_money(d_)}</td>"
                    f"<td class='{'ok' if res>=0 else 'bad'}'>{fmt_money(res)}</td>"
                    f"</tr>"
                )
        
            if atual is not None:
                resultado = total_r - total_d
                html += (
                    f"<tr>"
                    f"<th>Total</th>"
                    f"<th>{fmt_money(total_r)}</th>"
                    f"<th>{fmt_money(total_d)}</th>"
                    f"<th class='{'ok' if resultado>=0 else 'bad'}'>{fmt_money(resultado)}</th>"
                    f"</tr></table></div>"
                )
        
        else:
            # Quando NÃO separa por fazenda, aceite linhas em qualquer ordem:
            # (ym, rec, des) ou (faz, ym, rec, des) ou (id, ym, rec, des) etc.
            import re
        
            def _is_number_like(x):
                if isinstance(x, (int, float)):
                    return True
                s = str(x or "").strip()
                return bool(re.fullmatch(r"[-+]?[\d.,]+", s))
        
            def _parse_num(x):
                if isinstance(x, (int, float)):
                    return float(x)
                s = str(x or "").strip()
                s = re.sub(r"[^\d,.\-+]", "", s)
                if "," in s and "." in s:
                    if s.rfind(",") > s.rfind("."):
                        s = s.replace(".", "").replace(",", ".")
                    else:
                        s = s.replace(",", "")
                elif "," in s:
                    s = s.replace(".", "").replace(",", ".")
                else:
                    s = s.replace(",", "")
                try:
                    return float(s)
                except Exception:
                    return 0.0
        
            agg = {}  # ym -> [rec, des]
            for row in (rows or []):
                if not isinstance(row, (list, tuple)) or not row:
                    continue
                
                # ym
                ym = ""
                for x in row:
                    xs = str(x or "").strip()
                    if xs.isdigit() and len(xs) == 6:
                        ym = xs
                        break
                    
                # dois últimos números = rec/des
                num_vals = [_parse_num(val) for val in row if _is_number_like(val)]
                rec = des = 0.0
                if len(num_vals) >= 2:
                    rec, des = num_vals[-2], num_vals[-1]
        
                if ym:
                    if ym not in agg:
                        agg[ym] = [0.0, 0.0]
                    agg[ym][0] += rec
                    agg[ym][1] += des
        
            # Renderização
            html += "<div class='card'><table><tr><th>Mês</th><th>Receitas</th><th>Despesas</th><th>Resultado</th></tr>"
            total_r = total_d = 0.0
        
            for ym in sorted(agg.keys()):
                r, d = agg[ym]
                res = r - d
                total_r += r
                total_d += d
                html += (
                    f"<tr>"
                    f"<td>{_lab(ym)}</td>"
                    f"<td>{fmt_money(r)}</td>"
                    f"<td>{fmt_money(d)}</td>"
                    f"<td class='{'ok' if res>=0 else 'bad'}'>{fmt_money(res)}</td>"
                    f"</tr>"
                )
        
            resultado = total_r - total_d
            html += (
                f"<tr>"
                f"<th>Total</th>"
                f"<th>{fmt_money(total_r)}</th>"
                f"<th>{fmt_money(total_d)}</th>"
                f"<th class='{'ok' if resultado>=0 else 'bad'}'>{fmt_money(resultado)}</th>"
                f"</tr></table></div>"
            )
        
        html += self._html_footer()
        return html
        

    def _html_dre_por_fazenda(self, rows) -> str:
        # rows aceitos: (fazenda, rec, des, ...ignora extras)
        html = self._html_header("DRE por Fazenda (multi-seção)")
        ini = self.dt_ini.date().toString("dd/MM/yyyy")
        fim = self.dt_fim.date().toString("dd/MM/yyyy")

        for r in (rows or []):
            if not isinstance(r, (list, tuple)):
                continue
            faz = r[0] if len(r) > 0 else ""
            rec = r[1] if len(r) > 1 else 0
            des = r[2] if len(r) > 2 else 0
            try:
                rec = float(rec or 0)
            except Exception:
                rec = 0.0
            try:
                des = float(des or 0)
            except Exception:
                des = 0.0
            res = rec - des

            html += f"""
            <div class='card'>
              <h2>{faz}</h2>
              <table>
                <tr><th>Indicador</th><th>Valor</th></tr>
                <tr><td>Receitas</td><td>{fmt_money(rec)}</td></tr>
                <tr><td>Despesas</td><td>{fmt_money(des)}</td></tr>
                <tr><td><b>Resultado</b></td><td class='{"ok" if res>=0 else "bad"}'><b>{fmt_money(res)}</b></td></tr>
              </table>
              <div class='muted'>Período: {ini} a {fim}</div>
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
        txt = (data or "").strip()
        _d = QDate.fromString(txt, "dd/MM/yyyy")
        if not _d.isValid():
            _d = QDate.fromString(txt[:10], "yyyy-MM-dd")  # ISO (Postgres/Supabase)
        if not _d.isValid():
            _d = QDate.fromString(txt, "yyyyMMdd")         # fallback (AAAAmmdd)
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
        
        setup_interactive_table(
            self.tabela,
            header_movable=True,           # <<< passa a mover colunas
            select_rows=True,
            extended_selection=True,
            resize_mode=QHeaderView.Stretch,
            stretch_last_section=True,
        )
        
        # sinais específicos:
        self.tabela.cellClicked.connect(self._select_row)
        hdr = self.tabela.horizontalHeader()
        hdr.sectionDoubleClicked.connect(self._toggle_sort)  # sua ordenação cíclica custom
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
        # CONTAS
        vis = [not self.tabela.isColumnHidden(c) for c in range(self.tabela.columnCount())]
        kv_set(f"ui:columns:contas::{CURRENT_PROFILE}", vis)

    def _load_column_filter(self):
        """Aplica preferências (contas) salvas no Supabase à tabela de contas."""
        vis = kv_get(f"ui:columns:contas::{CURRENT_PROFILE}", []) or []
        for c, shown in enumerate(vis):
            if c < self.tabela.columnCount():
                self.tabela.setColumnHidden(c, not bool(shown))
        # sincroniza o menu de checkboxes
        for action in self._filter_menu.actions():
            w = action.defaultWidget()
            if isinstance(w, QCheckBox):
                lbl = w.text()
                if lbl in self._contas_labels:
                    idx = self._contas_labels.index(lbl)
                    w.setChecked(not self.tabela.isColumnHidden(idx))
    
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
        
        setup_interactive_table(
            self.tabela,
            header_movable=True,           # <<< libera mover colunas
            select_rows=True,
            extended_selection=True,
            resize_mode=QHeaderView.Stretch,
            stretch_last_section=True,
        )
        
        # seus sinais/botões seguem:
        self.tabela.cellClicked.connect(self._select_row)
        # se já havia ordenação no header, mantenha o connect existente
        
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
        """Atualiza somente o tópico 'imoveis' no Supabase."""
        vis = [not self.tabela.isColumnHidden(c) for c in range(self.tabela.columnCount())]
        kv_set(f"ui:columns:imoveis::{CURRENT_PROFILE}", vis)


    def _load_imoveis_column_filter(self):
        """Aplica o tópico 'imoveis' salvo no Supabase à tabela."""
        vis = kv_get(f"ui:columns:imoveis::{CURRENT_PROFILE}", []) or []
        for c, shown in enumerate(vis):
            if c < self.tabela.columnCount():
                self.tabela.setColumnHidden(c, not bool(shown))
        # sincroniza o menu de checkboxes
        for wa in self._imoveis_filter_menu.actions():
            w = wa.defaultWidget()
            if isinstance(w, QCheckBox):
                lbl = w.text()
                if lbl in self._imoveis_labels:
                    idx = self._imoveis_labels.index(lbl)
                    w.setChecked(not self.tabela.isColumnHidden(idx))


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

        setup_interactive_table(
            self.tabela,
            header_movable=True,
            select_rows=True,           # <<< garante LINHA inteira
            extended_selection=True,    # CTRL/SHIFT
            resize_mode=QHeaderView.Stretch,  # você já usava Stretch aqui
            stretch_last_section=True,
        )

        # sinais específicos desta aba permanecem:
        self.tabela.cellClicked.connect(self._select_row)
        # ordenação cíclica custom que você já tem segue ativa (sectionDoubleClicked -> _toggle_sort_participantes)

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
        vis = [not self.tabela.isColumnHidden(c) for c in range(self.tabela.columnCount())]
        kv_set(f"ui:columns:participantes::{CURRENT_PROFILE}", vis)


    def _load_participantes_column_filter(self):
        vis = kv_get(f"ui:columns:participantes::{CURRENT_PROFILE}", []) or []
        for c, shown in enumerate(vis):
            if c < self.tabela.columnCount():
                self.tabela.setColumnHidden(c, not bool(shown))
        # sincroniza o menu de checkboxes
        for wa in self._part_filter_menu.actions():
            w = wa.defaultWidget()
            if isinstance(w, QCheckBox):
                lbl = w.text()
                if lbl in self._participantes_labels:
                    idx = self._participantes_labels.index(lbl)
                    w.setChecked(not self.tabela.isColumnHidden(idx))


    def _filter_participantes(self, text: str):
        # usa cache por linha e aplica já (delay=0)
        ListAccelerator.filter(self.tabela, text, delay_ms=0)

    def importar_participantes(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Importar Participantes",
            "",
            "Arquivos suportados (*.txt *.xlsx *.xls);;Apenas TXT (*.txt);;Apenas Excel (*.xlsx *.xls);;Todos os arquivos (*)"
        )
        if not path:
            return
        try:
            if path.lower().endswith(".txt"):
                self._import_participantes_txt(path)
            else:
                self._import_participantes_excel(path)
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
        # atualiza listas/combos abertos imediatamente
        try:
            if hasattr(self, "_broadcast_participantes_changed"):
                self._broadcast_participantes_changed()
            if hasattr(self, "_reload_participantes"):
                self._reload_participantes()
        except Exception:
            pass
        
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

    def _import_participantes_txt(self, path: str):
        """
        Importa participantes de um TXT (delimitadores aceitos: ; | ,) com pelo menos CPF/CNPJ e Nome.
        Faz upsert em cada linha, mostra resumo e ATUALIZA a UI na hora.
        """
        from PySide6.QtWidgets import QMessageBox, QApplication
        import re, os

        if not path or not os.path.exists(path):
            QMessageBox.warning(self, "Importar participantes", "Arquivo inválido.")
            return

        def _digits(s): return re.sub(r"\D+", "", str(s or ""))

        ok, upd, err, total = 0, 0, 0, 0
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    total += 1
                    line = line.strip()
                    if not line:
                        continue
                    # tenta ; depois | depois ,
                    parts = None
                    for sep in (";", "|", ","):
                        p = [x.strip() for x in line.split(sep)]
                        if len(p) >= 2:
                            parts = p
                            break
                    if not parts:
                        err += 1
                        continue

                    # heurística: pega primeiros 2 campos como cpf_cnpj e nome
                    cpf_cnpj = _digits(parts[0])
                    nome     = parts[1]
                    # tipo (PF=1, PJ=2) — tenta descobrir pelo tamanho
                    if len(cpf_cnpj) == 11:
                        tipo = 1
                    elif len(cpf_cnpj) == 14:
                        tipo = 2
                    else:
                        # se vier separado em outra coluna, tenta achar
                        tipo = 1 if any("pf" == (c or "").lower() for c in parts[2:3]) else (2 if any("pj" == (c or "").lower() for c in parts[2:3]) else 1)

                    try:
                        pid_before = self.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj = ?", (cpf_cnpj,))
                        pid = self.db.upsert_participante(cpf_cnpj, nome, tipo)
                        if pid_before and pid_before[0]:
                            upd += 1
                        else:
                            ok += 1
                    except Exception:
                        err += 1
                        continue
        finally:
            QApplication.restoreOverrideCursor()

        QMessageBox.information(
            self, "Importar participantes",
            f"Linhas: {total}\nNovos: {ok}\nAtualizados: {upd}\nErros: {err}"
        )

        # === ATUALIZA UI NA HORA ===
        try:
            if hasattr(self, "_broadcast_participantes_changed"):
                self._broadcast_participantes_changed()
            if hasattr(self, "_reload_participantes"):
                self._reload_participantes()
        except Exception:
            pass

    def _import_participantes_excel(self, path: str):
        """
        Importa participantes de Excel. Colunas esperadas (case-insensitive):
        cpf_cnpj, nome, [tipo]. Se 'tipo' faltar, deduz por tamanho do cpf_cnpj.
        Atualiza a UI ao final.
        """
        from PySide6.QtWidgets import QMessageBox, QApplication
        import pandas as pd
        import re, os

        if not path or not os.path.exists(path):
            QMessageBox.warning(self, "Importar participantes (Excel)", "Arquivo inválido.")
            return

        def _digits(s): return re.sub(r"\D+", "", str(s or ""))

        ok, upd, err, total = 0, 0, 0, 0
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            df = pd.read_excel(path)
            # normaliza nomes de colunas
            cols = {str(c).strip().lower(): c for c in df.columns}
            if "cpf_cnpj" not in cols or "nome" not in cols:
                QMessageBox.warning(self, "Importar participantes (Excel)", "Planilha precisa ter colunas 'cpf_cnpj' e 'nome'.")
                return

            for _, row in df.iterrows():
                total += 1
                cpf_cnpj = _digits(row[cols["cpf_cnpj"]])
                nome     = str(row[cols["nome"]]).strip()
                if not cpf_cnpj or not nome:
                    err += 1
                    continue

                if "tipo" in cols:
                    raw_tipo = str(row[cols["tipo"]]).strip().lower()
                    if raw_tipo in ("1", "pf", "pessoa fisica", "pessoa física"):
                        tipo = 2  # PF
                    elif raw_tipo in ("2", "pj", "pessoa juridica", "pessoa jurídica", "cnpj"):
                        tipo = 1  # PJ
                    else:
                        tipo = 2 if len(cpf_cnpj) == 11 else 1
                    

                try:
                    pid_before = self.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj = ?", (cpf_cnpj,))
                    pid = self.db.upsert_participante(cpf_cnpj, nome, tipo)
                    if pid_before and pid_before[0]:
                        upd += 1
                    else:
                        ok += 1
                except Exception:
                    err += 1
                    continue
        finally:
            QApplication.restoreOverrideCursor()

        QMessageBox.information(
            self, "Importar participantes (Excel)",
            f"Linhas: {total}\nNovos: {ok}\nAtualizados: {upd}\nErros: {err}"
        )

        # === ATUALIZA UI NA HORA ===
        try:
            if hasattr(self, "_broadcast_participantes_changed"):
                self._broadcast_participantes_changed()
            if hasattr(self, "_reload_participantes"):
                self._reload_participantes()
        except Exception:
            pass

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
        if dlg.exec():
            if getattr(dlg, "novo_id", None):
                # refresh imediato da grade local
                self.carregar_participantes()
                # mantém o broadcast para outras telas/janelas ouvindo
                self._broadcast_participantes_changed()


    def editar_participante(self):
        id_ = self.tabela.item(self.selected_row,0).data(Qt.UserRole)
        dlg = CadastroParticipanteDialog(self, id_)
        if dlg.exec():
            if getattr(dlg, "novo_id", None):
                self.carregar_participantes()
                self._broadcast_participantes_changed()


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

    def _reload_participantes(self):
        # usado pelo broadcast para atualizar imediatamente a grade
        self.carregar_participantes()

    def _broadcast_participantes_changed(self):
        # delega para a janela principal, se existir
        win = self.window()
        if hasattr(win, "_broadcast_participantes_changed"):
            try:
                win._broadcast_participantes_changed()
            except Exception:
                pass

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
        # Realtime: começa a escutar mudanças do banco
        self._rt = RealtimeBridge(self.db.sb, [
            "lancamento","conta_bancaria","imovel_rural","cultura","area_producao","estoque","participante","perfil_param"
        ])
        self._rt.start(self._on_realtime_change)

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
        self.tab_lanc = QTableWidget(0, len(self._lanc_labels))
        self.tab_lanc.setHorizontalHeaderLabels(self._lanc_labels)
        
        # Substitui bloco repetitivo por setup único:
        setup_interactive_table(
            self.tab_lanc,
            header_movable=True,
            select_rows=True,
            extended_selection=True,
            resize_mode=QHeaderView.ResizeToContents,
            stretch_last_section=False,  # você usa False aqui e ativa stretch depois quando quiser
        )
        
        # sinais/botões específicos desta tela continuam:
        self.tab_lanc.itemSelectionChanged.connect(self._update_lanc_buttons)
        
        l_l.addWidget(self.tab_lanc)
        
        # aplica filtro inicial ao abrir
        self.carregar_lancamentos()
        self.dashboard.load_data()
        # contador no layout (sem sobrepor a barra de filtros)
        attach_counter_in_layout(self.tab_lanc, self.lanc_filter_layout)

        # === Preferências de visibilidade das colunas via Supabase (meta_kv) ===
        try:
            vis = kv_get(f"ui::lanc_columns::{CURRENT_PROFILE}", {}) or {}
            if isinstance(vis, dict) and vis:
                labels = [self.tab_lanc.horizontalHeaderItem(i).text() for i in range(self.tab_lanc.columnCount())]
                for i, label in enumerate(labels):
                    self.tab_lanc.setColumnHidden(i, not bool(vis.get(label, True)))
        except Exception as e:
            print("prefs colunas (load) erro:", e)

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

    def _on_realtime_change(self, payload):
        # payload["table"] tem o nome da tabela; payload["eventType"] = INSERT|UPDATE|DELETE
        t = payload.get("table")
        try:
            if t in ("lancamento","conta_bancaria"):
                # atualize dashboards/saldos/combos conforme sua UI:
                if hasattr(self, "dashboard"): 
                    try: self.dashboard.recarregar()
                    except Exception: pass
            if t in ("imovel_rural","conta_bancaria","participante"):
                # recarrega combos/listas simples
                if hasattr(self, "cadw"):
                    try:
                        w0 = self.cadw.widget(0); w0.carregar_imoveis()
                        w1 = self.cadw.widget(1); w1.carregar_contas()
                        w2 = self.cadw.widget(2); w2.carregar_participantes()  # <<< NOVO
                    except Exception:
                        pass

        except Exception as e:
            print("on_realtime_change error:", e)

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
        # monta vetor de visibilidade (True = coluna visível)
        vis = [not self.tab_lanc.isColumnHidden(c) for c in range(self.tab_lanc.columnCount())]
        kv_set(f"ui:lanc_columns::{CURRENT_PROFILE}", vis)

    
    def _load_lanc_filter_settings(self):
        # lê vetor de visibilidade do Supabase
        vis = kv_get(f"ui:lanc_columns::{CURRENT_PROFILE}", [])
        if isinstance(vis, list) and vis:
            for c, visible in enumerate(vis):
                if c < self.tab_lanc.columnCount():
                    self.tab_lanc.setColumnHidden(c, not bool(visible))
    
        # sincroniza os checkboxes do menu com o estado atual das colunas
        if hasattr(self, "_lanc_filter_menu") and hasattr(self, "_lanc_labels"):
            for wa in self._lanc_filter_menu.actions():
                if isinstance(wa, QWidgetAction):
                    w = wa.defaultWidget()
                    if isinstance(w, QCheckBox):
                        label = w.text()
                        if label in self._lanc_labels:
                            col = self._lanc_labels.index(label)
                            w.setChecked(not self.tab_lanc.isColumnHidden(col))
    
    
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

    def _update_lanc_buttons(self):
        sel = self.tab_lanc.selectionModel()
        has = bool(sel and sel.hasSelection())
        self.btn_edit_lanc.setEnabled(has)
        self.btn_del_lanc.setEnabled(has)

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
        self.db.set_profile(profile)
        self.dashboard.db.set_profile(profile)

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
        im_w = self.cadw.widget(0); im_w.db.set_profile(profile); im_w.carregar_imoveis()
        ct_w = self.cadw.widget(1); ct_w.db.set_profile(profile); ct_w.carregar_contas()

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
        btn_export_plan.clicked.connect(lambda: (dlg.accept(), self._exportar_planilha_lcdPR()))
    
        dlg.exec()
    
    def carregar_lancamentos(self):
        self.tab_lanc.clearSelection()
        self.btn_edit_lanc.setEnabled(False)
        self.btn_del_lanc.setEnabled(False)

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
        row = self.tab_lanc.currentRow()
        if row < 0:
            return
        it = self.tab_lanc.item(row, 0)
        if not it:
            return
        lanc_id = int(it.text())
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
        self.carregar_lancamentos(); self.dashboard.load_data()

    def carregar_planejamento(self):
        """
        Planejamento (áreas de produção) vindo do Supabase.
        Colunas: Cultura | Área (ha) | Plantio | Colheita Est. | Produt. Est.
        Filtra pelos imóveis do perfil selecionado (self.db.perfil_id).
        """
        try:
            sb = self.db.sb

            # 1) Descobrir imóveis do perfil (se houver perfil selecionado)
            imovel_ids = []
            if getattr(self.db, "perfil_id", None):
                im_rows = (sb.table("imovel_rural")
                             .select("id")
                             .eq("perfil_id", self.db.perfil_id)
                             .execute().data) or []
                imovel_ids = [r["id"] for r in im_rows]

            # 2) Buscar áreas de produção
            ap_q = (sb.table("area_producao")
                      .select("id,imovel_id,cultura_id,area,data_plantio,data_colheita_estimada,produtividade_estimada"))
            ap_rows = (ap_q.execute().data) or []

            # Se houver perfil, filtra client-side pelos imóveis daquele perfil
            if imovel_ids:
                ap_rows = [r for r in ap_rows if r.get("imovel_id") in imovel_ids]

            # 3) Mapas auxiliares (imóveis e culturas)
            # Imóveis (id -> nome)
            im_map = {}
            if ap_rows:
                im_ids = sorted({r.get("imovel_id") for r in ap_rows if r.get("imovel_id") is not None})
                if im_ids:
                    im_all = (sb.table("imovel_rural")
                                .select("id,nome_imovel")
                                .in_("id", im_ids)
                                .execute().data) or []
                    im_map = {r["id"]: r["nome_imovel"] for r in im_all}

            # Culturas (id -> nome)
            cu_map = {}
            if ap_rows:
                cu_ids = sorted({r.get("cultura_id") for r in ap_rows if r.get("cultura_id") is not None})
                if cu_ids:
                    cu_all = (sb.table("cultura")
                                .select("id,nome")
                                .in_("id", cu_ids)
                                .execute().data) or []
                    cu_map = {r["id"]: r["nome"] for r in cu_all}

            # 4) Montar a tabela
            headers = ["Cultura", "Área (ha)", "Plantio", "Colheita Est.", "Produt. Est."]
            self.tab_plan.setColumnCount(len(headers))
            self.tab_plan.setHorizontalHeaderLabels(headers)
            self.tab_plan.setRowCount(0)

            def _fmt_data(val):
                # Aceita 'YYYY-MM-DD' (Postgres) e transforma pra 'DD/MM/YYYY'
                if not val:
                    return ""
                s = str(val)
                if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                    y, m, d = s[:10].split("-")
                    return f"{int(d):02d}/{int(m):02d}/{y}"
                return s

            for r in ap_rows:
                cultura = cu_map.get(r.get("cultura_id"), "")
                area = r.get("area") or 0
                pl = _fmt_data(r.get("data_plantio"))
                ce = _fmt_data(r.get("data_colheita_estimada"))
                prod = r.get("produtividade_estimada") or 0

                row = self.tab_plan.rowCount()
                self.tab_plan.insertRow(row)
                self.tab_plan.setItem(row, 0, QTableWidgetItem(str(cultura)))
                self.tab_plan.setItem(row, 1, QTableWidgetItem(f"{area} ha"))
                self.tab_plan.setItem(row, 2, QTableWidgetItem(str(pl)))
                self.tab_plan.setItem(row, 3, QTableWidgetItem(str(ce)))
                self.tab_plan.setItem(row, 4, QTableWidgetItem(str(prod)))

            self.tab_plan.resizeColumnsToContents()

        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Falha ao carregar planejamento (Supabase): {e}")


    def novo_lancamento(self):
        dlg = LancamentoDialog(self)
        if dlg.exec(): self.carregar_lancamentos(); self.dashboard.load_data()

    def cad_imovel(self): self.tabs.setCurrentIndex(2); self.cadw.setCurrentIndex(0)
    def cad_conta(self): self.tabs.setCurrentIndex(2); self.cadw.setCurrentIndex(1)
    def cad_participante(self): self.tabs.setCurrentIndex(2); self.cadw.setCurrentIndex(2)
    
    def gerar_txt(self, path: str):
        """Wrapper com diagnóstico — chama a implementação real e captura exatamente onde estourou."""
        import traceback
        from PySide6.QtWidgets import QMessageBox
        try:
            return self._gerar_txt_impl(path)
        except Exception as e:
            tb = traceback.format_exc()
            # Mostra um diagnóstico curto na tela e deixa claro o ponto da falha
            QMessageBox.critical(self, "Erro ao gerar TXT",
                f"{e.__class__.__name__}: {e}\n\nTraceback (últimas linhas):\n{tb[-1200:]}")
            raise  # repropaga para não mascarar
        
    def _gerar_txt_impl(self, path: str):
        """
        IMPLEMENTAÇÃO ÚNICA E ROBUSTA — sem desempacotes perigosos, sem índices fixos.
        Substitui 100% a sua função anterior.
        """
        import os, re, unicodedata
        from decimal import Decimal, ROUND_HALF_UP
        from PySide6.QtWidgets import QMessageBox
        from PySide6.QtCore import QSettings
    
        # ===== helpers =====
        NL = "\r\n"
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
        def _clean(s):
            s = re.sub(r"[|\r\n]+", " ", str(s or "")).strip()
            s = s.replace("—","-").replace("º","o").replace("ª","a")
            return unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
        def _cents(val):
            try: q = Decimal(str(val if val is not None else 0)).quantize(Decimal("0.01"))
            except Exception: q = Decimal("0.00")
            v = int(q*100)
            return "000" if v == 0 else str(v)
        def _perc5(v):
            try: d = Decimal(str(v).replace(",", "."))
            except Exception: d = Decimal("0")
            return f"{int((d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)*100).to_integral_value()):05d}"
    
        # ===== cabeçalho =====
        settings = QSettings("AgroLCDPR", "UI")
        versao = str(settings.value("param/version", "0013") or "0013").strip()
        ident  = _digits(settings.value("param/ident", "") or "")
    
        # fallback: pega primeiro CPF (11 dígitos) existente
        if not ident:
            row = self.db.fetch_one(
                "SELECT cpf_cnpj FROM participante ORDER BY id LIMIT 1"
            )
            if row and isinstance(row, (list, tuple)) and len(row) >= 1:
                ident = _digits(row[0])
    
        nome         = _clean(settings.value("param/nome", ""))
        ind_ini_per  = str(settings.value("param/ind_ini_per", "0"))
        sit_especial = str(settings.value("param/sit_especial", "0"))
    
        dt_ini_txt = self.dt_ini.date().toString("ddMMyyyy")
        dt_fim_txt = self.dt_fim.date().toString("ddMMyyyy")
        if ind_ini_per == "0":
            ano = self.dt_ini.date().toString("yyyy")
            dt_ini_txt = f"0101{ano}"
    
        if not ident:
            QMessageBox.warning(self, "LCDPR",
                "Informe o CPF do declarante em Configurações > Declarante ou cadastre um participante Pessoa Física.")
            return
    
        # ===== abre arquivo =====
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
                # ===== 0000 (usa perfil_param primeiro) =====
                row_pp = self.db.get_perfil_param(CURRENT_PROFILE)
                keys_pp = [
                    "version","ind_ini_per","sit_especial","ident","nome",
                    "logradouro","numero","complemento","bairro","uf",
                    "cod_mun","cep","telefone","email"
                ]
                pp = {k: "" for k in keys_pp}
                if isinstance(row_pp, (list, tuple)):
                    for i, val in enumerate(row_pp):
                        if i < len(keys_pp):
                            pp[keys_pp[i]] = "" if val is None else str(val)
        
                def _g(campo_pp: str, key_ini: str):
                    v = (pp.get(campo_pp, "") or "").strip()
                    if not v:
                        v = (settings.value(key_ini, "") or "").strip()
                    return v
        
                versao      = (settings.value("param/version", "0013") or "0013")
                ind_ini_per = (settings.value("param/ind_ini_per", "0") or "0")
                sit_especial= (settings.value("param/sit_especial", "0") or "0")
        
                ident = _digits(_g("ident", "param/ident"))
                nome  = _clean(_g("nome",  "param/nome"))
        
                # Datas: se Regular (0), inicio deve ser 01/01/AAAA
                dt_ini_txt = self.dt_ini.date().toString("ddMMyyyy")
                dt_fim_txt = self.dt_fim.date().toString("ddMMyyyy")
                if str(ind_ini_per) == "0":
                    ano = self.dt_ini.date().toString("yyyy")
                    dt_ini_txt = f"0101{ano}"
        
                if not ident:
                    QMessageBox.warning(self, "LCDPR",
                        "Informe o CPF do declarante em Configurações > Declarante ou cadastre um participante PF.")
                    return
        
                # 0000
                f.write("0000|" + "|".join([
                    "LCDPR", str(versao), _digits(ident), nome,
                    str(ind_ini_per), str(sit_especial), "",
                    dt_ini_txt, dt_fim_txt
                ]) + NL)
        
                # 0010
                f.write("0010|1" + NL)
        
                # ===== 0030 (endereço) — layout: logradouro|numero|complemento|bairro|UF|cod_mun|cep|telefone|email =====
                logradouro  = _clean(_g("logradouro",  "param/logradouro"))
                numero      = _clean(_g("numero",      "param/numero"))
                complemento = _clean(_g("complemento", "param/complemento"))
                bairro      = _clean(_g("bairro",      "param/bairro"))
                uf          = (_clean(_g("uf",         "param/uf")) or "")[:2].upper()
                cod_mun     = _digits(_g("cod_mun",    "param/cod_mun")).zfill(7)
                cep         = _digits(_g("cep",        "param/cep")).zfill(8)
                telefone    = _digits(_g("telefone",   "param/telefone"))
                email       = _clean(_g("email",       "param/email"))
        
                if not logradouro:
                    raise ValueError("Endereço (logradouro) obrigatório para o registro 0030.")
        
                f.write("0030|" + "|".join([
                    logradouro, _digits(numero), complemento, bairro,
                    uf, cod_mun, cep, telefone, email
                ]) + NL)
        
                # 0040 — imóveis (sem indexação perigosa)
                for row in (self.db.fetch_all(
                    "SELECT cod_imovel,pais,moeda,cad_itr,caepf,insc_estadual,"
                    "       nome_imovel,endereco,num,compl,bairro,uf,cod_mun,cep,"
                    "       tipo_exploracao,participacao "
                    "FROM imovel_rural"
                ) or []):
                    buf = (list(row) + [None]*16)[:16]
                    (cod, pais, moeda, cad_itr, caepf, ie, nome_imovel, end_, num_, comp_,
                     bai, uf_, mun, cep_, tipo_expl, part) = buf
                    f.write("0040|" + "|".join([
                        _digits(cod).zfill(3),
                        _clean(pais),
                        _clean(moeda),
                        _digits(cad_itr).zfill(8),
                        _digits(caepf).zfill(14),
                        _digits(ie),
                        _clean(nome_imovel),
                        _clean(end_),
                        _clean(num_),
                        _clean(comp_),
                        _clean(bai),
                        _clean(uf_).upper()[:2],
                        _digits(mun).zfill(7),
                        _digits(cep_).zfill(8),
                        str(tipo_expl or ""),
                        _perc5(part or 0),
                    ]) + NL)

                # 0050 – contas (AG=4, CONTA=16; se vier maior, mantém os 4/16 dígitos da DIREITA)
                for row in self.db.fetch_all(
                    "SELECT cod_conta,pais_cta,banco,nome_banco,agencia,num_conta FROM conta_bancaria"
                ) or []:
                    (cod, pais_cta, banco, nome_bco, ag, num_cta) = (list(row)+[None]*6)[:6]
                    ag_d  = _digits(ag)
                    cta_d = _digits(num_cta)
                    if not ag_d or not cta_d:
                        continue
                    ag_d  = ag_d[-4:].zfill(4)         # força 4 dígitos
                    cta_d = cta_d[-16:].zfill(16)      # força 16 dígitos

                    f.write("0050|" + "|".join([
                        _digits(cod).zfill(3),
                        _clean(pais_cta),
                        (_digits(banco).zfill(3) if banco else ""),
                        _clean(nome_bco),
                        ag_d,
                        cta_d,
                    ]) + NL)

                # Q100 — vamos pelo cliente REST do Supabase (sem JOIN SQL) para evitar shape inesperado
                d1_ord = int(self.dt_ini.date().toString("yyyyMMdd"))
                d2_ord = int(self.dt_fim.date().toString("yyyyMMdd"))
                sb_cli = self.db.sb

                lans = (sb_cli.table("lancamento")
                            .select("id,data,data_ord,cod_imovel,cod_conta,id_participante,num_doc,tipo_doc,historico,tipo_lanc,valor_entrada,valor_saida,saldo_final,natureza_saldo")
                            .gte("data_ord", d1_ord)
                            .lte("data_ord", d2_ord)
                            .order("data_ord").order("id")
                            .execute().data) or []

                im_ids = sorted({r.get("cod_imovel") for r in lans if r.get("cod_imovel") is not None})
                ct_ids = sorted({r.get("cod_conta") for r in lans if r.get("cod_conta") is not None})
                pa_ids = sorted({r.get("id_participante") for r in lans if r.get("id_participante") is not None})

                im_map = {}
                if im_ids:
                    for r in (sb_cli.table("imovel_rural").select("id,cod_imovel").in_("id", im_ids).execute().data or []):
                        im_map[r["id"]] = r.get("cod_imovel")
                ct_map = {}
                if ct_ids:
                    for r in (sb_cli.table("conta_bancaria").select("id,cod_conta").in_("id", ct_ids).execute().data or []):
                        ct_map[r["id"]] = r.get("cod_conta")
                pa_map = {}
                if pa_ids:
                    for r in (sb_cli.table("participante").select("id,cpf_cnpj").in_("id", pa_ids).execute().data or []):
                        pa_map[r["id"]] = r.get("cpf_cnpj")

                for r in lans:
                    f.write("Q100|" + "|".join([
                        _ddmmyyyy(r.get("data")),
                        (_digits(im_map.get(r.get("cod_imovel"))).zfill(3) if r.get("cod_imovel") in im_map else ""),
                        (_digits(ct_map.get(r.get("cod_conta"))).zfill(3) if r.get("cod_conta") in ct_map else ""),
                        _clean(r.get("num_doc")),
                        str(r.get("tipo_doc") or ""),
                        _clean(r.get("historico")),
                        _digits(pa_map.get(r.get("id_participante")) or ident),
                        str(r.get("tipo_lanc") or ""),
                        _cents(r.get("valor_entrada")),
                        _cents(r.get("valor_saida")),
                        _cents(r.get("saldo_final")),
                        str((r.get("natureza_saldo") or "")).upper(),
                    ]) + NL)

                # === Q200 — resumo mensal (mmaaaa) no mesmo layout do arquivo modelo ===
                # Fonte: os mesmos lançamentos já carregados (lans), filtrados por data_ord no período
                from decimal import Decimal
                from collections import defaultdict

                # Se você NÃO tiver a lista 'lans' disponível aqui, descomente o bloco abaixo para recarregar via REST:
                # d1_ord = int(self.dt_ini.date().toString("yyyyMMdd"))
                # d2_ord = int(self.dt_fim.date().toString("yyyyMMdd"))
                # lans = (self.db.sb.table("lancamento")
                #             .select("id,data_ord,valor_entrada,valor_saida")
                #             .gte("data_ord", d1_ord)
                #             .lte("data_ord", d2_ord)
                #             .order("data_ord").order("id")
                #             .execute().data) or []

                agg = defaultdict(lambda: {"ent": Decimal("0"), "sai": Decimal("0")})

                for r in (lans or []):
                    # data_ord vem como int AAAAMMDD ou AAAAMM; vamos normalizar para AAAAMM
                    ordv = str(r.get("data_ord") or "")
                    ym = ordv[:6] if len(ordv) >= 6 else ""
                    if not ym:
                        continue
                    ent = Decimal(str(r.get("valor_entrada") or 0))
                    sai = Decimal(str(r.get("valor_saida") or 0))
                    agg[ym]["ent"] += ent
                    agg[ym]["sai"] += sai

                saldo_acum = Decimal("0")
                for ym in sorted(agg.keys()):  # ex.: "202501", "202502", ...
                    y, m = ym[:4], ym[4:6]
                    mmaaaa  = f"{m}{y}"  # ex.: "012025" (mês na frente)
                    tot_ent = agg[ym]["ent"]
                    tot_sai = agg[ym]["sai"]
                    saldo_acum += (tot_ent - tot_sai)
                    nat = "P" if saldo_acum >= 0 else "N"
                    f.write("Q200|" + "|".join([
                        mmaaaa,
                        _cents(tot_ent),                      # zero sai como "000"
                        _cents(tot_sai),
                        _cents(abs(saldo_acum)),              # acumulado ABS como no modelo
                        nat
                    ]) + NL)

        # 9999 — total de linhas
        total = 0
        with open(path, "r", encoding="utf-8") as fr:
            for _ in fr:
                total += 1
        total += 1
        with open(path, "a", encoding="utf-8", newline="") as fa:
            fa.write(f"9999||||||{total}{NL}")
    
        # Lembra o último caminho (se existir utilitário no seu projeto)
        try:
            save_last_txt_path(path)
        except Exception:
            pass
        
        from PySide6.QtWidgets import QMessageBox
        QMessageBox.information(self, "LCDPR", f"Arquivo gerado com sucesso em:\n{path}")
    
    def _exportar_planilha_lcdPR(self):
        """
        Exporta TODOS os lançamentos do período selecionado em uma planilha Excel,
        com as colunas solicitadas e mapeamentos legíveis.
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

        # 2) Intervalo (usa data_ord)
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

        # 4) Busca via Supabase (sem SQL)
        sb = self.db.sb

        # Lançamentos do período (campos necessários)
        lans = (sb.table("lancamento")
                  .select("id,data,data_ord,cod_imovel,cod_conta,id_participante,num_doc,tipo_doc,historico,tipo_lanc,valor_entrada,valor_saida")
                  .gte("data_ord", d1_ord)
                  .lte("data_ord", d2_ord)
                  .order("data_ord")
                  .order("id")
                  .execute().data) or []

        if not lans:
            QMessageBox.information(self, "Exportar Planilha LCDPR",
                                    "Nenhum lançamento encontrado no período selecionado.")
            return

        # IDs p/ mapear
        im_ids  = {r["cod_imovel"] for r in lans if r.get("cod_imovel") is not None}
        ct_ids  = {r["cod_conta"] for r in lans if r.get("cod_conta") is not None}
        p_ids   = {r["id_participante"] for r in lans if r.get("id_participante") is not None}

        # Dicionários de apoio
        im_map = {}
        if im_ids:
            ims = (sb.table("imovel_rural")
                     .select("id,cod_imovel,nome_imovel")
                     .in_("id", list(im_ids))
                     .execute().data) or []
            im_map = {r["id"]: (r.get("cod_imovel") or "", r.get("nome_imovel") or "") for r in ims}

        ct_map = {}
        if ct_ids:
            cts = (sb.table("conta_bancaria")
                     .select("id,cod_conta,nome_banco")
                     .in_("id", list(ct_ids))
                     .execute().data) or []
            ct_map = {r["id"]: (r.get("cod_conta") or "", r.get("nome_banco") or "") for r in cts}

        p_map = {}
        if p_ids:
            ps = (sb.table("participante")
                    .select("id,nome,cpf_cnpj")
                    .in_("id", list(p_ids))
                    .execute().data) or []
            p_map = {r["id"]: (r.get("nome") or "", r.get("cpf_cnpj") or "") for r in ps}

        def _fmt_data_br(s: str) -> str:
            # normaliza para DD/MM/YYYY
            from datetime import datetime
            if not s:
                return ""
            s = str(s).strip()
            try:
                if "/" in s:
                    # já está em dd/mm/yyyy?
                    d, m, y = s.split("/")[:3]
                    if len(d) == 2 and len(m) == 2 and len(y) == 4:
                        return f"{d}/{m}/{y}"
                if "-" in s:
                    parts = s.split("-")
                    if len(parts[0]) == 4:  # yyyy-mm-dd
                        y, m, d = parts[:3]
                        return f"{d}/{m}/{y}"
                    else:  # dd-mm-yyyy
                        d, m, y = parts[:3]
                        return f"{d}/{m}/{y}"
                # tenta ISO puro
                dt = datetime.fromisoformat(s)
                return dt.strftime("%d/%m/%Y")
            except Exception:
                return s

        # 5) Monta linhas finais
        data_rows = []
        for r in lans:
            data_fmt = _fmt_data_br(r.get("data") or "")
            cod_imovel, _nome_imovel = im_map.get(r.get("cod_imovel"), ("", ""))
            cod_conta, conta_bancaria = ct_map.get(r.get("cod_conta"), ("", ""))
            participante, cpf_cnpj = p_map.get(r.get("id_participante"), ("", ""))

            tipo_doc_desc  = map_tipo_doc.get(int(r["tipo_doc"]) if r.get("tipo_doc") is not None else 0, "")
            tipo_lanc_desc = map_tipo_lanc.get(int(r["tipo_lanc"]) if r.get("tipo_lanc") is not None else 0, "")

            data_rows.append({
                "DATA":                data_fmt or "",
                "IMOVEL RURAL":        cod_imovel or "",
                "CONTA BANCARIA":      conta_bancaria or "",
                "CODIGO DA CONTA":     cod_conta or "",
                "PARTICIPANTE":        participante or "",
                "CPF/CNPJ":            cpf_cnpj or "",
                "NUMERO DO DOCUMENTO": (r.get("num_doc") or "").strip(),
                "TIPO":                tipo_doc_desc,
                "HISTORICO":           r.get("historico") or "",
                "TIPO DE LANÇAMENTO":  tipo_lanc_desc,
                "VALOR ENTRADA":       float(r.get("valor_entrada") or 0),
                "VALOR SAIDA":         float(r.get("valor_saida") or 0),
            })

        df = pd.DataFrame(data_rows, columns=[
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

        # 6) Salva no Excel
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

    @staticmethod
    def _extract_name_from_historico(h: str) -> str:
        if not h:
            return ""
        # pega o ÚLTIMO grupo entre parênteses
        m = re.search(r"\(([^)]+)\)\s*$", h.strip())
        if m:
            return m.group(1).strip()
        # fallback: depois de dois espaços até o '|', se existir
        m = re.search(r"\s{2,}([A-ZÀ-Ú0-9 .'-]+?)(?:\s*\|.*)?$", h.upper())
        return (m.group(1).title().strip() if m else "")

    def _ensure_participante(self, digits: str, historico: str = "") -> int:
        import re
        digits = re.sub(r"\D", "", str(digits or ""))
        if not digits:
            return 0

        # já existe?
        row = self.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj=?", (digits,))
        if row and row[0]:
            return int(row[0])

        is_pf = (len(digits) == 11)
        tipo_contraparte = 2 if is_pf else 1
        nome = ""

        if is_pf:
            # 1) histórico
            try:
                nome = self._extract_name_from_historico(historico)
            except Exception:
                nome = ""
            # 2) Receita (opcional)
            if not nome:
                try:
                    info = consulta_receita(digits, tipo='cpf')
                    nome = (info.get('nome') or info.get('nomeCompleto') or "").strip()
                except Exception:
                    pass
            if not nome:
                nome = f"CPF {digits}"
        else:
            try:
                info = consulta_receita(digits, tipo='cnpj')
                nome = _nome_cnpj_from_receita(info)
            except Exception:
                nome = ""
            if not nome:
                nome = f"CNPJ {digits}"

        pid = self.db.upsert_participante(digits, nome, tipo_contraparte)
        if pid:
            # 🔔 força recarregar combos/listas de participantes em TODAS as telas
            try:
                self._broadcast_participantes_changed()
                if hasattr(self, "_reload_participantes"):
                    self._reload_participantes()
                # Alguns painéis mantêm um cache próprio
                if hasattr(self, "carregar_lancamentos"):
                    self.carregar_lancamentos()
            except Exception:
                pass
            return int(pid)

        # fallback
        row = self.db.fetch_one("SELECT id FROM participante WHERE cpf_cnpj=?", (digits,))
        return int(row[0]) if row and row[0] else 0

    def _broadcast_participantes_changed(self):
        """Pede para todas as janelas/diálogos e o MainWindow recarregarem a lista de participantes."""
        try:
            from PySide6.QtWidgets import QApplication, QDialog
            # MainWindow
            if hasattr(self, "_reload_participantes"):
                try:
                    self._reload_participantes()
                except Exception:
                    pass
            # Diálogos abertos
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
        from datetime import datetime

        sb = self.db.sb
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

        # ---- contagem de linhas para progresso ----
        with open(path, encoding='utf-8') as _f:
            total = sum(1 for _ in _f)

        # ---- caches ----
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

                        # Layout 1 (11 colunas) -> YYYY-MM-DD ...
                        if len(parts) == 11 and re.match(r"\d{4}-\d{2}-\d{2}$", parts[0]):
                            (data_iso, cod_imovel, cod_conta, num_doc, raw_tipo_doc, historico,
                             participante_raw, tipo_lanc_raw, raw_ent, raw_sai, _) = parts

                            y, m, d = data_iso.split("-")
                            data_iso = f"{y}-{m}-{d}"         # ISO (já está), só mantenha
                            data_str = f"{d}/{m}/{y}"         # BR
                            data_ord = int(f"{y}{m}{d}")      # AAAAMMDD
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
                                    # garante auto-cadastro
                                    pid = self._ensure_participante(digits, historico or "")
                                    part_cache[digits] = pid
                                    id_participante = pid
                            elif (participante_raw or "").isdigit():
                                id_participante = int(participante_raw)

                            tipo_lanc = int(tipo_lanc_raw) if (tipo_lanc_raw or "").isdigit() else (1 if sai > 0 else 2)

                        elif len(parts) == 12 and re.match(r"\d{2}-\d{2}-\d{4}$", parts[0]):
                            (data_br, cod_imovel, cod_conta, num_doc, raw_tipo_doc, historico,
                             cpf_cnpj_raw, tipo_lanc_raw, cent_ent, cent_sai, _cent_saldo, _nat_raw) = parts
                        
                            # ✅ define primeiro d,m,y e só então monta as variações
                            d, m, y = data_br.split("-")                  # DD-MM-AAAA
                            data_iso = f"{y}-{m}-{d}"                    # AAAA-MM-DD (ISO)
                            data_str = f"{d}/{m}/{y}"                    # DD/MM/AAAA (legado/visual)
                            data_ord = int(f"{y}{m}{d}")                 # AAAAMMDD (inteiro para filtros)
                        
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

                        # FK imóvel (normalização 1/01/001) com cache ***FILTRADO POR PERFIL***
                        id_imovel = None
                        perfil_id = getattr(self.db, "perfil_id", None)

                        for c in _norms(cod_imovel):
                            if c in im_cache:
                                id_imovel = im_cache[c]
                                break

                            # consulta com escopo do perfil
                            if perfil_id:
                                row = self.db.fetch_one(
                                    "SELECT id FROM imovel_rural WHERE cod_imovel=? AND perfil_id=?",
                                    (c, perfil_id),
                                )
                            else:
                                row = self.db.fetch_one(
                                    "SELECT id FROM imovel_rural WHERE cod_imovel=?",
                                    (c,),
                                )

                            if row:
                                id_imovel = row[0]
                                for alt in _norms(cod_imovel):
                                    im_cache[alt] = id_imovel
                                break

                        if not id_imovel:
                            raise ValueError(f"Linha {lineno}: imóvel '{cod_imovel}' não encontrado no perfil atual")


                        # FK conta (normalização 1/01/001) com cache ***FILTRADO POR PERFIL***
                        id_conta = None
                        perfil_id = getattr(self.db, "perfil_id", None)

                        for c in _norms(cod_conta):
                            if c in ct_cache:
                                id_conta = ct_cache[c]
                                break
                            
                            # consulta com escopo do perfil
                            if perfil_id:
                                row = self.db.fetch_one(
                                    "SELECT id FROM conta_bancaria WHERE cod_conta=? AND perfil_id=?",
                                    (c, perfil_id),
                                )
                            else:
                                row = self.db.fetch_one(
                                    "SELECT id FROM conta_bancaria WHERE cod_conta=?",
                                    (c,),
                                )

                            if row:
                                id_conta = row[0]
                                for alt in _norms(cod_conta):
                                    ct_cache[alt] = id_conta
                                break
                            
                        if not id_conta:
                            raise ValueError(f"Linha {lineno}: conta '{cod_conta}' não encontrada no perfil atual")


                        # Saldo/natureza por conta (pega último saldo do BD uma única vez)
                        if id_conta not in saldos:
                            last = (sb.table("lancamento")
                                      .select("saldo_final,natureza_saldo,id")
                                      .eq("cod_conta", id_conta)
                                      .order("id", desc=True)
                                      .limit(1).execute().data)
                            if last:
                                base = float(last[0].get("saldo_final") or 0.0)
                                nat  = (last[0].get("natureza_saldo") or "P").upper()
                                saldos[id_conta] = base if nat == "P" else -base
                            else:
                                saldos[id_conta] = 0.0

                        saldo_ant = saldos[id_conta]
                        saldo_f = saldo_ant + (ent or 0.0) - (sai or 0.0)
                        saldos[id_conta] = saldo_f
                        nat = 'P' if saldo_f >= 0 else 'N'

                        # De-dup por participante + número do documento (sem espaços)
                        num_doc_n = (num_doc or "").replace(" ", "")
                        if id_participante:
                            dup = (sb.table("lancamento")
                                     .select("id")
                                     .eq("id_participante", id_participante)
                                     .eq("num_doc", num_doc_n)
                                     .limit(1).execute().data)
                            if dup:
                                if lineno % 200 == 0:
                                    GlobalProgress.set_value(lineno)
                                continue

                        # Insert
                        payload = {
                            "data": data_iso,
                            "data_ord": data_ord,
                            "cod_imovel": id_imovel,
                            "cod_conta": id_conta,
                            "num_doc": num_doc_n or None,
                            "tipo_doc": int(tipo_doc),
                            "historico": historico,
                            "id_participante": id_participante,
                            "tipo_lanc": int(tipo_lanc),
                            "valor_entrada": float(ent or 0.0),
                            "valor_saida": float(sai or 0.0),
                            "saldo_final": float(abs(saldo_f)),
                            "natureza_saldo": nat,
                            "usuario": usuario_ts,
                            "categoria": categoria,
                        }
                        sb.table("lancamento").insert(payload).execute()

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
                        [data_iso, id_imovel, id_conta, ((num_doc or '').strip() or None), tipo_doc, historico,
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

    def closeEvent(self, event):
        try:
            labels = [self.tab_lanc.horizontalHeaderItem(i).text() for i in range(self.tab_lanc.columnCount())]
            config = {label: not self.tab_lanc.isColumnHidden(i) for i, label in enumerate(labels)}
            kv_set(f"ui::lanc_columns::{CURRENT_PROFILE}", config)
        except Exception as e:
            print("prefs colunas (save) erro:", e)

        try:
            if hasattr(self, "_rt") and self._rt: self._rt.stop()
        except Exception: pass
        try:
            if hasattr(self, "db") and self.db: self.db.close()
        except Exception: pass
        super().closeEvent(event)


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