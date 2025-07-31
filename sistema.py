import os
import sys
import re
import json
import csv
import sqlite3
from datetime import datetime
import pandas as pd
import requests

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLineEdit, QDateEdit, QComboBox, QLabel, QTextEdit,
    QTableWidget, QTableWidgetItem, QHeaderView, QTabWidget, QDialog,
    QDialogButtonBox, QMessageBox, QFormLayout, QGroupBox, QFrame,
    QStatusBar, QToolBar, QFileDialog, QCheckBox, QMenu, QToolButton,
    QWidgetAction
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
CACHE_FILE   = os.path.join(CACHE_FOLDER, 'receita_cache.json')
API_URL_CNPJ = 'https://www.receitaws.com.br/v1/cnpj/'
API_URL_CPF  = 'https://www.receitaws.com.br/v1/cpf/'

# —————— Configuração para salvar último caminho do TXT LCDPR ——————
TXT_PREF_FILE = os.path.join(CACHE_FOLDER, 'lcdpr_txt_path.json')

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

# --- CONSTANTES E ESTILO GLOBAL ---
DB_FILENAME = 'lcdpr.db'
APP_ICON    = 'agro_icon.png'

# --- CLASSE DE ACESSO AOS DADOS ---
class Database:
    def __init__(self, filename: str = DB_FILENAME):
        # abre/cria o arquivo e gera self.conn
        self.conn = sqlite3.connect(filename)
        # garante que todas as tabelas e views existam
        self._create_tables()
        self._create_views()

    def _create_tables(self):
        cursor = self.conn.cursor()
        cursor.executescript("""
        -- Imóveis rurais
        CREATE TABLE IF NOT EXISTS imovel_rural (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cod_imovel TEXT UNIQUE NOT NULL,
            pais TEXT NOT NULL DEFAULT 'BR',
            moeda TEXT NOT NULL DEFAULT 'BRL',
            cad_itr TEXT,
            caepf TEXT,
            insc_estadual TEXT,
            nome_imovel TEXT NOT NULL,
            endereco TEXT NOT NULL,
            num TEXT,
            compl TEXT,
            bairro TEXT NOT NULL,
            uf TEXT NOT NULL,
            cod_mun TEXT NOT NULL,
            cep TEXT NOT NULL,
            tipo_exploracao INTEGER NOT NULL,
            participacao REAL NOT NULL DEFAULT 100.0,
            area_total REAL,
            area_utilizada REAL,
            data_cadastro DATE DEFAULT CURRENT_DATE
        );

        -- Contas bancárias
        CREATE TABLE IF NOT EXISTS conta_bancaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cod_conta TEXT UNIQUE NOT NULL,
            pais_cta TEXT NOT NULL DEFAULT 'BR',
            banco TEXT,
            nome_banco TEXT NOT NULL,
            agencia TEXT NOT NULL,
            num_conta TEXT NOT NULL,
            saldo_inicial REAL DEFAULT 0,
            data_abertura DATE DEFAULT CURRENT_DATE
        );

        -- Participantes
        CREATE TABLE IF NOT EXISTS participante (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cpf_cnpj TEXT UNIQUE NOT NULL,
            nome TEXT NOT NULL,
            tipo_contraparte INTEGER NOT NULL,
            data_cadastro DATE DEFAULT CURRENT_DATE
        );

        -- Culturas
        CREATE TABLE IF NOT EXISTS cultura (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            tipo TEXT NOT NULL,
            ciclo TEXT,
            unidade_medida TEXT
        );

        -- Áreas de produção
        CREATE TABLE IF NOT EXISTS area_producao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            imovel_id INTEGER NOT NULL,
            cultura_id INTEGER NOT NULL,
            area REAL NOT NULL,
            data_plantio DATE,
            data_colheita_estimada DATE,
            produtividade_estimada REAL,
            FOREIGN KEY(imovel_id) REFERENCES imovel_rural(id),
            FOREIGN KEY(cultura_id) REFERENCES cultura(id)
        );

        -- Estoques
        CREATE TABLE IF NOT EXISTS estoque (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            produto TEXT NOT NULL,
            quantidade REAL NOT NULL,
            unidade_medida TEXT NOT NULL,
            valor_unitario REAL,
            local_armazenamento TEXT,
            data_entrada DATE DEFAULT CURRENT_DATE,
            data_validade DATE,
            imovel_id INTEGER,
            FOREIGN KEY(imovel_id) REFERENCES imovel_rural(id)
        );

        -- Lançamentos contábeis
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
            categoria TEXT,
            area_afetada INTEGER,
            quantidade REAL,
            unidade_medida TEXT,
            FOREIGN KEY(cod_imovel) REFERENCES imovel_rural(id),
            FOREIGN KEY(cod_conta) REFERENCES conta_bancaria(id),
            FOREIGN KEY(id_participante) REFERENCES participante(id),
            FOREIGN KEY(area_afetada) REFERENCES area_producao(id)
        );
        """)
        self.conn.commit()

    def _create_views(self):
        cursor = self.conn.cursor()
        cursor.executescript("""
        -- Saldo atual das contas
        CREATE VIEW IF NOT EXISTS saldo_contas AS
        SELECT 
            cb.id,
            cb.cod_conta,
            cb.nome_banco,
            l.saldo_final * 
              (CASE l.natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) 
            AS saldo_atual
        FROM conta_bancaria cb
        LEFT JOIN (
            SELECT cod_conta, MAX(id) AS max_id
            FROM lancamento
            GROUP BY cod_conta
        ) last_l ON cb.id = last_l.cod_conta
        LEFT JOIN lancamento l ON last_l.max_id = l.id;

        -- Resumo por categoria
        CREATE VIEW IF NOT EXISTS resumo_categorias AS
        SELECT 
            categoria,
            SUM(valor_entrada) AS total_entradas,
            SUM(valor_saida)   AS total_saidas,
            strftime('%Y', data) AS ano,
            strftime('%m', data) AS mes
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
QMainWindow {
    background-color: #1B1D1E;    /* quase preto frio */
}
QWidget {
    font-family: 'Segoe UI', Arial, sans-serif;
    color: #E0E0E0;               /* cinza claro */
    background-color: #1B1D1E;
}
QLineEdit, QDateEdit, QComboBox, QTextEdit {
    color: #E0E0E0;
    background-color: #2B2F31;    /* cinza escuro */
    border: 1px solid #1e5a9c;    /* verde musgo */
    border-radius: 6px;
    padding: 6px;
}
QLineEdit::placeholder {
    color: #5A5A5A;
}
QPushButton {
    background-color: #1e5a9c;    /* verde musgo */
    color: #FFFFFF;
    border: none;
    border-radius: 6px;
    padding: 8px 16px;
    font-weight: bold;
}
QPushButton:hover {
    background-color: #002a54;
}
QPushButton:pressed {
    background-color: #002a54;
}
QPushButton#danger {
    background-color: #C0392B;    /* vermelho forte */
}
QPushButton#danger:hover {
    background-color: #E74C3C;    /* vermelho vivo */
}

QPushButton#success {
    background-color: #27AE60;    /* verde vívido */
}
QPushButton#success:hover {
    background-color: #2ECC71;    /* verde claro e brilhante */
}

QGroupBox {
    border: 1px solid #11398a;    /* verde-água intenso */
    border-radius: 6px;
    margin-top: 10px;
    font-weight: bold;
    background-color: #0d1b3d;    /* grafite escuro */
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 5px;
    color: #ffffff;               /* verde-água claro */
}

QTableWidget {
    background-color: #222426;
    color: #E0E0E0;
    border: 1px solid #1e5a9c;
    border-radius: 4px;
    gridline-color: #3A3C3D;
    alternate-background-color: #2A2C2D;
}
QHeaderView::section {
    background-color: #1e5a9c;
    color: #FFFFFF;
    padding: 6px;
    border: none;
}
QTabWidget::pane {
    border: 1px solid #1e5a9c;
    border-radius: 4px;
    background: #212425;
    margin-top: 5px;
}
QTabBar::tab {
    background: #2A2C2D;
    color: #E0E0E0;
    padding: 8px 16px;
    border: 1px solid #1e5a9c;
    border-top-left-radius: 4px;
    border-top-right-radius: 4px;
    margin-right: 2px;
}
QTabBar::tab:selected {
    background: #1e5a9c;
    color: #FFFFFF;
    border-bottom: 2px solid #002a54;
}
QStatusBar {
    background-color: #212425;
    color: #7F7F7F;
    border-top: 1px solid #1e5a9c;
}
"""

class NumericItem(QTableWidgetItem):
    def __init__(self, value, text=None):
        # text é o que aparece na tabela; value é um número puro, usado para ordenar
        super().__init__(text or str(value))
        self._value = value

    def __lt__(self, other):
        if isinstance(other, NumericItem):
            return self._value < other._value
        return super().__lt__(other)
    
class CurrencyLineEdit(QLineEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAlignment(Qt.AlignRight)
        self.setPlaceholderText("R$ 0,00")
        self.textChanged.connect(self._format_currency)

    def _format_currency(self, text):
        digits = re.sub(r'[^\d]', '', text)
        if not digits:
            self.blockSignals(True)
            self.setText('')
            self.blockSignals(False)
            return
        value = int(digits)
        inteiro = value // 100
        cents = value % 100
        inteiro_str = f"{inteiro:,}".replace(",", ".")
        formatted = f"R$ {inteiro_str},{cents:02d}"
        self.blockSignals(True)
        self.setText(formatted)
        self.blockSignals(False)

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

# --- DIALOG CÓDIGO DE IMÓVEL ---
class CadastroImovelDialog(CadastroBaseDialog):
    def __init__(self, parent=None, imovel_id=None):
        super().__init__(parent)
        self.imovel_id = imovel_id
        self.configure_window()
        self._build_ui()
        self._load_data()

    def configure_window(self):
        self.setWindowTitle("Cadastro de Imóvel Rural")
        self.setMinimumSize(900, 780)

    def _build_ui(self):
        header = QLabel("Cadastro de Imóvel Rural")
        header.setFont(QFont('', 14, QFont.Bold))
        header.setStyleSheet("color: #ffffff; margin-bottom: 8px;")
        self.layout.insertWidget(0, header)

        # Identificação
        grp1 = QGroupBox("Identificação do Imóvel")
        f1 = QFormLayout(grp1)
        self.cod_imovel = QLineEdit(); f1.addRow("Código:", self.cod_imovel)
        self.pais = QComboBox(); self.pais.addItems(["BR","AR","US","…"]); f1.addRow("País:", self.pais)
        self.moeda = QComboBox(); self.moeda.addItems(["BRL","USD","EUR","…"]); f1.addRow("Moeda:", self.moeda)
        self.nome_imovel = QLineEdit(); f1.addRow("Nome:", self.nome_imovel)
        self.cad_itr = QLineEdit(); f1.addRow("CAD ITR:", self.cad_itr)
        self.caepf = QLineEdit(); f1.addRow("CAEPF:", self.caepf)
        self.insc_estadual = QLineEdit(); f1.addRow("Inscrição Est.:", self.insc_estadual)
        self.form_layout.addRow(grp1)

        # Localização
        grp2 = QGroupBox("Localização")
        f2 = QFormLayout(grp2)
        self.endereco = QLineEdit(); f2.addRow("Endereço:", self.endereco)
        self.num = QLineEdit(); f2.addRow("Número:", self.num)
        self.compl = QLineEdit(); f2.addRow("Complemento:", self.compl)
        self.bairro = QLineEdit(); f2.addRow("Bairro:", self.bairro)
        self.uf = QLineEdit(); f2.addRow("UF:", self.uf)
        self.cod_mun = QLineEdit(); f2.addRow("Cód. Município:", self.cod_mun)
        self.cep = QLineEdit(); f2.addRow("CEP:", self.cep)
        self.form_layout.addRow(grp2)

        # Exploração
        grp3 = QGroupBox("Exploração Agrícola")
        f3 = QFormLayout(grp3)
        self.tipo_exploracao = QComboBox()
        self.tipo_exploracao.addItems([
            "1 - Exploração individual","2 - Condomínio","3 - Imóvel arrendado",
            "4 - Parceria","5 - Comodato","6 - Outros"
        ])
        f3.addRow("Tipo:", self.tipo_exploracao)
        self.participacao = QLineEdit("100.00"); f3.addRow("Participação (%):", self.participacao)
        self.form_layout.addRow(grp3)

        for w in [self.cod_imovel, self.pais, self.moeda, self.nome_imovel,
                  self.cad_itr, self.caepf, self.insc_estadual, self.endereco,
                  self.num, self.compl, self.bairro, self.uf, self.cod_mun,
                  self.cep, self.tipo_exploracao, self.participacao]:
            w.setFixedHeight(25)

        grp4 = QGroupBox("Áreas do Imóvel (ha)")
        f4 = QFormLayout(grp4)
        self.area_total = QLineEdit()
        f4.addRow("Área Total:", self.area_total)
        self.area_utilizada = QLineEdit()
        f4.addRow("Área Utilizada:", self.area_utilizada)
        self.form_layout.addRow(grp4)

        for w in [self.area_total, self.area_utilizada]:
            w.setFixedHeight(25)

    def _load_data(self):
        if not self.imovel_id:
            return
        row = self.db.fetch_one("""
            SELECT cod_imovel, pais, moeda, cad_itr, caepf, insc_estadual,
                   nome_imovel, endereco, num, compl, bairro, uf, cod_mun, cep,
                   tipo_exploracao, participacao,
                   area_total, area_utilizada
            FROM imovel_rural WHERE id=?
        """, (self.imovel_id,))

        if not row:
            return

        (cod, pais, moeda, cad, caepf, ie,
         nome, end, num, comp, bar, uf, mun, cep,
         tipo, part, at, au) = row

        self.cod_imovel.setText(cod)
        self.pais.setCurrentText(pais)
        self.moeda.setCurrentText(moeda)
        self.cad_itr.setText(cad or "")
        self.caepf.setText(caepf or "")
        self.insc_estadual.setText(ie or "")
        self.nome_imovel.setText(nome)
        self.endereco.setText(end)
        self.num.setText(num or "")
        self.compl.setText(comp or "")
        self.bairro.setText(bar)
        self.uf.setText(uf)
        self.cod_mun.setText(mun)
        self.cep.setText(cep)
        self.tipo_exploracao.setCurrentIndex(tipo-1)
        self.participacao.setText(f"{part:.2f}")
        self.area_total.setText(f"{at or 0:.2f}")
        self.area_utilizada.setText(f"{au or 0:.2f}")

    def salvar(self):
        campos = [
            self.cod_imovel.text().strip(),
            self.pais.currentText(),
            self.moeda.currentText(),
            self.nome_imovel.text().strip(),
            self.endereco.text().strip(),
            self.bairro.text().strip(),
            self.uf.text().strip(),
            self.cod_mun.text().strip(),
            self.cep.text().strip()
        ]
        if not all(campos):
            QMessageBox.warning(self, "Obrigatório", "Preencha todos os campos obrigatórios!")
            return

        data = (
            self.cod_imovel.text().strip(),
            self.pais.currentText(),
            self.moeda.currentText(),
            self.cad_itr.text().strip() or None,
            self.caepf.text().strip() or None,
            self.insc_estadual.text().strip() or None,
            self.nome_imovel.text().strip(),
            self.endereco.text().strip(),
            self.num.text().strip() or None,
            self.compl.text().strip() or None,
            self.bairro.text().strip(),
            self.uf.text().strip(),
            self.cod_mun.text().strip(),
            self.cep.text().strip(),
            self.tipo_exploracao.currentIndex()+1,
            float(self.participacao.text()),
            float(self.area_total.text() or 0),
            float(self.area_utilizada.text() or 0),
        )

        try:
            if self.imovel_id:
                sql = """
                    UPDATE imovel_rural SET
                      cod_imovel=?,pais=?,moeda=?,cad_itr=?,caepf=?,insc_estadual=?,
                      nome_imovel=?,endereco=?,num=?,compl=?,bairro=?,uf=?,cod_mun=?,cep=?,
                      tipo_exploracao=?,participacao=?, area_total=?, area_utilizada=?
                    WHERE id=?
                """
                self.db.execute_query(sql, data + (self.imovel_id,))
                msg = "Atualizado com sucesso!"
            else:
                sql = """
                    INSERT INTO imovel_rural (
                      cod_imovel,pais,moeda,cad_itr,caepf,insc_estadual,
                      nome_imovel,endereco,num,compl,bairro,uf,cod_mun,cep,
                      tipo_exploracao,participacao, area_total, area_utilizada
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """
                self.db.execute_query(sql, data)
                msg = "Cadastrado com sucesso!"
            QMessageBox.information(self, "Sucesso", msg)
            self.accept()
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
        if not self.conta_id:
            return

        row = self.db.fetch_one(
            "SELECT cod_conta, pais_cta, banco, nome_banco, agencia, num_conta, saldo_inicial "
            "FROM conta_bancaria WHERE id = ?",
            (self.conta_id,)
        )
        if not row:
            return

        cod, pais, banco, nome, agencia, num_conta, saldo = row
        self.cod_conta.setText(cod)
        self.pais_cta.setCurrentText(pais)
        self.banco.setText(banco or "")
        self.nome_banco.setText(nome or "")
        self.agencia.setText(agencia or "")
        self.num_conta.setText(num_conta or "")
        # formata o CurrencyLineEdit
        self.saldo_inicial.setText(f"{saldo:.2f}")

    def salvar(self):
        # 1) coleta e valida campos obrigatórios
        cod_conta    = self.cod_conta.text().strip()
        nome_banco   = self.nome_banco.text().strip()
        banco        = self.banco.text().strip()
        agencia      = self.agencia.text().strip()
        num_conta    = self.num_conta.text().strip()
        saldo_raw    = self.saldo_inicial.text().strip()

        if not (cod_conta and nome_banco and agencia and num_conta):
            QMessageBox.warning(
                self, "Campos Obrigatórios",
                "Preencha Código da Conta, Nome do Banco, Agência e Número da Conta."
            )
            return

        # 2) função auxiliar para extrair valor numérico do CurrencyLineEdit
        def parse_currency(text: str) -> float:
            digits = re.sub(r"[^\d]", "", text)
            if not digits:
                return 0.0
            inteiro  = int(digits) // 100
            centavos = int(digits) % 100
            return inteiro + centavos / 100.0

        saldo_inicial = parse_currency(saldo_raw)

        # 3) prepara dados para salvar
        data = (
            cod_conta,
            "BR",           # país (fixo) — ajuste se quiser expor como campo
            banco,
            nome_banco,
            agencia,
            num_conta,
            saldo_inicial
        )

        # 4) insere ou atualiza no banco
        try:
            if self.conta_id:
                sql = """
                    UPDATE conta_bancaria
                    SET cod_conta = ?, pais_cta = ?, banco = ?, nome_banco = ?,
                        agencia = ?, num_conta = ?, saldo_inicial = ?
                    WHERE id = ?
                """
                self.db.execute_query(sql, data + (self.conta_id,))
                msg = "Conta bancária atualizada com sucesso!"
            else:
                sql = """
                    INSERT INTO conta_bancaria
                    (cod_conta, pais_cta, banco, nome_banco, agencia, num_conta, saldo_inicial)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                self.db.execute_query(sql, data)
                msg = "Conta bancária cadastrada com sucesso!"

            QMessageBox.information(self, "Sucesso", msg)
            self.accept()

        except Exception as e:
            QMessageBox.critical(
                self, "Erro ao Salvar",
                f"Não foi possível salvar a conta bancária:\n{e}"
            )



# --- DIALOG CADASTRO PARTICIPANTE COM MÁSCARA DINÂMICA ---
class CadastroParticipanteDialog(QDialog):
    def __init__(self, parent=None, participante_id=None):
        super().__init__(parent)
        self.participante_id = participante_id
        self.setWindowTitle("Cadastro de Participante")
        self.setMinimumSize(400, 250)

        self.db = Database()
        layout = QVBoxLayout(self)

        # Cabeçalho
        hdr = QLabel("Cadastro de Participante")
        hdr.setFont(QFont('', 16, QFont.Bold))
        hdr.setStyleSheet("margin-bottom:15px;")
        layout.addWidget(hdr)

        # Formulário
        form_layout = QFormLayout()
        grp = QGroupBox("Dados do Participante")
        grp.setLayout(form_layout)
        layout.addWidget(grp)

        # Dentro de __init__, substitua esta parte:
        self.tipo = QComboBox()
        self.tipo.addItems(["Pessoa Jurídica", "Pessoa Física", "Órgão Público", "Outros"])
        self.tipo.currentIndexChanged.connect(self._ajustar_mask)
        form_layout.addRow("Tipo:", self.tipo)
        
        self.cpf_cnpj = QLineEdit()
        self.cpf_cnpj.setPlaceholderText("Digite CPF ou CNPJ")
        self.cpf_cnpj.editingFinished.connect(self._on_cpf_cnpj)
        form_layout.addRow("CPF/CNPJ:", self.cpf_cnpj)
        # chamada inicial
        self._ajustar_mask( self.tipo.currentIndex() )

        # Nome
        self.nome = QLineEdit()
        form_layout.addRow("Nome:", self.nome)

        # Botões
        btns = QHBoxLayout()
        btns.addStretch()
        salvar = QPushButton("Salvar")
        salvar.setObjectName("success")
        salvar.clicked.connect(self.salvar)
        btns.addWidget(salvar)
        cancelar = QPushButton("Cancelar")
        cancelar.setObjectName("danger")
        cancelar.clicked.connect(self.reject)
        btns.addWidget(cancelar)
        layout.addLayout(btns)

        # Se for edição, carrega dados existentes
        if participante_id:
            row = self.db.fetch_one(
                "SELECT cpf_cnpj, nome, tipo_contraparte FROM participante WHERE id=?",
                (participante_id,)
            )
            if row:
                self.tipo.setCurrentIndex(row[2] - 1)
                self.cpf_cnpj.setText(row[0])
                self.nome.setText(row[1])

    def _ajustar_mask(self, idx):
        cur = self.cpf_cnpj.cursorPosition()
        if idx == 0:   # Pessoa Jurídica agora é índice 0
            self.cpf_cnpj.setInputMask("00.000.000/0000-00;_")
        elif idx == 1: # Pessoa Física agora é índice 1
            self.cpf_cnpj.setInputMask("000.000.000-00;_")
        else:
            self.cpf_cnpj.setInputMask("")
        self.cpf_cnpj.setCursorPosition(cur)

    def _on_cpf_cnpj(self):
        raw = self.cpf_cnpj.text().strip()
        digits = re.sub(r'\D', '', raw)
        idx = self.tipo.currentIndex()
        if idx == 0:  # PF
            if not valida_cpf(raw):
                QMessageBox.warning(self, "CPF inválido", "O CPF digitado não é válido.")
                self.nome.clear()
                return
        elif idx == 1 and len(digits) != 14:
            return

        try:
            kind = 'cpf' if idx == 0 else 'cnpj'
            info = consulta_receita(digits, tipo=kind)
        except requests.HTTPError as e:
            # 404 ou outro erro
            return
        except Exception:
            return

        # só preenche se status OK
        if info.get('status') == 'OK':
            nome_api = info.get('nome') or info.get('fantasia')
            if nome_api:
                self.nome.setText(nome_api)

    def salvar(self):
        raw = self.cpf_cnpj.text().strip()
        digits = re.sub(r'\D', '', raw)
        idx = self.tipo.currentIndex()

        # 1) Validação CPF formal
        if idx == 0:  # Pessoa Física
            if not valida_cpf(raw):
                QMessageBox.warning(self, "Inválido", "CPF inválido.")
                return

        # 2) Validação CNPJ por consulta na Receita
        elif idx == 1:  # Pessoa Jurídica
            if len(digits) != 14:
                QMessageBox.warning(self, "Inválido", "CNPJ deve ter 14 dígitos.")
                return
            try:
                info = consulta_receita(digits, tipo='cnpj')
            except requests.HTTPError:
                QMessageBox.warning(self, "Inválido", "Não foi possível consultar o CNPJ na Receita Federal.")
                return
            if info.get('status') != 'OK':
                QMessageBox.warning(self, "Não Encontrado", "CNPJ não localizado na Receita Federal.")
                return

        # 3) Outros tipos (Órgão Público, Outros) pulam validação
        #    mas você pode adicionar regras se precisar.

        # 4) Nome não pode ficar vazio
        nome = self.nome.text().strip()
        if not nome:
            QMessageBox.warning(self, "Inválido", "Nome não pode ficar vazio.")
            return

        # 5) Evita duplicação no banco
        exists = self.db.fetch_one(
            "SELECT id FROM participante WHERE cpf_cnpj = ?",
            (digits,)
        )
        if exists and not self.participante_id:
            QMessageBox.information(
                self, "Já existe",
                f"Participante já cadastrado (ID {exists[0]})."
            )
            return

        # 6) Persistência no SQLite
        data = (digits, nome, idx + 1)
        try:
            if self.participante_id:
                self.db.execute_query(
                    "UPDATE participante SET cpf_cnpj = ?, nome = ?, tipo_contraparte = ? WHERE id = ?",
                    data + (self.participante_id,)
                )
            else:
                self.db.execute_query(
                    "INSERT INTO participante (cpf_cnpj, nome, tipo_contraparte) VALUES (?, ?, ?)",
                    data
                )
            QMessageBox.information(self, "Sucesso", "Participante salvo com sucesso!")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Erro ao Salvar",
                                 f"Não foi possível salvar participante:\n{e}")

class ParametrosDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Parâmetros do Contribuinte")
        self.setMinimumSize(400, 500)
        self.settings = QSettings("PrimeOnHub", "AgroApp")
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
                nat
            ]

            if self.lanc_id:
                sql = """
                    UPDATE lancamento SET
                        data = ?, cod_imovel = ?, cod_conta = ?, num_doc = ?, tipo_doc = ?,
                        historico = ?, id_participante = ?, tipo_lanc = ?,
                        valor_entrada = ?, valor_saida = ?, saldo_final = ?, natureza_saldo = ?
                    WHERE id = ?
                """
                self.db.execute_query(sql, params + [self.lanc_id])
            else:
                sql = """
                    INSERT INTO lancamento (
                        data, cod_imovel, cod_conta, num_doc, tipo_doc,
                        historico, id_participante, tipo_lanc,
                        valor_entrada, valor_saida, saldo_final, natureza_saldo
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
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
        path = os.path.join(CACHE_FOLDER, "lanc_filter.json")
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
        path = os.path.join(CACHE_FOLDER, "lanc_filter.json")
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
        path = os.path.join(CACHE_FOLDER, "lanc_filter.json")
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
        path = os.path.join(CACHE_FOLDER, "lanc_filter.json")
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

            
# --- WIDGET GERENCIAMENTO PARTICIPANTES ---
class GerenciamentoParticipantesWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db = Database()
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(10,10,10,10)

        # cabeçalhos e estado de ordenação
        self._participantes_labels = ["CPF/CNPJ","Nome","Tipo","Cadastro"]
        self._participantes_sort_state = {}

        # monta UI (inclui self.tabela e self._part_filter_menu)
        self._build_ui()
        # aplica filtro salvo
        self._load_participantes_column_filter()
        # carrega dados
        self.carregar_participantes()

    def _build_ui(self):
        tl = QHBoxLayout()
        tl.setContentsMargins(0,0,10,10)

        # CRUD + pesquisa
        self.btn_novo = QPushButton("Novo Participante")
        self.btn_novo.setIcon(QIcon.fromTheme("document-new"))
        self.btn_novo.clicked.connect(self.novo_participante)
        tl.addWidget(self.btn_novo)

        self.btn_editar = QPushButton("Editar")
        self.btn_editar.setEnabled(False)
        self.btn_editar.setIcon(QIcon.fromTheme("document-edit"))
        self.btn_editar.clicked.connect(self.editar_participante)
        tl.addWidget(self.btn_editar)

        self.btn_excluir = QPushButton("Excluir")
        self.btn_excluir.setEnabled(False)
        self.btn_excluir.setIcon(QIcon.fromTheme("edit-delete"))
        self.btn_excluir.clicked.connect(self.excluir_participante)
        tl.addWidget(self.btn_excluir)

        self.btn_importar = QPushButton("Importar")
        self.btn_importar.setIcon(QIcon.fromTheme("document-import"))
        self.btn_importar.clicked.connect(self.importar_participantes)
        tl.addWidget(self.btn_importar)

        self.search_part = QLineEdit()
        self.search_part.setPlaceholderText("Pesquisar participantes…")
        self.search_part.textChanged.connect(self._filter_participantes)
        tl.addWidget(self.search_part)

        # botão de filtro de colunas
        btn_filter = QToolButton()
        btn_filter.setText("⚙️")
        btn_filter.setAutoRaise(True)
        btn_filter.setPopupMode(QToolButton.InstantPopup)
        self._part_filter_menu = QMenu(self)
        for col, lbl in enumerate(self._participantes_labels):
            wa = QWidgetAction(self._part_filter_menu)
            chk = QCheckBox(lbl)
            chk.setChecked(True)
            chk.toggled.connect(lambda vis, c=col: self._toggle_participantes_column(c, vis))
            wa.setDefaultWidget(chk)
            self._part_filter_menu.addAction(wa)
        btn_filter.setMenu(self._part_filter_menu)
        tl.addWidget(btn_filter)

        tl.addStretch()
        self.layout.addLayout(tl)

        # tabela
        self.tabela = QTableWidget(0, len(self._participantes_labels))
        self.tabela.setHorizontalHeaderLabels(self._participantes_labels)
        self.tabela.setAlternatingRowColors(True)
        self.tabela.setShowGrid(False)
        self.tabela.verticalHeader().setVisible(False)
        self.tabela.setSelectionBehavior(QTableWidget.SelectRows)
        self.tabela.setEditTriggers(QTableWidget.NoEditTriggers)
        self.tabela.cellClicked.connect(self._select_row)

        hdr = self.tabela.horizontalHeader()
        hdr.setHighlightSections(False)
        hdr.setDefaultAlignment(Qt.AlignCenter)
        hdr.setSectionResizeMode(QHeaderView.Stretch)
        # duplo‑clique para ordenação cíclica
        hdr.sectionDoubleClicked.connect(self._toggle_sort_participantes)

        self.layout.addWidget(self.tabela)

    def _toggle_sort_participantes(self, index: int):
        """Cicla entre sem ordenação, asc e desc."""
        state = self._participantes_sort_state.get(index, 0)
        if state == 0:
            self.tabela.sortItems(index, Qt.AscendingOrder)
            new = 1
        elif state == 1:
            self.tabela.sortItems(index, Qt.DescendingOrder)
            new = 2
        else:
            self.carregar_participantes()
            new = 0
        self._participantes_sort_state = {index: new}

    def _toggle_participantes_column(self, col: int, visible: bool):
        """Esconde/exibe coluna e salva só 'participantes' no lanc_filter.json."""
        self.tabela.setColumnHidden(col, not visible)
        self._save_participantes_column_filter()

    def _save_participantes_column_filter(self):
        path = os.path.join(CACHE_FOLDER, "lanc_filter.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except:
            cfg = {}
        vis = [not self.tabela.isColumnHidden(c)
               for c in range(self.tabela.columnCount())]
        cfg["participantes"] = vis
        os.makedirs(CACHE_FOLDER, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)

    def _load_participantes_column_filter(self):
        path = os.path.join(CACHE_FOLDER, "lanc_filter.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            vis = cfg.get("participantes", [])
        except:
            return
        for c, shown in enumerate(vis):
            self.tabela.setColumnHidden(c, not shown)
        # sincroniza checkboxes
        for wa in self._part_filter_menu.actions():
            chk = wa.defaultWidget()
            if isinstance(chk, QCheckBox):
                idx = self._participantes_labels.index(chk.text())
                chk.setChecked(not self.tabela.isColumnHidden(idx))

    def _filter_participantes(self, text: str):
        text = text.lower()
        for row in range(self.tabela.rowCount()):
            hide = True
            for col in range(self.tabela.columnCount()):
                item = self.tabela.item(row, col)
                if item and text in item.text().lower():
                    hide = False
                    break
            self.tabela.setRowHidden(row, hide)

    def importar_participantes(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Importar Participantes", "", "TXT (*.txt);;Excel (*.xlsx *.xls)"
        )
        if not path:
            return
        try:
            if path.lower().endswith('.txt'):
                self._import_participantes_txt(path)
            else:
                self._import_participantes_excel(path)
            self.carregar_participantes()
        except Exception:
            QMessageBox.warning(
                self, "Importação Falhou",
                "Arquivo não segue o layout esperado e não foi importado."
            )


    def _import_participantes_txt(self, path):
        with open(path, encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) != 3:
                    raise ValueError("Layout de TXT inválido")
                cpf_cnpj, nome, tipo = parts
                self.db.execute_query(
                    """
                    INSERT OR REPLACE INTO participante (
                        cpf_cnpj, nome, tipo_contraparte
                    ) VALUES (?, ?, ?)
                    """,
                    (cpf_cnpj.strip(), nome.strip(), int(tipo))
                )

    def _import_participantes_excel(self, path):
        df = pd.read_excel(path, dtype=str)
        required = ['cpf_cnpj','nome','tipo_contraparte']
        if not all(col in df.columns for col in required):
            raise ValueError("Layout de Excel inválido")
        df.fillna('', inplace=True)
        for row in df.itertuples(index=False):
            self.db.execute_query(
                """
                INSERT OR REPLACE INTO participante (
                    cpf_cnpj, nome, tipo_contraparte
                ) VALUES (?, ?, ?)
                """,
                (row.cpf_cnpj.strip(), row.nome.strip(), int(row.tipo_contraparte))
            )

    def carregar_participantes(self):
        rows = self.db.fetch_all(
            "SELECT id,cpf_cnpj,nome,tipo_contraparte,data_cadastro "
            "FROM participante ORDER BY data_cadastro DESC"
        )
        self.tabela.setRowCount(len(rows))
        tipos = {1:"PJ",2:"PF",3:"Órgão Público",4:"Outros"}

        for r, (id_, cpf, nome, tipo, data_str) in enumerate(rows):
            formatted_date = QDate.fromString(data_str, "yyyy-MM-dd").toString("dd/MM/yyyy")
            vals = [cpf, nome, tipos.get(tipo, str(tipo)), formatted_date]
            for c, v in enumerate(vals):
                item = QTableWidgetItem(v)
                item.setTextAlignment(Qt.AlignCenter)
                self.tabela.setItem(r, c, item)
            self.tabela.item(r, 0).setData(Qt.UserRole, id_)

        self.btn_editar.setEnabled(False)
        self.btn_excluir.setEnabled(False)

    def _select_row(self, row, _):
        self.selected_row = row
        self.btn_editar.setEnabled(True)
        self.btn_excluir.setEnabled(True)

    def novo_participante(self):
        dlg = CadastroParticipanteDialog(self)
        if dlg.exec():
            self.carregar_participantes()

    def editar_participante(self):
        id_ = self.tabela.item(self.selected_row,0).data(Qt.UserRole)
        dlg = CadastroParticipanteDialog(self, id_)
        if dlg.exec():
            self.carregar_participantes()

    def excluir_participante(self):
        indices = self.tabela.selectionModel().selectedRows()
        if not indices:
            return
        nomes = [self.tabela.item(idx.row(), 1).text() for idx in indices]
        resp = QMessageBox.question(
            self, "Confirmar Exclusão",
            f"Excluir participantes: {', '.join(nomes)}?",
            QMessageBox.Yes | QMessageBox.No
        )
        if resp != QMessageBox.Yes:
            return
        for idx in indices:
            pid = self.tabela.item(idx.row(), 0).data(Qt.UserRole)
            try:
                self.db.execute_query("DELETE FROM participante WHERE id=?", (pid,))
            except Exception as e:
                QMessageBox.critical(self, "Erro", f"Erro ao excluir participante ID {pid}: {e}")
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


# --- JANELA PRINCIPAL ---
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db = Database()
        self.setWindowTitle("Sistema AgroContábil - LCDPR")
        self.setGeometry(100,100,1200,800)
        self.setStyleSheet(STYLE_SHEET)

        # define this before _setup_ui()
        self._lanc_labels = [
            "ID", "Data", "Imóvel",
            "Documento", "Participante",
            "Histórico", "Tipo",
            "Entrada", "Saída", "Saldo"
        ]
        # 2) Só aí monte toda a UI
        self._setup_ui()
        self._lanc_sort_state = {}

    def _setup_ui(self):
        # Ícone e menus
        self.setWindowIcon(QIcon(APP_ICON))
        self._create_menu()
        self._create_toolbar()

        # Cria o container de abas
        self.tabs = QTabWidget()
        self.tabs.setContentsMargins(10, 10, 10, 10)
        self.setCentralWidget(self.tabs)

        # --- Aba Painel ---
        self.dashboard = DashboardWidget()
        self.tabs.addTab(self.dashboard, "Painel")

        # --- Aba Lançamentos ---
        w_l = QWidget()
        l_l = QVBoxLayout(w_l)
        l_l.setContentsMargins(10, 10, 10, 10)
        
        # filtros, botões e pesquisa
        self.lanc_filter_layout = QHBoxLayout()
        
        # intervalo “De” / “Até”
        self.lanc_filter_layout.addWidget(QLabel("De:"))
        self.dt_ini = QDateEdit(QDate.currentDate().addMonths(-1))
        self.dt_ini.setCalendarPopup(True)
        self.dt_ini.setDisplayFormat("dd/MM/yyyy")
        self.lanc_filter_layout.addWidget(self.dt_ini)
        
        self.lanc_filter_layout.addWidget(QLabel("Até:"))
        self.dt_fim = QDateEdit(QDate.currentDate())
        self.dt_fim.setCalendarPopup(True)
        self.dt_fim.setDisplayFormat("dd/MM/yyyy")
        self.lanc_filter_layout.addWidget(self.dt_fim)
        
        # botões
        btn_filtrar = QPushButton("Filtrar")
        btn_filtrar.clicked.connect(self.carregar_lancamentos)
        self.lanc_filter_layout.addWidget(btn_filtrar)
        
        self.btn_edit_lanc = QPushButton("Editar Lançamento")
        self.btn_edit_lanc.setEnabled(False)
        self.btn_edit_lanc.clicked.connect(self.editar_lancamento)
        self.lanc_filter_layout.addWidget(self.btn_edit_lanc)
        
        self.btn_del_lanc = QPushButton("Excluir Lançamento")
        self.btn_del_lanc.setEnabled(False)
        self.btn_del_lanc.clicked.connect(self.excluir_lancamento)
        self.lanc_filter_layout.addWidget(self.btn_del_lanc)
        
        self.btn_import_lanc = QPushButton("Importar Lançamentos")
        self.btn_import_lanc.setIcon(QIcon.fromTheme("document-import"))
        self.btn_import_lanc.clicked.connect(self.importar_lancamentos)
        self.lanc_filter_layout.addWidget(self.btn_import_lanc)
        
        # campo de pesquisa
        self.search_lanc = QLineEdit()
        self.search_lanc.setPlaceholderText("Pesquisar…")
        self.search_lanc.textChanged.connect(self._filter_lancamentos)
        self.lanc_filter_layout.addWidget(self.search_lanc)

        # botão de filtro de colunas usando o emoji diretamente
        self.btn_filter = QToolButton()
        self.btn_filter.setText("⚙️")                 # coloca o emoji como texto
        self.btn_filter.setAutoRaise(True)             # estilo flat
        self.btn_filter.setPopupMode(QToolButton.InstantPopup)
        self.lanc_filter_layout.addWidget(self.btn_filter)

        # dentro de _setup_ui(), logo após criar self.btn_filter:

        self._lanc_filter_menu = QMenu(self)
        for col, lbl in enumerate(self._lanc_labels):
            wa = QWidgetAction(self._lanc_filter_menu)
            chk = QCheckBox(lbl)
            chk.setChecked(True)
            # conecta direto: se Checked => mostra, senão => oculta
            chk.toggled.connect(lambda vis, c=col: self._toggle_lanc_column(c, vis))
            wa.setDefaultWidget(chk)
            self._lanc_filter_menu.addAction(wa)
        
        self.btn_filter.setMenu(self._lanc_filter_menu)
        self.btn_filter.setPopupMode(QToolButton.InstantPopup)

        l_l.addLayout(self.lanc_filter_layout)

        # Tabela de lançamentos (cria antes de usar)
        self.tab_lanc = QTableWidget(0, len(self._lanc_labels))
        self.tab_lanc.setHorizontalHeaderLabels(self._lanc_labels)
        self.tab_lanc.setSelectionBehavior(QTableWidget.SelectRows)
        self.tab_lanc.setEditTriggers(QTableWidget.NoEditTriggers)
        self.tab_lanc.cellClicked.connect(lambda r, _: (
            self.btn_edit_lanc.setEnabled(True),
            self.btn_del_lanc.setEnabled(True)
        ))
        l_l.addWidget(self.tab_lanc)

        # carrega visibilidade de colunas salva
        config_file = os.path.join(CACHE_FOLDER, 'lanc_columns.json')
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                vis = json.load(f)
            for i, label in enumerate(self.tab_lanc.horizontalHeaderLabels()):
                self.tab_lanc.setColumnHidden(i, not vis.get(label, True))

        # conecta duplo‑clique do header para ordenação cíclica
        header = self.tab_lanc.horizontalHeader()
        header.sectionDoubleClicked.connect(self.toggle_sort_lanc)

        # Estilo “mais bonito”
        self.tab_lanc.setAlternatingRowColors(True)
        self.tab_lanc.setShowGrid(False)
        self.tab_lanc.verticalHeader().setVisible(False)
        hdr = self.tab_lanc.horizontalHeader()
        hdr.setHighlightSections(False)
        hdr.setDefaultAlignment(Qt.AlignCenter)
        hdr.setSectionResizeMode(QHeaderView.Stretch)
        self.tab_lanc.setStyleSheet("""
            QTableWidget::item { padding: 8px; }
            QHeaderView::section { padding: 8px; font-weight: bold; }
        """)
        
        self.tabs.addTab(w_l, "Lançamentos")

        # --- Aba Cadastros ---
        self.cadw = CadastrosWidget()
        self.tabs.addTab(self.cadw, "Cadastros")

        # --- Aba Planejamento ---
        w_p = QWidget()
        l_p = QVBoxLayout(w_p)
        l_p.setContentsMargins(10, 10, 10, 10)

        self.tab_plan = QTableWidget(0, 5)
        self.tab_plan.setHorizontalHeaderLabels([
            "Cultura", "Área", "Plantio",
            "Colheita Est.", "Prod. Est."
        ])
        self.tab_plan.setSelectionBehavior(QTableWidget.SelectRows)
        self.tab_plan.setEditTriggers(QTableWidget.NoEditTriggers)
        l_p.addWidget(self.tab_plan)

        # <<< Estilização “mais bonita” >>>
        self.tab_plan.setAlternatingRowColors(True)
        self.tab_plan.setShowGrid(False)
        self.tab_plan.verticalHeader().setVisible(False)
        hdr2 = self.tab_plan.horizontalHeader()
        hdr2.setHighlightSections(False)
        hdr2.setDefaultAlignment(Qt.AlignCenter)
        hdr2.setSectionResizeMode(QHeaderView.Stretch)
        self.tab_plan.setStyleSheet("""
            QTableWidget::item { padding: 8px; }
            QHeaderView::section { padding: 8px; font-weight: bold; }
        """)

        self.tabs.addTab(w_p, "Planejamento")

        # --- Status Bar ---
        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.status.showMessage("Sistema iniciado com sucesso!")

        # Carrega dados iniciais
        self.carregar_lancamentos()
        self.carregar_planejamento()
        # depois de carregar os lançamentos:
        self._load_lanc_filter_settings()

    def _toggle_lanc_column(self, col: int, visible: bool):
        """Esconde/exibe a coluna e salva o estado em disco."""
        self.tab_lanc.setColumnHidden(col, not visible)
        self._save_lanc_filter_settings()

    def _save_lanc_filter_settings(self):
        """Grava um JSON com o estado de todas as seções (contas, participantes, imoveis, lançamentos)."""
        os.makedirs(CACHE_FOLDER, exist_ok=True)
        path = os.path.join(CACHE_FOLDER, "lanc_filter.json")
        # carrega tudo que já existe
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}
        # atualiza só o tópico de lançamentos
        cfg["lancamentos"] = [
            not self.tab_lanc.isColumnHidden(c)
            for c in range(self.tab_lanc.columnCount())
        ]
        # salva de volta mantendo os outros tópicos intactos
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)

    def _load_lanc_filter_settings(self):
        """Carrega o JSON e aplica apenas o tópico de lançamentos."""
        path = os.path.join(CACHE_FOLDER, "lanc_filter.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            vis = cfg.get("lancamentos", [])
        except Exception:
            return

        # aplica visibilidade das colunas de lançamentos
        for c, shown in enumerate(vis):
            self.tab_lanc.setColumnHidden(c, not shown)

        # sincroniza os checkboxes do menu
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
    
        # first, hide/show columns based on the saved vis list
        for c, shown in enumerate(vis):
            self.tab_lanc.setColumnHidden(c, not shown)
    
        # now sync all the checkboxes in the menu
        for wa in self._lanc_filter_menu.actions():
            # only QWidgetActions hold our QCheckBox
            if isinstance(wa, QWidgetAction):
                chk = wa.defaultWidget()
                if isinstance(chk, QCheckBox):
                    label = chk.text()
                    try:
                        idx = self._lanc_labels.index(label)
                    except ValueError:
                        # unknown label: skip
                        continue
                    # set the checkbox to reflect the column’s visibility
                    chk.setChecked(not self.tab_lanc.isColumnHidden(idx))

    def toggle_sort_lanc(self, index: int):
        # state: 0 = sem ordenação, 1 = asc, 2 = desc
        state = self._lanc_sort_state.get(index, 0)
        if state == 0:
            self.tab_lanc.sortItems(index, Qt.AscendingOrder)
            new = 1
        elif state == 1:
            self.tab_lanc.sortItems(index, Qt.DescendingOrder)
            new = 2
        else:
            self.carregar_lancamentos()
            new = 0
        # só lembre o estado desta coluna
        self._lanc_sort_state = {index: new}

    def show_lanc_filter_dialog(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("Filtro de Colunas")
        layout = QVBoxLayout(dlg)
    
        # obtém os rótulos de cada coluna
        labels = [
            self.tab_lanc.horizontalHeaderItem(col).text()
            for col in range(self.tab_lanc.columnCount())
        ]
    
        # cria um checkbox para cada coluna
        for col, label in enumerate(labels):
            chk = QCheckBox(label)
            chk.setChecked(not self.tab_lanc.isColumnHidden(col))
            chk.stateChanged.connect(
                lambda state, c=col: self.tab_lanc.setColumnHidden(c, state != Qt.Checked)
            )
            layout.addWidget(chk)
    
        dlg.exec()
    
    
    def _filter_lancamentos(self, text: str):
        text = text.lower()
        for row in range(self.tab_lanc.rowCount()):
            hide = True
            for col in range(self.tab_lanc.columnCount()):
                item = self.tab_lanc.item(row, col)
                if item and text in item.text().lower():
                    hide = False
                    break
            self.tab_lanc.setRowHidden(row, hide)


    def _create_menu(self):
        mb = self.menuBar()
        m1 = mb.addMenu("&Arquivo")
        a1 = QAction("Novo Lançamento", self); a1.triggered.connect(self.novo_lancamento)
        m1.addAction(a1)
        a2 = QAction("Exportar Dados", self); a2.triggered.connect(self.exportar_dados)
        m1.addAction(a2)
        m1.addSeparator()
        a3 = QAction("Sair", self); a3.triggered.connect(self.close)
        m1.addAction(a3)

        m2 = mb.addMenu("&Cadastros")
        for txt,fn in [
            ("Imóvel Rural", lambda: self.tabs.setCurrentIndex(1)),
            ("Conta Bancária", lambda: self.tabs.setCurrentIndex(2)),
            ("Participante", lambda: self.tabs.setCurrentIndex(3)),
            ("Cultura", lambda: QMessageBox.information(self,"Cultura","Em desenvolvimento"))
        ]:
            act = QAction(txt, self); act.triggered.connect(fn); m2.addAction(act)

        # MainWindow._create_menu (em vez de somente Imóvel/Conta/Participante, adicione:)
        param = QAction("Parâmetros", self)
        param.triggered.connect(self.abrir_parametros)
        m2.addAction(param)

        m3 = mb.addMenu("&Relatórios")
        bal = QAction("Balancete", self); bal.triggered.connect(self.abrir_balancete)
        m3.addAction(bal)
        raz = QAction("Razão", self); raz.triggered.connect(self.abrir_razao)
        m3.addAction(raz)

        m4 = mb.addMenu("&Ajuda")
        m4.addAction(QAction("Manual do Usuário", self))
        sb = QAction("Sobre o Sistema", self); sb.triggered.connect(self.mostrar_sobre)
        m4.addAction(sb)

    def abrir_parametros(self):
        dlg = ParametrosDialog(self)
        dlg.exec()

    def _create_toolbar(self):
        tb = QToolBar("Barra de Ferramentas", self)
        tb.setIconSize(QSize(32,32))
        self.addToolBar(Qt.LeftToolBarArea, tb)
        tb.addAction(QAction(QIcon("icons/add.png"), "Novo Lançamento", self, triggered=self.novo_lancamento))
        tb.addAction(QAction(QIcon("icons/farm.png"), "Cad. Imóvel",     self, triggered=lambda: self.tabs.setCurrentIndex(1)))
        tb.addAction(QAction(QIcon("icons/bank.png"), "Cad. Conta",      self, triggered=lambda: self.tabs.setCurrentIndex(2)))
        tb.addAction(QAction(QIcon("icons/users.png"),"Cad. Participante",self, triggered=lambda: self.tabs.setCurrentIndex(3)))
        tb.addAction(QAction(QIcon("icons/report.png"),"Relatórios",     self, triggered=lambda: self.tabs.setCurrentIndex(4)))
        # Substitui o antigo "Gerar TXT LCDPR"
        tb.addAction(QAction(QIcon("icons/txt.png"), "Arquivo LCDPR", self, triggered=self.arquivo_lcdpr))

    def arquivo_lcdpr(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("Arquivo LCDPR")
        dlg.setMinimumSize(300, 150)
        layout = QVBoxLayout(dlg)

        btn_export = QPushButton("Exportar arquivo LCDPR")
        btn_import = QPushButton("Importar arquivo LCDPR")
        layout.addWidget(btn_export)
        layout.addWidget(btn_import)

        btn_export.clicked.connect(lambda: self.show_export_dialog(dlg))
        btn_import.clicked.connect(lambda: (dlg.accept(), self.importar_arquivo_lcdpr()))

        dlg.exec()


    def carregar_lancamentos(self):
        d1 = self.dt_ini.date().toString("dd/MM/yyyy")
        d2 = self.dt_fim.date().toString("dd/MM/yyyy")
        q = f"""
        SELECT l.id, l.data, i.nome_imovel,
               l.num_doc,
               p.nome AS participante,
               l.historico,
               CASE l.tipo_lanc WHEN 1 THEN 'Receita'
                                 WHEN 2 THEN 'Despesa'
                                 ELSE 'Adiantamento' END AS tipo,
               l.valor_entrada, l.valor_saida,
               (l.saldo_final * CASE l.natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) AS saldo
        FROM lancamento l
        JOIN imovel_rural i ON l.cod_imovel = i.id
        LEFT JOIN participante p ON l.id_participante = p.id
        WHERE l.data BETWEEN '{d1}' AND '{d2}'
        ORDER BY l.data DESC
        """
        rows = self.db.fetch_all(q)
        self.tab_lanc.setRowCount(len(rows))

        for r, row in enumerate(rows):
            for c, val in enumerate(row):
                # ID
                if c == 0:
                    item = NumericItem(int(val))
                # Data
                elif c == 1:
                    date = QDate.fromString(val, "dd/MM/yyyy")
                    item = QTableWidgetItem(date.toString("dd/MM/yyyy"))
                # Valores monetários: colunas 7=Entrada, 8=Saída, 9=Saldo
                elif c in (7, 8, 9):
                    num = float(val)
                    br = f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    item = NumericItem(num, f"R$ {br}")
                # Textos (Imóvel, Documento, Participante, Histórico, Tipo)
                else:
                    item = QTableWidgetItem(str(val))

                # Centraliza texto
                item.setTextAlignment(Qt.AlignCenter)

                # Cores específicas
                if c == 7:      # Entrada
                    item.setForeground(QColor("#27ae60"))
                elif c == 8:    # Saída
                    item.setForeground(QColor("#e74c3c"))
                elif c == 9:    # Saldo
                    color = "#27ae60" if float(val) >= 0 else "#e74c3c"
                    item.setForeground(QColor(color))

                self.tab_lanc.setItem(r, c, item)

    def editar_lancamento(self):
        row = self.tab_lanc.currentRow()
        lanc_id = int(self.tab_lanc.item(row,0).text())
        dlg = LancamentoDialog(self, lanc_id)
        if dlg.exec():
            self.carregar_lancamentos()
            self.dashboard.load_data()

    def excluir_lancamento(self):
        indices = self.tab_lanc.selectionModel().selectedRows()
        if not indices:
            return
        ids = [int(self.tab_lanc.item(idx.row(), 0).text()) for idx in indices]
        resp = QMessageBox.question(
            self, "Confirmar Exclusão",
            f"Excluir lançamentos IDs: {', '.join(map(str, ids))}?",
            QMessageBox.Yes | QMessageBox.No
        )
        if resp != QMessageBox.Yes:
            return
        for id_ in ids:
            try:
                self.db.execute_query("DELETE FROM lancamento WHERE id=?", (id_,))
            except Exception as e:
                QMessageBox.critical(self, "Erro", f"Erro ao excluir lançamento ID {id_}: {e}")
        self.carregar_lancamentos()
        self.dashboard.load_data()

    def carregar_planejamento(self):
        q = """
        SELECT c.nome, a.area, a.data_plantio, a.data_colheita_estimada, a.produtividade_estimada
        FROM area_producao a
        JOIN cultura c ON a.cultura_id=c.id
        """
        rows = self.db.fetch_all(q)
        self.tab_plan.setRowCount(len(rows))
        for r,(cultura,area,pl,ce,prod) in enumerate(rows):
            self.tab_plan.setItem(r,0, QTableWidgetItem(cultura))
            self.tab_plan.setItem(r,1, QTableWidgetItem(f"{area} ha"))
            self.tab_plan.setItem(r,2, QTableWidgetItem(pl or ""))
            self.tab_plan.setItem(r,3, QTableWidgetItem(ce or ""))
            self.tab_plan.setItem(r,4, QTableWidgetItem(f"{prod}"))

    def novo_lancamento(self):
        dlg = LancamentoDialog(self)
        if dlg.exec():
            self.carregar_lancamentos()
            self.dashboard.load_data()

    def cad_imovel(self):
        self.tabs.setCurrentIndex(1)

    def cad_conta(self):
        self.tabs.setCurrentIndex(2)

    def cad_participante(self):
        self.tabs.setCurrentIndex(3)

    def exportar_dados(self):
        path, _ = QFileDialog.getSaveFileName(self, "Exportar Dados", "", "CSV (*.csv)")
        if not path: return
        try:
            lancs = self.db.fetch_all("SELECT * FROM lancamento")
            with open(path,'w',newline='',encoding='utf-8') as f:
                w = csv.writer(f, delimiter=';')
                w.writerow([
                    "ID","Data","Imóvel","Conta","Documento","Tipo Doc",
                    "Histórico","Participante","Tipo","Entrada","Saída","Saldo","Natureza","Categoria"
                ])
                for l in lancs:
                    w.writerow(l[1:])
            QMessageBox.information(self, "Exportação", "Dados exportados com sucesso!")
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro na exportação: {e}")

    def gerar_txt(self, path: str = None):
        if path is None:
            last, _ = load_last_txt_path(), None
            path, _ = QFileDialog.getSaveFileName(self,"Salvar LCDPR",last,"TXT (*.txt)")
            if not path: return

        settings = QSettings("PrimeOnHub","AgroApp")
        ver   = settings.value("param/version","0013")
        iden  = settings.value("param/ident","")
        nome  = settings.value("param/nome","")
        mov   = settings.value("param/ind_mov","0")
        rec   = settings.value("param/ind_rec","0")
        dt1   = self.dt_ini.date().toString("ddMMyyyy")
        dt2   = self.dt_fim.date().toString("ddMMyyyy")

        try:
            with open(path,'w',encoding='utf-8') as f:
                f.write(f"0000|LCDPR|{ver}|{iden}|{nome}|{mov}|{rec}||{dt1}|{dt2}\n")
                f.write("0010|1\n")
                log   = settings.value("param/logradouro","")
                num   = settings.value("param/numero","")
                comp  = settings.value("param/complemento","")
                bai   = settings.value("param/bairro","")
                uf    = settings.value("param/uf","")
                mun   = settings.value("param/cod_mun","")
                cep   = settings.value("param/cep","")
                tel   = settings.value("param/telefone","")
                em    = settings.value("param/email","")
                f.write(f"0030|{log}|{num}|{comp}|{bai}|{uf}|{mun}|{cep}|{tel}|{em}\n")

                # 0040
                for im in self.db.fetch_all(
                    "SELECT cod_imovel,pais,moeda,cad_itr,caepf,insc_estadual,"
                    "nome_imovel,endereco,num,compl,bairro,uf,cod_mun,cep,"
                    "tipo_exploracao,participacao FROM imovel_rural"
                ):
                    f.write("0040|" + "|".join(str(x or "") for x in im) + "|\n")

                # 0045
                for cod, tipo, perc in self.db.fetch_all(
                    "SELECT cod_imovel,tipo_exploracao,participacao FROM imovel_rural"
                ):
                    f.write(f"0045|{cod}|{tipo}|{iden}|{nome}|{perc:.2f}|\n")

                # 0050,0100,Q100 e 9999 (inalterados)…
            save_last_txt_path(path)
            QMessageBox.information(self,"Sucesso",f"Arquivo {os.path.basename(path)} gerado!")
        except Exception as e:
            QMessageBox.critical(self,"Erro ao gerar TXT",str(e))

    def importar_arquivo_lcdpr(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Importar arquivo LCDPR", "", "TXT (*.txt);;Todos os arquivos (*)"
        )
        if not path:
            return

        # Campos esperados para o registro 0040
        expected_fields = [
            "cod_imovel","pais","moeda","cad_itr","caepf","insc_estadual",
            "nome_imovel","endereco","num","compl","bairro","uf",
            "cod_mun","cep","tipo_exploracao","participacao",
            "area_total","area_utilizada"
        ]
        warnings = []

        try:
            with open(path, 'rb') as f:
                for lineno, raw in enumerate(f, start=1):
                    # decodifica UTF-8 ou Latin‑1
                    try:
                        linha = raw.decode('utf-8')
                    except UnicodeDecodeError:
                        linha = raw.decode('latin-1')

                    parts = linha.rstrip('\r\n').split("|")
                    # remove pipe vazio inicial
                    if parts and parts[0] == "":
                        parts = parts[1:]
                    if len(parts) < 2:
                        continue

                    reg, campos = parts[0], parts[1:]

                    # === 0040: Imóveis rurais ===
                    if reg == "0040":
                        if len(campos) < 18:
                            nome_im = campos[6].strip() if len(campos) > 6 else "<sem nome>"
                            falt = [
                                expected_fields[i]
                                for i in range(18)
                                if i >= len(campos) or not campos[i].strip()
                            ]
                            warnings.append(f"L{lineno}: imóvel '{nome_im}' faltando: {', '.join(falt)}")
                            campos += [""] * (18 - len(campos))

                        (
                            cod_imovel, pais, moeda, cad_itr, caepf, insc_estadual,
                            nome_imovel, endereco, num, compl, bairro, uf,
                            cod_mun, cep, tipo_exploracao, participacao,
                            area_total, area_utilizada
                        ) = campos[:18]

                        # ajusta participacao (ex: arquivo traz 10000 → 100%)
                        try:
                            participacao_val = float(participacao) / 100.0
                        except (ValueError, TypeError):
                            participacao_val = None

                        self.db.execute_query(
                            """
                            INSERT OR REPLACE INTO imovel_rural (
                              cod_imovel,pais,moeda,cad_itr,caepf,insc_estadual,
                              nome_imovel,endereco,num,compl,bairro,uf,
                              cod_mun,cep,tipo_exploracao,participacao,
                              area_total,area_utilizada
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            [
                                cod_imovel or None, pais or None, moeda or None,
                                cad_itr or None, caepf or None, insc_estadual or None,
                                nome_imovel or None, endereco or None,
                                num or None, compl or None, bairro or None,
                                uf or None, cod_mun or None, cep or None,
                                int(tipo_exploracao) if tipo_exploracao.isdigit() else None,
                                participacao_val,
                                float(area_total) if area_total else None,
                                float(area_utilizada) if area_utilizada else None,
                            ]
                        )

                    # === 0050: Contas bancárias ===
                    elif reg == "0050":
                        # aceita 6 ou 7 campos; se 6, saldo inicia em zero
                        if len(campos) < 6:
                            continue
                        cod_cta = campos[0].strip()
                        pais_cta = campos[1].strip()
                        banco_cod = campos[2].strip()
                        nome_banco = campos[3].strip()
                        agencia = campos[4].strip()
                        num_conta = campos[5].strip()
                        # saldo pode estar em campo 6
                        if len(campos) >= 7:
                            raw_saldo = campos[6].strip()
                            try:
                                saldo_val = float(raw_saldo.replace(',', '.'))
                            except ValueError:
                                saldo_val = 0.0
                        else:
                            saldo_val = 0.0

                        self.db.execute_query(
                            """
                            INSERT OR REPLACE INTO conta_bancaria (
                                cod_conta, pais_cta, banco, nome_banco,
                                agencia, num_conta, saldo_inicial
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            [
                                cod_cta or None,
                                pais_cta or None,
                                banco_cod or None,
                                nome_banco or None,
                                agencia or None,
                                num_conta or None,
                                saldo_val
                            ]
                        )

                    # === 0100: Participantes ===
                    elif reg == "0100" and len(campos) >= 3:
                        cpf_cnpj = campos[0].strip()
                        nome_p = campos[1].strip()
                        tipo = campos[2].strip()
                        try:
                            tipo_pc = int(tipo)
                        except ValueError:
                            tipo_pc = 1 if len(re.sub(r'\D', '', cpf_cnpj)) == 11 else 2

                        self.db.execute_query(
                            """
                            INSERT OR REPLACE INTO participante (
                                cpf_cnpj, nome, tipo_contraparte
                            ) VALUES (?, ?, ?)
                            """,
                            [cpf_cnpj or None, nome_p or None, tipo_pc]
                        )

            # exibe avisos, se houver
            if warnings:
                QMessageBox.warning(
                    self, "Importação concluída com avisos",
                    "\n".join(warnings),
                    QMessageBox.Ok
                )

            # atualiza todas as abas e o painel
            self.cadw.widget(0).carregar_imoveis()
            self.cadw.widget(1).carregar_contas()
            self.cadw.widget(2).carregar_participantes()
            self.carregar_lancamentos()
            self.dashboard.load_data()

            QMessageBox.information(self, "Importação", "Arquivo LCDPR importado com sucesso!")

        except Exception as e:
            QMessageBox.warning(self, "Importação Falhou", str(e))


    def show_export_dialog(self, parent_dialog):
        parent_dialog.hide()

        dlg = QDialog(self)
        dlg.setWindowTitle("Exportar Arquivo LCDPR")
        dlg.setMinimumSize(400, 120)
        layout = QVBoxLayout(dlg)

        # Linha de caminho + botão "..."
        hl = QHBoxLayout()
        path_edit = QLineEdit(load_last_txt_path())
        path_edit.setPlaceholderText("Cole o caminho ou clique em ...")
        browse = QPushButton("...")
        hl.addWidget(path_edit)
        hl.addWidget(browse)
        layout.addLayout(hl)

        # Botões Voltar / Cancelar / Salvar
        bl = QHBoxLayout()
        voltar    = QPushButton("Voltar")
        cancelar  = QPushButton("Cancelar")
        salvar    = QPushButton("Salvar")
        bl.addWidget(voltar)
        bl.addWidget(cancelar)
        bl.addStretch()
        bl.addWidget(salvar)
        layout.addLayout(bl)

        # Conexões
        browse.clicked.connect(lambda: self._browse_save_path(path_edit))
        voltar.clicked.connect(lambda: (dlg.close(), parent_dialog.show()))
        cancelar.clicked.connect(dlg.close)
        salvar.clicked.connect(lambda: self._do_export_and_close(dlg, parent_dialog, path_edit.text()))

        dlg.exec()

    def _browse_save_path(self, path_edit: QLineEdit):
        # usa o último caminho salvo como pasta inicial
        last = load_last_txt_path()
        path, _ = QFileDialog.getSaveFileName(
            self, "Salvar Arquivo LCDPR", last, "Arquivo TXT (*.txt)"
        )
        if path:
            path_edit.setText(path)

    def _do_export_and_close(self, dlg_export: QDialog, dlg_menu: QDialog, path: str):
        if not path.strip():
            QMessageBox.warning(self, "Aviso", "Informe um caminho válido para salvar.")
            return
        # reutiliza sua função de geração de TXT, mas passando o caminho
        try:
            self.gerar_txt(path)
            save_last_txt_path(path)
            QMessageBox.information(self, "Sucesso", "Arquivo LCDPR salvo com sucesso!")
        except Exception as e:
            QMessageBox.critical(self, "Erro ao salvar", str(e))
        finally:
            dlg_export.close()
            # fecha o menu principal de Arquivo LCDPR
            dlg_menu.accept()

    def abrir_balancete(self):
        dlg = RelatorioPeriodoDialog("Balancete", self)
        if dlg.exec():
            d1,d2 = dlg.periodo
            # lógica de balancete

    def abrir_razao(self):
        dlg = RelatorioPeriodoDialog("Razão", self)
        if dlg.exec():
            d1,d2 = dlg.periodo
            # lógica de razão

    def mostrar_sobre(self):
        QMessageBox.information(
            self, "Sobre o Sistema",
            "Sistema AgroContábil - LCDPR\n\n"
            "Versão: 2.0\n"
            "© 2023 AgroTech Solutions\n\n"
            "Funcionalidades:\n"
            "- Gestão de propriedades rurais\n"
            "- Controle financeiro completo\n"
            "- Planejamento de safras\n"
            "- Gerenciamento de estoque\n"
            "- Geração do LCDPR"
        )

    def importar_lancamentos(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Importar Lançamentos", "", "TXT (*.txt);;Excel (*.xlsx *.xls)"
        )
        if not path:
            return
        try:
            if path.lower().endswith('.txt'):
                self._import_lancamentos_txt(path)
            else:
                self._import_lancamentos_excel(path)
            self.carregar_lancamentos()
            self.dashboard.load_data()
        except Exception as e:
            QMessageBox.warning(
                self, "Importação Falhou",
                f"Arquivo não segue o layout esperado:\n{e}"
            )

    def _import_lancamentos_txt(self, path):
        with open(path, encoding='utf-8') as f:
            for lineno, line in enumerate(f, 1):
                parts = line.strip().split("|")

                # Layout A: 11 campos, data ISO
                if len(parts) == 11 and re.match(r"\d{4}-\d{2}-\d{2}", parts[0]):
                    (
                        data_str, cod_imovel, cod_conta, num_doc, raw_tipo_doc,
                        historico, id_participante, tipo_lanc,
                        raw_ent, raw_sai, _
                    ) = parts
                    tipo_doc = int(raw_tipo_doc)
                    ent = float(raw_ent.replace(",", ".")) if raw_ent else 0.0
                    sai = float(raw_sai.replace(",", ".")) if raw_sai else 0.0

                # Layout B: 12 campos, data BR, e valor no campo 10 → despesa
                elif len(parts) == 12 and re.match(r"\d{2}-\d{2}-\d{4}", parts[0]):
                    (
                        data_br, cod_imovel, cod_conta, num_doc, _,
                        historico, id_participante, tipo_lanc,
                        _, raw_val, _, _
                    ) = parts
                    d, m, y = data_br.split("-")
                    data_str = f"{y}-{m}-{d}"
                    tipo_doc = 4  # Fatura/Despesa default
                    ent = 0.0
                    # trata 57000 → 570.00 como despesa
                    sai = float(raw_val) / 100.0 if raw_val.isdigit() else 0.0

                else:
                    raise ValueError(f"Linha {lineno}: formato não reconhecido ({len(parts)} colunas)")

                # sobrescreve tipo_doc por palavras-chave
                desc = historico.upper()
                if "TALAO" in desc:
                    tipo_doc = 4
                elif any(k in desc for k in ("FOLHA","IRPJ","INSS","FGTS")):
                    tipo_doc = 4

                # busca IDs
                im = self.db.fetch_one("SELECT id FROM imovel_rural WHERE cod_imovel=?", (cod_imovel,))
                if not im:
                    raise ValueError(f"Linha {lineno}: imóvel '{cod_imovel}' não encontrado")
                ct = self.db.fetch_one("SELECT id FROM conta_bancaria WHERE cod_conta=?", (cod_conta,))
                if not ct:
                    raise ValueError(f"Linha {lineno}: conta '{cod_conta}' não encontrada")
                id_imovel, id_conta = im[0], ct[0]

                # calcula saldo final
                last = self.db.fetch_one(
                    "SELECT (saldo_final * CASE natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) "
                    "FROM lancamento WHERE cod_conta=? ORDER BY id DESC LIMIT 1",
                    (id_conta,)
                )
                saldo_ant = last[0] if last and last[0] is not None else 0.0
                saldo_f = saldo_ant + ent - sai
                nat = 'P' if saldo_f >= 0 else 'N'

                # insere sem categoria
                self.db.execute_query(
                    """
                    INSERT INTO lancamento (
                        data, cod_imovel, cod_conta, num_doc, tipo_doc,
                        historico, id_participante, tipo_lanc,
                        valor_entrada, valor_saida,
                        saldo_final, natureza_saldo, categoria
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NULL)
                    """,
                    [
                        data_str,
                        id_imovel,
                        id_conta,
                        num_doc or None,
                        tipo_doc,
                        historico,
                        int(id_participante),
                        int(tipo_lanc),
                        ent,
                        sai,
                        abs(saldo_f),
                        nat
                    ]
                )


    def _import_lancamentos_excel(self, path):
        df = pd.read_excel(path, dtype=str)
        # campos que obrigatoriamente devem existir
        required = [
            'data','cod_imovel','cod_conta','num_doc','tipo_doc',
            'historico','id_participante','tipo_lanc',
            'valor_entrada','valor_saida','categoria'
        ]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Colunas faltando no Excel: {', '.join(missing)}")
        df.fillna('', inplace=True)

        for lineno, row in enumerate(df.itertuples(index=False), start=2):  # start=2 se tiver cabeçalho
            # mapeia códigos
            im = self.db.fetch_one(
                "SELECT id FROM imovel_rural WHERE cod_imovel=?", (row.cod_imovel,)
            )
            if not im:
                raise ValueError(f"Linha {lineno}: imóvel '{row.cod_imovel}' não encontrado")
            ct = self.db.fetch_one(
                "SELECT id FROM conta_bancaria WHERE cod_conta=?", (row.cod_conta,)
            )
            if not ct:
                raise ValueError(f"Linha {lineno}: conta '{row.cod_conta}' não encontrada")

            id_imovel, id_conta = im[0], ct[0]
            ent, sai = float(row.valor_entrada), float(row.valor_saida)

            # calcula saldo...
            last = self.db.fetch_one(
                "SELECT (saldo_final * CASE natureza_saldo WHEN 'P' THEN 1 ELSE -1 END) "
                "FROM lancamento WHERE cod_conta=? ORDER BY id DESC LIMIT 1",
                (id_conta,)
            )
            saldo_ant = last[0] if last and last[0] is not None else 0.0
            saldo_f = saldo_ant + ent - sai
            nat = 'P' if saldo_f >= 0 else 'N'

            # insere no banco
            self.db.execute_query(
                """
                INSERT INTO lancamento (
                    data, cod_imovel, cod_conta, num_doc, tipo_doc,
                    historico, id_participante, tipo_lanc,
                    valor_entrada, valor_saida,
                    saldo_final, natureza_saldo, categoria
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.data, id_imovel, id_conta,
                    row.num_doc or None, int(row.tipo_doc), row.historico,
                    int(row.id_participante), int(row.tipo_lanc),
                    ent, sai,
                    abs(saldo_f), nat, row.categoria
                )
            )

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
