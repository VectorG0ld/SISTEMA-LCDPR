import os, re, sqlite3, math
from dotenv import load_dotenv
from supabase import create_client, Client

# === CONFIG ===
load_dotenv()
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_ANON_KEY"]
PROFILE      = os.environ.get("PROFILE", "Cleuber Marcos")

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_DIR, "banco_de_dados", PROFILE, "data", "lcdpr.db")

# === HELPERS ===
def chunked(iterable, size=500):
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]

def q(sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()

def to_dicts(rows, cols):
    return [dict(zip(cols, r)) for r in rows]

# === MAIN ===
if __name__ == "__main__":
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"SQLite não encontrado: {DB_PATH}")

    sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    conn = sqlite3.connect(DB_PATH)

    print("→ Migrando IMÓVEIS…")
    rows = q("""select id,cod_imovel,pais,moeda,cad_itr,caepf,insc_estadual,
                       nome_imovel,endereco,num,compl,bairro,uf,cod_mun,cep,
                       tipo_exploracao,participacao,area_total,area_utilizada,date(data_cadastro)
                  from imovel_rural""")
    cols = ["id","cod_imovel","pais","moeda","cad_itr","caepf","insc_estadual",
            "nome_imovel","endereco","num","compl","bairro","uf","cod_mun","cep",
            "tipo_exploracao","participacao","area_total","area_utilizada","data_cadastro"]
    data = to_dicts(rows, cols)
    for ch in chunked(data, 500):
        sb.table("imovel_rural").upsert(ch, on_conflict="cod_imovel").execute()

    print("→ Migrando CONTAS…")
    rows = q("""select id,cod_conta,pais_cta,banco,nome_banco,agencia,num_conta,saldo_inicial,date(data_abertura)
                  from conta_bancaria""")
    cols = ["id","cod_conta","pais_cta","banco","nome_banco","agencia","num_conta","saldo_inicial","data_abertura"]
    data = to_dicts(rows, cols)
    for ch in chunked(data, 500):
        sb.table("conta_bancaria").upsert(ch, on_conflict="cod_conta").execute()

    print("→ Migrando PARTICIPANTES…")
    rows = q("""select id,cpf_cnpj,nome,tipo_contraparte,date(data_cadastro) from participante""")
    cols = ["id","cpf_cnpj","nome","tipo_contraparte","data_cadastro"]
    data = to_dicts(rows, cols)
    for ch in chunked(data, 500):
        sb.table("participante").upsert(ch, on_conflict="cpf_cnpj").execute()

    print("→ Migrando PERFIL_PARAM…")
    rows = q("""select profile,version,ind_ini_per,sit_especial,ident,nome,logradouro,numero,complemento,
                       bairro,uf,cod_mun,cep,telefone,email,updated_at from perfil_param""")
    if rows:
        cols = ["profile","version","ind_ini_per","sit_especial","ident","nome","logradouro","numero","complemento",
                "bairro","uf","cod_mun","cep","telefone","email","updated_at"]
        data = to_dicts(rows, cols)
        for ch in chunked(data, 500):
            sb.table("perfil_param").upsert(ch, on_conflict="profile").execute()

    print("→ Migrando LANÇAMENTOS… (pode demorar)")
    rows = q("""select id,data,cod_imovel,cod_conta,num_doc,tipo_doc,historico,id_participante,tipo_lanc,
                       valor_entrada,valor_saida,saldo_final,natureza_saldo,usuario,categoria,data_ord,
                       area_afetada,quantidade,unidade_medida
                  from lancamento
                 order by id""")
    cols = ["id","data","cod_imovel","cod_conta","num_doc","tipo_doc","historico","id_participante","tipo_lanc",
            "valor_entrada","valor_saida","saldo_final","natureza_saldo","usuario","categoria","data_ord",
            "area_afetada","quantidade","unidade_medida"]
    data = to_dicts(rows, cols)

    # normaliza num_doc (mesma regra do app)
    def norm_num_doc(s):
        if s is None: return None
        n = re.sub(r"\D+", "", str(s))
        return n or None

    for d in data:
        d["num_doc"] = norm_num_doc(d.get("num_doc"))

    # insere em lotes mantendo id como chave primária natural (bigserial aceita, mas não regrava seq)
    for ch in chunked(data, 500):
        sb.table("lancamento").upsert(ch, on_conflict="id").execute()

    print("✔ Migração concluída.")
