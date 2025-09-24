# cloud_sync.py
import os
import re
import asyncio
import threading
from typing import Callable, Optional, Dict, Any

from dotenv import load_dotenv
from supabase import acreate_client, AsyncClient

# Carrega .env da pasta do projeto
load_dotenv()

def _get_env() -> tuple[str, str]:
    url = os.getenv("SUPABASE_URL", "").strip()
    key = (os.getenv("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_KEY") or "").strip()

    if not url or not key:
        raise RuntimeError(
            "Credenciais Supabase ausentes. Configure .env com SUPABASE_URL e SUPABASE_ANON_KEY."
        )

    # Validação simples de formato (evita 'Invalid URL')
    if not re.match(r"^https://[a-zA-Z0-9-]+\.supabase\.co/?$", url):
        raise RuntimeError(
            f"SUPABASE_URL inválida: {url}\nExemplo esperado: https://seu-projeto.supabase.co"
        )
    return url, key

SUPABASE_URL, SUPABASE_KEY = _get_env()

_SB: Optional[AsyncClient] = None
_LOOP: Optional[asyncio.AbstractEventLoop] = None

_CHANNEL_NAME = "realtime:lancamento"
_TABLE = "lancamento"

def _get_loop() -> asyncio.AbstractEventLoop:
    global _LOOP
    if _LOOP and _LOOP.is_running():
        return _LOOP
    _LOOP = asyncio.new_event_loop()
    t = threading.Thread(target=_LOOP.run_forever, daemon=True)
    t.start()
    return _LOOP

async def _init_async() -> None:
    global _SB
    _SB = await acreate_client(SUPABASE_URL, SUPABASE_KEY)

# Executa uma coroutine no event loop em background e retorna o resultado
def run_async(coro):
    loop = _get_loop()
    fut = asyncio.run_coroutine_threadsafe(coro, loop)
    return fut.result()

def init_client() -> None:
    loop = _get_loop()
    fut = asyncio.run_coroutine_threadsafe(_init_async(), loop)
    fut.result()

def sb() -> AsyncClient:
    assert _SB is not None, "Chame init_client() antes."
    return _SB

def init_realtime(on_change: Callable[[str, Dict[str, Any]], None]) -> None:
    loop = _get_loop()

    async def _subscribe():
        client = sb()
        ch = client.channel(_CHANNEL_NAME)

        async def _handle(payload: Dict[str, Any]):
            event = payload.get("eventType") or payload.get("type") or "*"
            threading.Thread(target=on_change, args=(event, payload), daemon=True).start()

        await ch.on_postgres_changes(
            event="*",
            schema="public",
            table=_TABLE,
            callback=lambda p: asyncio.create_task(_handle(p)),
        ).subscribe()

    asyncio.run_coroutine_threadsafe(_subscribe(), loop)

def upsert_lancamento(d: Dict[str, Any]) -> None:
    loop = _get_loop()
    async def _do():
        await sb().table("lancamento").upsert(d, on_conflict="id").execute()
    asyncio.run_coroutine_threadsafe(_do(), loop).result()

def shutdown_realtime() -> None:
    if not _LOOP:
        return
    async def _cleanup():
        if _SB:
            for ch in list(_SB.realtime.channels):
                await ch.unsubscribe()
    try:
        asyncio.run_coroutine_threadsafe(_cleanup(), _LOOP).result(timeout=2)
    except Exception:
        pass

# --- AUTH HELPERS (Supabase Auth) --------------------------------------------
from typing import Optional

_current_session = None
_current_user = None

async def _auth_sign_in(email: str, password: str):
    global _current_session, _current_user
    cli = sb()
    # supabase-auth v2: payload é dict com email/senha
    res = await cli.auth.sign_in_with_password({"email": email, "password": password})
    # res contem user e session
    _current_session = res.session
    _current_user = res.user
    return res

async def _auth_sign_out():
    global _current_session, _current_user
    cli = sb()
    try:
        await cli.auth.sign_out()
    finally:
        _current_session = None
        _current_user = None

def sign_in_blocking(email: str, password: str):
    """Chama sign-in async de forma bloqueante (para UI)."""
    return run_async(_auth_sign_in(email, password))

def sign_out_blocking():
    return run_async(_auth_sign_out())

def get_current_user() -> Optional[dict]:
    """Retorna o objeto user atual (dict-like) ou None."""
    return _current_user

# (Opcional) pegar perfil do usuário na tabela profiles
async def _load_profile():
    if not _current_user:
        return None
    cli = sb()
    uid = _current_user.id
    data = await cli.table("profiles").select("*").eq("user_id", uid).maybe_single().execute()
    return data.data

def load_profile_blocking():
    return run_async(_load_profile())

# === APP USERS (username+senha no Supabase) =============================

async def _rpc_login_user(username: str, password: str):
    """Chama a function RPC login_user no Postgres (retorna {id, username} ou vazio)."""
    res = await sb().rpc("login_user", {"p_username": username, "p_password": password}).execute()
    return (res.data or None)

async def _rpc_create_app_user(username: str, password: str):
    """Cria usuário do APP via RPC (requer ADM autenticado no Auth)."""
    res = await sb().rpc("create_app_user", {"p_username": username, "p_password": password}).execute()
    return res.data  # uuid


# --- COLAR NO cloud_sync.py (ADICIONE) -------------------------
import asyncio

def _run_in_loop(coro):
    # usa o loop já criado em init_client()
    from concurrent.futures import TimeoutError
    global _LOOP
    fut = asyncio.run_coroutine_threadsafe(coro, _LOOP)
    return fut.result()

# cloud_sync.py — trechos para registro/login de app_users

def _assert_ready():
    if _SB is None:
        raise RuntimeError("Supabase client not initialized. Call init_client() first.")

def app_create_user_blocking(username: str, password: str) -> str:
    """
    Cria usuário de aplicativo via RPC 'create_app_user'.
    Retorna o UUID criado (string). Lança exceção se username já existir.
    """
    async def _run():
        _assert_ready()
        res = await _SB.rpc('create_app_user', {
            'p_username': username,
            'p_password': password
        }).execute()
        # supabase-py v2: res.data carrega o retorno da função (uuid)
        return res.data
    return run_async(_run())

def app_login_blocking(username: str, password: str) -> bool:
    """
    Verifica credenciais do usuário de aplicativo via RPC 'verify_app_user'.
    Retorna True/False.
    """
    async def _run():
        _assert_ready()
        res = await _SB.rpc('verify_app_user', {
            'p_username': username,
            'p_password': password
        }).execute()
        return bool(res.data)
    return run_async(_run())

def admin_sign_out_blocking() -> None:
    """
    Faz logout do ADMIN, limpando a sessão atual do Supabase Auth.
    """
    async def _do():
        try:
            assert _SB is not None
            await _SB.auth.sign_out()
        except Exception as e:
            print("admin_sign_out_blocking error:", e)
    return _run_in_loop(_do())
# --- FIM DO TRECHO ---------------------------------------------
# --- alias p/ manter compatibilidade com o sistema.py
def admin_login_blocking(email: str, password: str):
    return sign_in_blocking(email, password)

# === CONSULTAS/CRUD DE LANÇAMENTOS (NUVEM COMO FONTE) =========================
from typing import List, Tuple

def _fmt_ddmmyyyy(val: str | None) -> str:
    s = (val or "").strip()
    if not s:
        return ""
    # já vem no formato?
    if "/" in s and len(s) == 10:
        return s
    # tenta YYYY-MM-DD -> DD/MM/YYYY
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    return s

def fetch_lancamentos_range_blocking(d1_ord: int, d2_ord: int) -> List[Tuple]:
    """
    Retorna linhas no MESMO formato esperado pela tabela do sistema:
    (id, data, nome_imovel, num_doc, nome_participante, historico, tipo, ent, sai, saldo, usuario)
    """
    async def _do():
        cli = sb()
        # 1) Busca os lançamentos do período
        res = await cli.table("lancamento").select(
            "id,data,cod_imovel,num_doc,id_participante,historico,tipo_lanc,"
            "valor_entrada,valor_saida,saldo_final,natureza_saldo,usuario,data_ord"
        ).gte("data_ord", d1_ord).lte("data_ord", d2_ord) \
         .order("data_ord", desc=True).order("id", desc=True).execute()
        lancs = res.data or []

        # 2) Tabelas auxiliares (nomes)
        im_ids = sorted({r.get("cod_imovel") for r in lancs if r.get("cod_imovel") is not None})
        pr_ids = sorted({r.get("id_participante") for r in lancs if r.get("id_participante") is not None})

        im_map = {}
        pr_map = {}
        if im_ids:
            r2 = await cli.table("imovel_rural").select("id,nome_imovel").in_("id", im_ids).execute()
            im_map = {r["id"]: r["nome_imovel"] for r in (r2.data or [])}
        if pr_ids:
            r3 = await cli.table("participante").select("id,nome").in_("id", pr_ids).execute()
            pr_map = {r["id"]: r["nome"] for r in (r3.data or [])}

        # 3) Monta tuplas
        out = []
        for r in lancs:
            tipo_num = r.get("tipo_lanc")
            tipo_lbl = "Receita" if tipo_num == 1 else ("Despesa" if tipo_num == 2 else "Adiantamento")
            saldo = float(r.get("saldo_final") or 0.0)
            if (r.get("natureza_saldo") or "P") != "P":
                saldo = -saldo
            out.append((
                int(r["id"]),
                _fmt_ddmmyyyy(r.get("data")),
                im_map.get(r.get("cod_imovel"), ""),
                r.get("num_doc"),
                pr_map.get(r.get("id_participante")),
                r.get("historico"),
                tipo_lbl,
                r.get("valor_entrada"),
                r.get("valor_saida"),
                saldo,
                r.get("usuario"),
            ))
        return out

    return run_async(_do())

def get_lancamento_blocking(lanc_id: int) -> dict | None:
    """Carrega 1 lançamento do Supabase (para diálogo de edição)."""
    async def _do():
        res = await sb().table("lancamento").select(
            "id,data,cod_imovel,cod_conta,num_doc,tipo_doc,historico,"
            "id_participante,tipo_lanc,valor_entrada,valor_saida,natureza_saldo"
        ).eq("id", lanc_id).maybe_single().execute()
        return res.data
    return run_async(_do())

def delete_lancamento_blocking(lanc_id: int) -> None:
    """Exclui o lançamento no Supabase."""
    async def _do():
        await sb().table("lancamento").delete().eq("id", lanc_id).execute()
    return run_async(_do())
