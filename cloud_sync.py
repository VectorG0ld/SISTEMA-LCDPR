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

