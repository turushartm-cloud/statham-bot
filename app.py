"""
Statham Trading Bot — RENDER (Unified v2.0)
============================================
Два брокера: Bybit  и  BingX — торгуют одновременно.
Для каждой биржи свой набор пар (BYBIT_PAIRS / BINGX_PAIRS).

Эндпоинты:
  POST /webhook/telegram  — Telegram-апдейты
  POST /webhook/bybit     — TradingView-сигналы
  GET  /health            — Healthcheck (публичный)
  GET  /setup             — Переустановить вебхук Telegram
  GET  /debug             — Последние 100 строк лога   [требует ?secret=RENDER_SECRET]
  GET  /trades            — Активные сделки            [требует ?secret=RENDER_SECRET]
  GET  /stats             — Статистика                 [требует ?secret=RENDER_SECRET]
  GET  /history           — История сделок             [требует ?secret=RENDER_SECRET]
  GET  /positions         — Открытые позиции           [требует ?secret=RENDER_SECRET]
  POST /close_all         — Аварийное закрытие         [требует ?secret=RENDER_SECRET]
  GET  /test_bybit        — Проверка Bybit API         [требует ?secret=RENDER_SECRET]
  GET  /test_bingx        — Проверка BingX API         [требует ?secret=RENDER_SECRET]

ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ:
  TG_TOKEN            — токен Telegram-бота
  TG_CHAT             — ID чата (-100...)
  TG_SIGNALS_TOPIC    — ID ветки сигналов
  TG_SESSIONS_TOPIC   — ID ветки сессий/F&G
  RENDER_URL          — свой URL на Render (для keepalive)
  ADMIN_IDS           — разрешённые Telegram user ID через запятую

  # ── Bybit ────────────────────────────────────────────────────────
  BYBIT_API_KEY       — ключ Bybit
  BYBIT_API_SECRET    — секрет Bybit
  TESTNET             — "false" для реального (ВАЖНО!)
  BYBIT_PAIRS         — пары для Bybit: BTCUSDT,ETHUSDT,...
                        (если пусто — Bybit НЕ торгует, только Telegram)

  # ── BingX ────────────────────────────────────────────────────────
  BINGX_API_KEY       — ключ BingX
  BINGX_API_SECRET    — секрет BingX
  BINGX_DEMO          — "true" для демо-счёта BingX
  BINGX_PAIRS         — пары для BingX: ASTERUSDT,EDGEUSDT,...
                        (если пусто — BingX НЕ торгует, только Telegram)

  # ── Торговля (общее) ─────────────────────────────────────────────
  ALLOWED_PAIRS       — не используется напрямую; формируется как объединение BYBIT_PAIRS + BINGX_PAIRS
  DEFAULT_LEVERAGE    — плечо по умолчанию (10)
  DEFAULT_SIZE_USDT   — размер позиции в USDT (1)
  TRAIL_PCT           — трейлинг % (0.5)
  PAIR_SETTINGS_JSON  — '{"BTCUSDT":{"leverage":10,"size_usdt":100}}'
  DATA_DIR            — директория для JSON-файлов (/tmp или /data)

  # ── Новые фильтры (v2.1) ────────────────────────────────────────────
  RENDER_SECRET       — секрет для /debug /trades /stats эндпоинтов

  # ── Better Stack app-level logs (только этот bot, без workspace stream) ─────
  BETTERSTACK_ENABLED      — "true" включает отправку structured logs
  BETTERSTACK_SOURCE_TOKEN — Source token из Better Stack HTTP source
  BETTERSTACK_INGEST_URL   — HTTPS ingest URL, например https://$INGESTING_HOST
"""

from __future__ import annotations
import calendar, hashlib, hmac as _hmac, json, math, os, re, time
import threading, datetime, logging
from flask import Flask, request, jsonify
import requests
import telebot

# ── Redis (Upstash) ────────────────────────────────────────────────────────────
try:
    import redis as _redis_lib
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

# ── Bybit ──────────────────────────────────────────────────────────────────────
try:
    from pybit.unified_trading import HTTP as BybitHTTP
    BYBIT_LIB = True
except ImportError:
    BYBIT_LIB = False

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Webhook/data contract.  These values are persisted with every new trade so
# the dashboard can isolate a clean post-v153 cohort without rotating Redis.
STRATEGY_VERSION = "v153"
SCHEMA_VERSION = 2
TP_CONTRACT = "4TP_20_25_30_25"

# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════════════════════════
TG_TOKEN          = os.environ.get("TG_TOKEN",          "")
TG_CHAT           = os.environ.get("TG_CHAT",           "-1003867089540")
TG_SIGNALS_TOPIC  = os.environ.get("TG_SIGNALS_TOPIC",  "6314")
TG_SESSIONS_TOPIC = os.environ.get("TG_SESSIONS_TOPIC", "1")
RENDER_URL        = os.environ.get("RENDER_URL",        "")

TESTNET = os.environ.get("TESTNET", "false").lower() == "true"

# Bybit
BYBIT_API_KEY    = os.environ.get("BYBIT_API_KEY",    "")
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "")

# BingX
BINGX_API_KEY    = os.environ.get("BINGX_API_KEY",    "")
BINGX_API_SECRET = os.environ.get("BINGX_API_SECRET", "")
BINGX_DEMO       = os.environ.get("BINGX_DEMO", "false").lower() == "true"
RENDER_SECRET    = (os.environ.get("RENDER_SECRET", "").strip()
                    or os.environ.get("DASHBOARD_SECRET", "").strip())

# App-level Better Stack logger.  Disabled unless explicitly enabled via ENV.
# Sends only statham-bot logs; avoids Render workspace-wide log stream noise.
BETTERSTACK_ENABLED = os.environ.get("BETTERSTACK_ENABLED", "false").lower() == "true"
BETTERSTACK_SOURCE_TOKEN = os.environ.get("BETTERSTACK_SOURCE_TOKEN", "").strip()
BETTERSTACK_INGEST_URL = os.environ.get("BETTERSTACK_INGEST_URL", "").strip().rstrip("/")
if BETTERSTACK_INGEST_URL and not BETTERSTACK_INGEST_URL.startswith(("http://", "https://")):
    BETTERSTACK_INGEST_URL = "https://" + BETTERSTACK_INGEST_URL
BETTERSTACK_TIMEOUT_SEC = float(os.environ.get("BETTERSTACK_TIMEOUT_SEC", "2"))

BYBIT_AVAILABLE = BYBIT_LIB and bool(BYBIT_API_KEY) and bool(BYBIT_API_SECRET)
BINGX_AVAILABLE = bool(BINGX_API_KEY) and bool(BINGX_API_SECRET)

ADMIN_IDS = set(
    int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",")
    if x.strip().lstrip("-").isdigit()
)


def _parse_pairs(env_key: str) -> set[str]:
    """
    Парсит список пар из переменной окружения.
    ПУСТОЙ env = биржа не торгует ничем (NO fallback на другую переменную).
    Логика: хочешь торговать парой — явно укажи в BYBIT_PAIRS / BINGX_PAIRS.
    Значения NONE, NULL, - также трактуются как «пусто».
    """
    raw = os.environ.get(env_key, "").strip()
    if not raw:
        return set()
    return {p.strip().upper().replace(".P", "") for p in raw.split(",")
            if p.strip() and p.strip().upper() not in ("NONE", "NULL", "-")}


# Legacy ENV only: no longer gates trading. All valid webhook entries are eligible.
BYBIT_PAIRS   = _parse_pairs("BYBIT_PAIRS")
BINGX_PAIRS   = _parse_pairs("BINGX_PAIRS")
ALLOWED_PAIRS = set()
PAIR_GATE_ENABLED = False

# DEFAULT_LEVERAGE должен быть одним числом, например "10"
# Используй PAIR_SETTINGS_JSON для разных плечей на разные пары
def _parse_leverage(raw: str, default: int = 10) -> int:
    """Безопасный парсинг плеча. Игнорирует мусор вроде '5-50'."""
    try:
        v = int(str(raw).strip().split("-")[0].split()[0])
        return max(1, min(v, 200))
    except Exception:
        return default
DEFAULT_LEVERAGE  = _parse_leverage(os.environ.get("DEFAULT_LEVERAGE", "10"))
DEFAULT_SIZE_USDT = float(os.environ.get("DEFAULT_SIZE_USDT", "1"))
TRAIL_PCT         = 0.0  # Trail управляется индикатором через sl_moved алерты

# ✅ IMPROVEMENT #9: Cooldown после SL hit (в секундах = bars × tf_minutes × 60)



def _parse_pair_settings(raw: str) -> dict:
    """
    Парсит PAIR_SETTINGS_JSON с автоисправлением распространённых ошибок.
    Частая проблема: лишние }} в конце значений из Render ENV editor:
      {"STOUSDT":{...}},"YBUSDT":{...}}   ← двойные }} закрывают JSON раньше
    Стратегия: пробуем raw → удаляем дубли }} → пересобираем вручную.
    """
    if not raw or raw.strip() in ("{}", ""):
        return {}
    # Попытка 1: прямой парсинг
    try:
        result = json.loads(raw)
        if isinstance(result, dict):
            return result
    except Exception:
        pass
    # Попытка 2: исправляем "}}" внутри значений объектов (не в конце строки)
    # Заменяем }}, на }, — типичная ошибка Render ENV editor
    try:
        fixed = raw.replace("}},", "},")
        result = json.loads(fixed)
        if isinstance(result, dict):
            log.warning(f"PAIR_SETTINGS_JSON | auto-fixed }}, → }}, parsed OK: {list(result.keys())}")
            return result
    except Exception:
        pass
    # Попытка 3: находим все пары ключ:объект через regex
    try:
        import re
        pairs = {}
        pattern = r'"([A-Z0-9]+)"\s*:\s*(\{[^{}]+\})'
        for m in re.finditer(pattern, raw):
            try:
                pairs[m.group(1)] = json.loads(m.group(2))
            except Exception:
                pass
        if pairs:
            log.warning(f"PAIR_SETTINGS_JSON | regex fallback, parsed {len(pairs)} pairs: {list(pairs.keys())}")
            return pairs
    except Exception:
        pass
    log.error(f"PAIR_SETTINGS_JSON | all parse attempts failed, using empty dict. Raw: {raw[:200]}")
    return {}

PAIR_SETTINGS: dict = {}  # PAIR_SETTINGS_JSON deprecated: use DEFAULT_LEVERAGE / DEFAULT_SIZE_USDT only.

# TP_CLOSE_PCT: распределение частичных закрытий по уровням TP
# ✅ EDGE-FIX: 4 TP, «прибыль течёт» — только 45% выходит у входа (TP1+TP2),
# 55% бежит до TP3/TP4 (3.5R/5R). Контракт содержит ровно четыре TP.
TP_CLOSE_PCT = {1: 0.20, 2: 0.25, 3: 0.30, 4: 0.25}


def calc_trade_pnl(pos: dict, sl_exit_price: float | None = None) -> dict:
    """
    Рассчитывает реализованный P&L с учётом частичных TP-закрытий и SL-выхода.

    Логика:
      - Для каждого tp{n}_hit: закрыта доля TP_CLOSE_PCT[n] от total_qty по tp{n}_price
      - Остаток (remaining_qty) закрыт по sl_exit_price
      - direction BUY:  profit% = (exit - entry) / entry × leverage
      - direction SELL: profit% = (entry - exit) / entry × leverage

    Возвращает dict: {
        "pnl_pct": float,          # итоговый P&L в % (с учётом плеча)
        "pnl_pct_no_lev": float,   # P&L без плеча (raw move)
        "tp_pnl_pct": float,       # P&L только от TP-частей
        "sl_pnl_pct": float,       # P&L от SL-остатка
        "highest_tp": int,
        "tps_hit": list[int],
        "remaining_pct": float,    # доля позиции, закрытая на SL (0–1)
        "closed_on_tp_pct": float, # доля, закрытая на TP (0–1)
    }
    """
    entry  = float(pos.get("entry_price") or 0)
    lev    = float(pos.get("leverage") or 1)
    dirn   = str(pos.get("direction") or "BUY").upper()
    total  = float(pos.get("total_qty") or 1)

    if entry <= 0:
        return {"pnl_pct": 0.0, "pnl_pct_no_lev": 0.0,
                "tp_pnl_pct": 0.0, "sl_pnl_pct": 0.0,
                "highest_tp": 0, "tps_hit": [],
                "remaining_pct": 0.0, "closed_on_tp_pct": 0.0}

    tps_hit: list[int] = []
    closed_pct   = 0.0   # доля позиции, закрытая на TP (0–1)
    tp_pnl_pct   = 0.0   # накопленный P&L от TP-закрытий (без плеча)

    for n in range(1, 5):
        if not pos.get(f"tp{n}_hit"):
            continue
        tp_price = float(pos.get(f"tp{n}_price") or 0)
        if tp_price <= 0:
            continue
        share = TP_CLOSE_PCT.get(n, 0.0)
        if dirn == "BUY":
            move = (tp_price - entry) / entry
        else:
            move = (entry - tp_price) / entry
        tp_pnl_pct += move * share
        closed_pct  += share
        tps_hit.append(n)

    remaining_pct = max(0.0, 1.0 - closed_pct)
    sl_pnl_pct    = 0.0

    if sl_exit_price and sl_exit_price > 0 and remaining_pct > 0:
        if dirn == "BUY":
            sl_move = (sl_exit_price - entry) / entry
        else:
            sl_move = (entry - sl_exit_price) / entry
        sl_pnl_pct = sl_move * remaining_pct

    total_no_lev = tp_pnl_pct + sl_pnl_pct
    total_with_lev = total_no_lev * lev
    notional_usd = entry * total
    pnl_usd = notional_usd * total_no_lev

    return {
        "pnl_pct":          round(total_with_lev * 100, 2),
        "pnl_pct_no_lev":   round(total_no_lev  * 100, 2),
        "tp_pnl_pct":       round(tp_pnl_pct    * 100, 2),
        "sl_pnl_pct":       round(sl_pnl_pct    * 100, 2),
        "highest_tp":       max(tps_hit, default=0),
        "tps_hit":          tps_hit,
        "remaining_pct":    round(remaining_pct * 100, 1),
        "closed_on_tp_pct": round(closed_pct    * 100, 1),
        "pnl_usd":          round(pnl_usd, 4),
        "notional_usd":     round(notional_usd, 4),
        "margin_usd":       round(notional_usd / lev, 4) if lev > 0 else None,
    }

DATA_DIR = os.environ.get("DATA_DIR", "/tmp")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Redis connection ───────────────────────────────────────────────────────────
_redis_client = None
_REDIS_PREFIX  = "statham:"

def _get_redis():
    """Возвращает Redis-клиент или None если Redis недоступен.
    При таймауте/ошибке инвалидирует клиент и пересоздаёт при следующем вызове."""
    global _redis_client
    if not _REDIS_AVAILABLE:
        return None
    url = os.environ.get("REDIS_URL", "").strip()
    if not url:
        return None
    if _redis_client is not None:
        return _redis_client
    try:
        client = _redis_lib.from_url(
            url, decode_responses=True,
            # Upstash имеет cold-start до 2–3 сек; 10s timeout надёжнее
            socket_timeout=10, socket_connect_timeout=10,
            retry_on_timeout=True,
            health_check_interval=30,
        )
        client.ping()
        _redis_client = client
        log.info("Redis | connected OK")
    except Exception as e:
        log.warning(f"Redis | connect failed: {e}")
    return _redis_client


def _redis_invalidate():
    """Сбрасывает кешированный клиент — следующий вызов _get_redis() пересоздаст."""
    global _redis_client
    _redis_client = None

def _rkey(path: str) -> str:
    """Преобразует путь к файлу в ключ Redis: /tmp/active_trades.json → statham:active_trades"""
    return _REDIS_PREFIX + os.path.basename(path).replace(".json", "")

STATS_FILE      = os.path.join(DATA_DIR, "trade_stats.json")
TRADES_FILE     = os.path.join(DATA_DIR, "active_trades.json")
HISTORY_FILE    = os.path.join(DATA_DIR, "trade_history.json")
POSITIONS_FILE  = os.path.join(DATA_DIR, "positions.json")
FG_STATE_FILE   = os.path.join(DATA_DIR, "fg_state.json")
SENT_FLAGS_FILE = os.path.join(DATA_DIR, "sent_flags.json")
CLOSED_TRADES_FILE = os.path.join(DATA_DIR, "closed_trades.json")
EQUITY_HISTORY_FILE = os.path.join(DATA_DIR, "equity_history.json")
LOG_FILE        = os.path.join(DATA_DIR, "bot.log")

MAX_QUEUE_ATTEMPTS = 3
QUEUE_RETRY_DELAY  = 15
_signal_queue: list[dict] = []
_queue_lock   = threading.Lock()
_pos_lock     = threading.Lock()
_bybit_lock   = threading.Lock()
_state_lock   = threading.RLock()
_bg_lock      = threading.Lock()
_emergency_stop = threading.Event()
_bg_started   = False

FG_SCHEDULED_HOURS = {4, 10, 16, 22}

bot = telebot.TeleBot(TG_TOKEN, threaded=False) if TG_TOKEN else None


# ══════════════════════════════════════════════════════════════════════════════
# СТАРТОВЫЕ ПРЕДУПРЕЖДЕНИЯ
# ══════════════════════════════════════════════════════════════════════════════
def _startup_warnings():
    if not TESTNET and (BYBIT_AVAILABLE or BINGX_AVAILABLE):
        log.warning("LIVE TRADING ACTIVE! TESTNET=false — торговля реальными деньгами!")
    if BYBIT_AVAILABLE:
        log.info(f"Bybit AVAILABLE | testnet={TESTNET} | pair gate disabled")
    if BINGX_AVAILABLE:
        log.info(f"BingX AVAILABLE | demo={BINGX_DEMO} | pair gate disabled")

    # ── Redis — обязательное хранилище ───────────────────────────────────
    redis_url = os.environ.get("REDIS_URL", "").strip()
    if not redis_url:
        log.critical(
            "REDIS_URL не задана! Все данные (сделки, позиции, история) "
            "хранятся только в /tmp и будут ПОТЕРЯНЫ при перезапуске Render. "
            "Задайте REDIS_URL в переменных окружения."
        )
    elif not _REDIS_AVAILABLE:
        log.critical(
            "Пакет redis не установлен. "
            "Данные хранятся только в /tmp — при перезапуске ПОТЕРЯЮТСЯ!"
        )
    else:
        r = _get_redis()
        if r is None:
            log.critical(
                "Redis недоступен (соединение не установлено). "
                "Данные хранятся только в /tmp — при перезапуске ПОТЕРЯЮТСЯ!"
            )
        else:
            log.info("Redis | storage OK — данные персистентны")

_startup_warnings()


# ══════════════════════════════════════════════════════════════════════════════
# БЕЗОПАСНОСТЬ
# ══════════════════════════════════════════════════════════════════════════════
def _http_auth(req) -> bool:
    """Protect operational JSON endpoints; /health remains public."""
    if not RENDER_SECRET:
        return False
    supplied = (req.args.get("secret", "").strip()
                or req.headers.get("X-Render-Secret", "").strip())
    return _hmac.compare_digest(supplied, RENDER_SECRET)


_rate_store: dict[str, list[float]] = {}
_rate_lock  = threading.Lock()
RATE_LIMIT  = 30
RATE_WINDOW = 60


def _rate_ok(ip: str) -> bool:
    now = time.time()
    with _rate_lock:
        times = [t for t in _rate_store.get(ip, []) if now - t < RATE_WINDOW]
        if len(times) >= RATE_LIMIT:
            return False
        times.append(now)
        _rate_store[ip] = times
        # Удаляем IP-адреса без активных запросов (чистим утечку памяти)
        stale = [k for k, v in _rate_store.items() if not v]
        for k in stale:
            del _rate_store[k]
    return True


# ══════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════════════════════
_BS_EVENT_MAP = (
    ("WEBHOOK |", "webhook_received"),
    ("WEBHOOK_COMPAT |", "webhook_received"),
    ("PARSED |", "entry_parsed"),
    ("ENTRY |", "exchange_market_order"),
    ("BYBIT_MARKET |", "exchange_market_order"),
    ("BINGX_MARKET |", "exchange_market_order"),
    ("BYBIT_STOP |", "sl_order_placed"),
    ("BINGX_STOP |", "sl_order_placed"),
    ("TP_ORDERS_PLACED |", "tp_orders_placed"),
    ("TP_HIT_EXCHANGE |", "tp_hit"),
    ("TP_REJECT |", "tp_reject"),
    ("SL_MOVED |", "sl_moved"),
    ("SL_HIT", "sl_hit"),
    ("BINGX_API_ERR |", "exchange_error"),
    ("BYBIT_", "exchange_event"),
    ("SYNC_STATE |", "state_sync"),
)


def _redact_log_text(text: str) -> str:
    text = re.sub(r"(?i)(secret=)[^&\s]+", r"\1***", str(text))
    text = re.sub(r"(?i)(token=)[^&\s]+", r"\1***", text)
    text = re.sub(r"(?i)(api[_-]?key=)[^&\s]+", r"\1***", text)
    return text


def _betterstack_event_name(entry: str) -> str:
    for prefix, event_name in _BS_EVENT_MAP:
        if entry.startswith(prefix):
            return event_name
    return "bot_log"


def _extract_log_field(entry: str, name: str) -> str:
    m = re.search(rf"\b{name}=([^\s|]+)", entry)
    return m.group(1) if m else ""


def _send_betterstack_log(payload: dict):
    if not (BETTERSTACK_ENABLED and BETTERSTACK_SOURCE_TOKEN and BETTERSTACK_INGEST_URL):
        return
    try:
        requests.post(
            BETTERSTACK_INGEST_URL,
            headers={
                "Authorization": f"Bearer {BETTERSTACK_SOURCE_TOKEN}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=BETTERSTACK_TIMEOUT_SEC,
        )
    except Exception:
        # Logging must never block or break trading.
        pass


def _emit_betterstack_log(entry: str, ts_iso: str):
    if not (BETTERSTACK_ENABLED and BETTERSTACK_SOURCE_TOKEN and BETTERSTACK_INGEST_URL):
        return
    safe_entry = _redact_log_text(entry)
    event_name = _betterstack_event_name(safe_entry)
    payload = {
        "dt": ts_iso,
        "service": "statham-bot",
        "environment": os.environ.get("RENDER_SERVICE_NAME", "render"),
        "event": event_name,
        "message": safe_entry,
        "strategy_version": STRATEGY_VERSION,
        "schema_version": SCHEMA_VERSION,
        "tp_contract": TP_CONTRACT,
        "ticker": _extract_log_field(safe_entry, "ticker"),
        "trade_id": _extract_log_field(safe_entry, "trade_id"),
    }
    threading.Thread(target=_send_betterstack_log, args=(payload,), daemon=True).start()


def log_event(event: str, **fields):
    """Structured Better Stack event. Must never break trading path."""
    if not (BETTERSTACK_ENABLED and BETTERSTACK_SOURCE_TOKEN and BETTERSTACK_INGEST_URL):
        return
    try:
        now = datetime.datetime.utcnow()
        safe_fields = {}
        for key, value in fields.items():
            if value is None:
                continue
            safe_fields[str(key)] = _redact_log_text(value) if isinstance(value, str) else value
        payload = {
            "dt": now.isoformat(timespec="milliseconds") + "Z",
            "service": "statham-bot",
            "environment": os.environ.get("RENDER_SERVICE_NAME", "render"),
            "event": str(event or "bot_event"),
            "strategy_version": STRATEGY_VERSION,
            "schema_version": SCHEMA_VERSION,
            "tp_contract": TP_CONTRACT,
            **safe_fields,
        }
        threading.Thread(target=_send_betterstack_log, args=(payload,), daemon=True).start()
    except Exception:
        pass


def write_log(entry: str):
    now  = datetime.datetime.utcnow()
    ts   = now.strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {entry}\n"
    log.info(entry)
    _emit_betterstack_log(entry, now.isoformat(timespec="milliseconds") + "Z")
    try:
        with _file_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line)
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
            if len(lines) > 1000:
                with open(LOG_FILE, "w", encoding="utf-8") as f:
                    f.writelines(lines[-1000:])
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# JSON ХЕЛПЕРЫ (с атомарной записью)
# ══════════════════════════════════════════════════════════════════════════════
_file_lock = threading.Lock()


def load_json(path: str, default):
    # 1) Пробуем Redis
    r = _get_redis()
    if r is not None:
        try:
            val = r.get(_rkey(path))
            if val is not None:
                return json.loads(val)
        except Exception as e:
            log.warning(f"REDIS_LOAD_ERR | {_rkey(path)} | {e}")
            # Таймаут/обрыв → сбрасываем клиент; следующий вызов пересоздаст
            _redis_invalidate()
    # 2) Фолбэк: файл
    if not os.path.exists(path):
        return default
    try:
        with _file_lock:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        write_log(f"LOAD_JSON_ERR | {path} | {e}")
        return default


def save_json(path: str, data):
    serialized = json.dumps(data, ensure_ascii=False)
    # 1) Сохраняем в Redis
    r = _get_redis()
    if r is not None:
        try:
            r.set(_rkey(path), serialized)
        except Exception as e:
            log.warning(f"REDIS_SAVE_ERR | {_rkey(path)} | {e}")
            _redis_invalidate()
    # 2) Фолбэк: файл (атомарная запись)
    try:
        tmp = path + ".tmp"
        with _file_lock:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(serialized)
            os.replace(tmp, path)
    except Exception as e:
        write_log(f"SAVE_JSON_ERR | {path} | {e}")


def load_stats()     : return load_json(STATS_FILE,    {"wins": 0, "losses": 0, "total": 0})
def load_trades()    : return load_json(TRADES_FILE,   {})
def load_history()   : return load_json(HISTORY_FILE,  [])
def load_positions() : return load_json(POSITIONS_FILE,{})
def save_stats(d)    : save_json(STATS_FILE,    d)
def save_trades(d)   : save_json(TRADES_FILE,   d)
def save_history(d)  : save_json(HISTORY_FILE,  d)
def save_positions(d): save_json(POSITIONS_FILE, d)


def load_closed_trades() -> dict:
    return load_json(CLOSED_TRADES_FILE, {})


def save_closed_trades(d: dict):
    save_json(CLOSED_TRADES_FILE, d)


def _was_sent(key: str) -> bool:
    with _state_lock:
        return load_json(SENT_FLAGS_FILE, {}).get(key, False)


def _mark_sent(key: str):
    with _state_lock:
        flags = load_json(SENT_FLAGS_FILE, {})
        flags[key] = True
        save_json(SENT_FLAGS_FILE, flags)


# ══════════════════════════════════════════════════════════════════════════════
# ХЕЛПЕРЫ
# ══════════════════════════════════════════════════════════════════════════════
def _msk() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)


def calc_winrate(wins: int, total: int) -> float:
    return round(wins / total * 100, 1) if total > 0 else 0.0


def fmt_duration(seconds: int) -> str:
    if seconds < 3600:  return f"{seconds // 60}м"
    if seconds < 86400: return f"{seconds // 3600}ч {(seconds % 3600) // 60}м"
    return f"{seconds // 86400}д {(seconds % 86400) // 3600}ч"


def is_admin_user(user_id: int) -> bool:
    if ADMIN_IDS and user_id in ADMIN_IDS:
        return True
    if not ADMIN_IDS and bot and TG_CHAT:
        try:
            admins = bot.get_chat_administrators(TG_CHAT)
            return user_id in [a.user.id for a in admins]
        except Exception:
            pass
    return False


def get_exchange_for(ticker: str) -> str:
    """Single-exchange fallback preference: BingX → Bybit."""
    if BINGX_AVAILABLE:
        return "bingx"
    if BYBIT_AVAILABLE:
        return "bybit"
    return "none"


def _execution_mode() -> str:
    return str(os.getenv("EXECUTION_MODE", "dual")).strip().lower()


def _dual_execution_enabled() -> bool:
    return _execution_mode() in ("dual", "both", "multi") and BINGX_AVAILABLE and BYBIT_AVAILABLE


def _entry_execution_targets() -> list[str]:
    if _dual_execution_enabled():
        return ["bingx", "bybit"]
    primary = get_exchange_for("")
    return [primary] if primary != "none" else []


def _active_exchanges_for(ticker: str, direction: str) -> list[str]:
    ticker = str(ticker or "").upper().replace(".P", "")
    direction = str(direction or "").upper()
    out: list[str] = []
    with _pos_lock:
        positions = load_positions()
        for ex in ("bingx", "bybit"):
            if positions.get(pos_key(ticker, direction, ex)):
                out.append(ex)
        legacy = positions.get(pos_key(ticker, direction))
        if legacy and not out:
            ex = str(legacy.get("exchange") or "").lower()
            out.append(ex if ex in ("bingx", "bybit") else "legacy")
    return out


def _is_exchange_offline_error(exc: Exception | str) -> bool:
    msg = str(exc).lower()
    return (
        "109418" in msg
        or "offline currently" in msg
        or "symbol is offline" in msg
        or "contract is offline" in msg
    )


def _entry_exchange_candidates(primary: str) -> list[str]:
    out: list[str] = []
    if primary and primary != "none":
        out.append(primary)
    if primary == "bingx" and BYBIT_AVAILABLE:
        out.append("bybit")
    return out


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM — ОТПРАВКА
# ══════════════════════════════════════════════════════════════════════════════
def send_tg(text: str, thread_id=None, chat_id=None, reply_to=None) -> dict:
    if not TG_TOKEN or not TG_CHAT:
        return {}
    if chat_id is None:
        chat_id = TG_CHAT
    payload: dict = {
        "chat_id":                  chat_id,
        "text":                     text,
        "parse_mode":               "HTML",
        "disable_web_page_preview": True,
    }
    if thread_id is not None:
        try: payload["message_thread_id"] = int(thread_id)
        except (ValueError, TypeError): pass
    if reply_to:
        payload["reply_parameters"] = {
            "message_id": reply_to,
            "allow_sending_without_reply": True,
        }
    for attempt in range(3):
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json=payload, timeout=15,
            )
            data = resp.json()
            if not data.get("ok"):
                err_code = data.get("error_code", "?")
                err_desc = data.get("description", "?")
                write_log(f"SEND_TG_FAIL | attempt={attempt} | http={resp.status_code} code={err_code} desc={err_desc}")
                # 429 = flood wait
                if resp.status_code == 429:
                    retry_after = data.get("parameters", {}).get("retry_after", 5)
                    write_log(f"SEND_TG_FLOOD | retry_after={retry_after}s")
                    time.sleep(retry_after)
                else:
                    time.sleep(2)
                continue
            write_log(f"SEND_TG_OK | chat={chat_id} thread={thread_id}")
            return data["result"]
        except Exception as e:
            write_log(f"SEND_TG_ERR | attempt={attempt} | {type(e).__name__}: {e}")
            time.sleep(2)
    write_log(f"SEND_TG_GIVE_UP | chat={chat_id} thread={thread_id} | all {3} attempts failed")
    return {}


def send_signals(text: str, reply_to=None) -> dict:
    return send_tg(text, thread_id=TG_SIGNALS_TOPIC, reply_to=reply_to)


def send_sessions(text: str) -> dict:
    try:
        return send_tg(text, thread_id=TG_SESSIONS_TOPIC)
    except Exception as e:
        write_log(f"SESSIONS_SEND_ERR | {e}")
        return send_tg(text)


def send_fg(text: str) -> dict:
    try:
        return send_tg(text, thread_id=TG_SESSIONS_TOPIC)
    except Exception as e:
        write_log(f"FG_SEND_ERR | {e}")
        return send_tg(text)


# ══════════════════════════════════════════════════════════════════════════════
# BYBIT КЛИЕНТ
# ══════════════════════════════════════════════════════════════════════════════
_bybit_session = None


def bybit():
    global _bybit_session
    if not BYBIT_LIB:
        raise RuntimeError("pybit не установлен")
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        raise RuntimeError("BYBIT_API_KEY / BYBIT_API_SECRET не заданы")
    with _bybit_lock:
        if _bybit_session is None:
            _bybit_session = BybitHTTP(
                testnet=TESTNET,
                api_key=BYBIT_API_KEY,
                api_secret=BYBIT_API_SECRET,
            )
            write_log(f"BYBIT | connected | testnet={TESTNET}")
    return _bybit_session


_instrument_cache: dict[str, tuple[dict, float]] = {}
_INSTRUMENT_TTL = 3600


def get_instrument(symbol: str) -> dict:
    now = time.time()
    if symbol in _instrument_cache:
        data, ts = _instrument_cache[symbol]
        if now - ts < _INSTRUMENT_TTL:
            return data
    resp = bybit().get_instruments_info(category="linear", symbol=symbol)
    data = resp["result"]["list"][0]
    _instrument_cache[symbol] = (data, now)
    return data


def round_qty(symbol: str, qty: float) -> str:
    try:
        step = float(get_instrument(symbol)["lotSizeFilter"]["qtyStep"])
        decimals = max(0, round(-math.log10(step)))
        return str(round(math.floor(qty / step) * step, decimals))
    except Exception:
        return str(round(qty, 4))


def round_price(symbol: str, price: float) -> str:
    try:
        tick = float(get_instrument(symbol)["priceFilter"]["tickSize"])
        decimals = max(0, round(-math.log10(tick)))
        return str(round(price, decimals))
    except Exception:
        return str(round(price, 4))


def min_qty(symbol: str) -> float:
    try:
        return float(get_instrument(symbol)["lotSizeFilter"]["minOrderQty"])
    except Exception:
        return 0.001


def bybit_get_price(symbol: str) -> float:
    resp = bybit().get_tickers(category="linear", symbol=symbol)
    return float(resp["result"]["list"][0]["lastPrice"])


def bybit_set_leverage(symbol: str, leverage: int):
    try:
        bybit().set_leverage(
            category="linear", symbol=symbol,
            buyLeverage=str(leverage), sellLeverage=str(leverage),
        )
    except Exception as e:
        write_log(f"BYBIT_LEVERAGE_WARN | {symbol} | {e}")


def bybit_place_market(symbol: str, side: str, qty: float, reduce_only: bool = False) -> dict:
    resp = bybit().place_order(
        category="linear", symbol=symbol,
        side=side, orderType="Market",
        qty=round_qty(symbol, qty),
        reduceOnly=reduce_only, timeInForce="IOC",
    )
    ret_code = resp['retCode']
    ret_msg  = resp.get('retMsg', '')
    order_id = (resp.get('result') or {}).get('orderId', '')
    write_log(f"BYBIT_MARKET | {symbol} {side} qty={qty} ro={reduce_only} | ret={ret_code} msg={ret_msg} order={order_id}")
    if ret_code != 0:
        raise Exception(f"Bybit {ret_code}: {ret_msg}")
    return resp


def bybit_live_position_qty(symbol: str, direction: str) -> float:
    """Read actual Bybit position size after market fill. direction: BUY/SELL."""
    try:
        resp = bybit().get_positions(category="linear", symbol=symbol)
        rows = ((resp.get("result") or {}).get("list") or [])
        want_side = "Buy" if str(direction).upper() == "BUY" else "Sell"
        for row in rows:
            if str(row.get("side") or "") != want_side:
                continue
            size = abs(float(row.get("size") or 0))
            if size > 0:
                return size
    except Exception as e:
        write_log(f"BYBIT_LIVE_QTY_WARN | {symbol} {direction} | {e}")
    return 0.0


def bybit_place_stop(symbol: str, side: str, qty: float,
                     trigger_price: float, reduce_only: bool = True) -> dict:
    # triggerDirection: 1 = price rises above triggerPrice (SHORT SL → side=Buy)
    #                   2 = price falls below triggerPrice  (LONG  SL → side=Sell)
    trigger_direction = 1 if side.lower() == "buy" else 2
    resp = bybit().place_order(
        category="linear", symbol=symbol,
        side=side, orderType="Market",
        qty=round_qty(symbol, qty),
        triggerPrice=round_price(symbol, trigger_price),
        triggerBy="LastPrice", triggerDirection=trigger_direction,
        orderFilter="StopOrder",
        reduceOnly=reduce_only, timeInForce="IOC",
    )
    write_log(f"BYBIT_STOP | {symbol} {side} qty={qty} trigger={trigger_price} dir={trigger_direction} | ret={resp['retCode']}")
    return resp


def bybit_cancel_all(symbol: str):
    try:
        bybit().cancel_all_orders(category="linear", symbol=symbol)
    except Exception as e:
        write_log(f"BYBIT_CANCEL_ERR | {symbol} | {e}")


def get_bybit_balance() -> str:
    try:
        resp   = bybit().get_wallet_balance(accountType="UNIFIED")
        equity = resp["result"]["list"][0]["totalEquity"]
        avail  = resp["result"]["list"][0]["totalAvailableBalance"]
        mode   = "🧪 TESTNET" if TESTNET else "🔴 LIVE"
        return (f"💰 <b>Bybit</b> {mode}\n"
                f"Equity: <b>{float(equity):.2f} USDT</b>\n"
                f"Доступно: <b>{float(avail):.2f} USDT</b>")
    except Exception as e:
        return f"❌ Ошибка Bybit: {e}"


# ══════════════════════════════════════════════════════════════════════════════
# BINGX КЛИЕНТ
# ══════════════════════════════════════════════════════════════════════════════
BINGX_BASE = "https://open-api-vst.bingx.com" if BINGX_DEMO else "https://open-api.bingx.com"


def _bingx_to_symbol(ticker: str) -> str:
    """BTCUSDT → BTC-USDT"""
    t = ticker.upper().replace(".P", "")
    if t.endswith("USDT") and "-" not in t:
        return t[:-4] + "-USDT"
    return t


def _bingx_sign(params: dict) -> str:
    # BingX требует строку без сортировки, в том порядке как параметры добавлялись
    # Но поскольку dict в Python 3.7+ сохраняет порядок, используем items()
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return _hmac.new(
        BINGX_API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _bingx_req(method: str, path: str, params: dict | None = None) -> dict:
    if params is None:
        params = {}
    params["timestamp"] = str(int(time.time() * 1000))
    # Строим строку запроса в том же порядке что и подпись
    query = "&".join(f"{k}={params[k]}" for k in params)
    sig   = _hmac.new(
        BINGX_API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    url     = BINGX_BASE + path + "?" + query + "&signature=" + sig
    headers = {"X-BX-APIKEY": BINGX_API_KEY, "X-SOURCE-KEY": "BX-AI-SKILL"}
    if method == "GET":
        resp = requests.get(url, headers=headers, timeout=10)
    elif method == "POST":
        resp = requests.post(url, headers=headers, timeout=10)
    elif method == "DELETE":
        resp = requests.delete(url, headers=headers, timeout=10)
    else:
        raise ValueError(f"Unknown method: {method}")
    data = resp.json()
    code = data.get("code", 0)
    if code != 0:
        write_log(f"BINGX_API_ERR | {method} {path} | code={code} msg={data.get('msg','')} http={resp.status_code}")
        raise Exception(f"BingX error {code}: {data.get('msg', '')}")
    return data


def bingx_get_price(ticker: str) -> float:
    sym  = _bingx_to_symbol(ticker)
    data = _bingx_req("GET", "/openApi/swap/v2/quote/ticker", {"symbol": sym})
    return float(data["data"]["lastPrice"])


def bingx_set_leverage(ticker: str, leverage: int):
    sym = _bingx_to_symbol(ticker)
    for side in ("LONG", "SHORT"):
        try:
            _bingx_req("POST", "/openApi/swap/v2/trade/leverage",
                       {"symbol": sym, "side": side, "leverage": str(leverage)})
        except Exception as e:
            write_log(f"BINGX_LEVERAGE_WARN | {ticker} {side} | {e}")


def bingx_round_qty(qty: float) -> float:
    return round(math.floor(qty * 1000) / 1000, 4)


def bingx_place_market(ticker: str, side: str, qty: float,
                       reduce_only: bool = False) -> dict:
    """side: 'Buy' / 'Sell' (как Bybit, конвертируем внутри)."""
    sym      = _bingx_to_symbol(ticker)
    bx_side  = side.upper()
    pos_side = ("LONG" if bx_side == "BUY" else "SHORT") if not reduce_only \
               else ("SHORT" if bx_side == "BUY" else "LONG")
    data = _bingx_req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": sym, "side": bx_side, "positionSide": pos_side,
        "type": "MARKET", "quantity": str(bingx_round_qty(qty)),
    })
    write_log(f"BINGX_MARKET | {ticker} {side} pos={pos_side} qty={qty} ro={reduce_only}")
    return data


def bingx_live_position_qty(ticker: str, direction: str) -> float:
    """Read actual BingX position size after market fill. direction: BUY/SELL."""
    try:
        sym = _bingx_to_symbol(ticker)
        want_pos = "LONG" if str(direction).upper() == "BUY" else "SHORT"
        data = _bingx_req("GET", "/openApi/swap/v2/user/positions", {})
        rows = data.get("data") or []
        if isinstance(rows, dict):
            rows = rows.get("positions") or rows.get("list") or []
        for row in rows:
            if str(row.get("symbol") or "").upper() != sym.upper():
                continue
            if str(row.get("positionSide") or "").upper() != want_pos:
                continue
            size = abs(float(row.get("positionAmt") or row.get("availableAmt") or 0))
            if size > 0:
                return size
    except Exception as e:
        write_log(f"BINGX_LIVE_QTY_WARN | {ticker} {direction} | {e}")
    return 0.0


def bingx_place_stop(ticker: str, side: str, qty: float,
                     trigger_price: float, reduce_only: bool = True) -> dict:
    sym      = _bingx_to_symbol(ticker)
    bx_side  = side.upper()
    pos_side = ("LONG" if bx_side == "BUY" else "SHORT") if not reduce_only \
               else ("SHORT" if bx_side == "BUY" else "LONG")
    # BingX fills may be slightly less than ordered qty (exchange rounding).
    # Floor to 1 decimal place for SL to ensure qty ≤ actual filled position.
    safe_qty = math.floor(qty * 10) / 10
    if safe_qty <= 0:
        safe_qty = bingx_round_qty(qty)
    data = _bingx_req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": sym, "side": bx_side, "positionSide": pos_side,
        "type": "STOP_MARKET", "quantity": str(bingx_round_qty(safe_qty)),
        "stopPrice": str(round(trigger_price, 8)), "workingType": "CONTRACT_PRICE",
    })
    write_log(f"BINGX_STOP | {ticker} {side} pos={pos_side} qty={qty}→{safe_qty} trigger={trigger_price}")
    return data


def bingx_cancel_all(ticker: str):
    sym = _bingx_to_symbol(ticker)
    try:
        _bingx_req("DELETE", "/openApi/swap/v2/trade/allOpenOrders", {"symbol": sym})
    except Exception as e:
        write_log(f"BINGX_CANCEL_ERR | {ticker} | {e}")


def _bingx_order_id(resp: dict) -> str:
    """Достаёт orderId из ответа BingX устойчиво: data.order.orderId ИЛИ data.orderId."""
    d = resp.get("data", {}) or {}
    o = d.get("order", {}) or {}
    return str(o.get("orderId") or d.get("orderId") or "")


def bingx_cancel_order_by_id(ticker: str, order_id) -> None:
    """Отменяет ОДИН ордер по его orderId. Не трогает остальные."""
    if not order_id or str(order_id) in ("ok", ""):
        return
    sym = _bingx_to_symbol(ticker)
    try:
        _bingx_req("DELETE", "/openApi/swap/v2/trade/order",
                   {"symbol": sym, "orderId": str(order_id)})
    except Exception as e:
        write_log(f"BINGX_CANCEL_ID_WARN | {ticker} id={order_id} | {e}")


def cancel_own_orders(pos: dict) -> None:
    """🔒 БЕЗОПАСНО: отменяет ТОЛЬКО ордера, которые бот сам выставил для ЭТОЙ позиции
    (по сохранённым в Redis orderId: sl_order_id + tp_order_ids). Ручные ордера юзера
    физически не могут попасть под отмену — их id нет в записи позиции.
    Заменяет опасный ex_cancel_all, который сносил ВСЕ ордера по символу."""
    if not pos:
        return
    exchange = pos.get("exchange", "bybit")
    if exchange == "none":
        return
    ticker = pos.get("symbol", "")
    ids = []
    if pos.get("sl_order_id"):
        ids.append(pos["sl_order_id"])
    tp_ids = pos.get("tp_order_ids") or {}
    if isinstance(tp_ids, dict):
        ids.extend(v for v in tp_ids.values() if v)
    for oid in ids:
        if str(oid) in ("ok", ""):
            continue  # id не сохранён (старый парсинг) — пропускаем, чужое НЕ трогаем
        if exchange == "bingx":
            bingx_cancel_order_by_id(ticker, oid)
        else:
            try:
                bybit().cancel_order(category="linear", symbol=ticker, orderId=str(oid))
            except Exception as e:
                write_log(f"BYBIT_CANCEL_ID_WARN | {ticker} id={oid} | {e}")


def cancel_sl_order(pos: dict) -> None:
    """Cancel only the bot-owned SL.  TP orders must survive an SL move."""
    if not pos or pos.get("exchange", "bybit") == "none":
        return
    order_id = pos.get("sl_order_id")
    if not order_id or str(order_id) in ("ok", ""):
        return
    ticker = pos.get("symbol", "")
    exchange = pos.get("exchange", "bybit")
    if exchange == "bingx":
        bingx_cancel_order_by_id(ticker, order_id)
    else:
        try:
            bybit().cancel_order(category="linear", symbol=ticker, orderId=str(order_id))
        except Exception as e:
            write_log(f"BYBIT_CANCEL_SL_WARN | {ticker} id={order_id} | {e}")

def get_bingx_balance() -> str:
    try:
        if BINGX_DEMO:
            data = _bingx_req("GET", "/openApi/swap/v2/user/balance")
            # balance — это список, берём первый элемент
            balance_data = data.get("data", {}).get("balance", [])
            if isinstance(balance_data, list):
                d = balance_data[0] if balance_data else {}
            else:
                d = balance_data
            equity = d.get("equity", "?")
            avail  = d.get("availableMargin", "?")
        else:
            data     = _bingx_req("GET", "/openApi/account/v3/balance")
            balances = data.get("data", {}).get("balance", {})
            equity   = balances.get("equity", "?")
            avail    = balances.get("availableMargin", "?")

        mode = "🧪 DEMO" if BINGX_DEMO else "🔴 LIVE"
        return (f"💰 <b>BingX</b> {mode}\n"
                f"Equity: <b>{float(equity):.2f} USDT</b>\n"
                f"Доступно: <b>{float(avail):.2f} USDT</b>")
    except Exception as e:
        return f"❌ Ошибка BingX: {e}"


def _bingx_balance_data() -> dict:
    """Read-only normalized balance payload for audit/equity snapshots."""
    paths = (["/openApi/swap/v2/user/balance", "/openApi/swap/v3/user/balance"]
             if BINGX_DEMO else
             ["/openApi/swap/v3/user/balance", "/openApi/swap/v2/user/balance"])
    last_error = None
    for path in paths:
        try:
            raw = _bingx_req("GET", path, {})
            balance = (raw.get("data") or {}).get("balance", {})
            if isinstance(balance, list):
                balance = balance[0] if balance else {}
            return {"endpoint": path, **(balance if isinstance(balance, dict) else {})}
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"BingX balance unavailable: {last_error}")


def _snapshot_equity() -> dict | None:
    if not BINGX_AVAILABLE:
        return None
    try:
        balance = _bingx_balance_data()
        row = {
            "time": int(time.time()),
            "time_msk": _msk().isoformat(),
            "exchange": "bingx",
            "demo": BINGX_DEMO,
            "equity": balance.get("equity"),
            "available_margin": balance.get("availableMargin") or balance.get("availableMarginV2"),
            "unrealized_profit": balance.get("unrealizedProfit"),
            "used_margin": balance.get("usedMargin") or balance.get("usedMarginV2"),
        }
        history = load_json(EQUITY_HISTORY_FILE, [])
        history.append(row)
        # 90 days of hourly observations with a small safety margin.
        save_json(EQUITY_HISTORY_FILE, history[-2200:])
        return row
    except Exception as exc:
        write_log(f"EQUITY_SNAPSHOT_ERR | {exc}")
        return None


def _bingx_audit_export(days: int = 30, limit: int = 1000) -> dict:
    """Read-only exchange ground truth: equity, fills and fee/funding ledger."""
    days = max(1, min(int(days), 90))
    limit = max(1, min(int(limit), 1000))
    now_ms = int(time.time() * 1000)
    params = {"startTime": now_ms - days * 86400000, "endTime": now_ms, "limit": limit}
    history_all = load_history()
    closed_all = load_closed_trades()
    sent_flags = load_json(SENT_FLAGS_FILE, {})
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            logs_tail = f.readlines()[-min(limit, 1000):]
    except Exception as exc:
        logs_tail = []
    result = {
        "generated_at": int(time.time()),
        "range_days": days,
        "limit": limit,
        "audit_contract": {
            "strategy_version": STRATEGY_VERSION,
            "schema_version": SCHEMA_VERSION,
            "tp_contract": TP_CONTRACT,
            "read_only": True,
        },
        "bot_state": {
            "active_trades": load_trades(),
            "positions": load_positions(),
            "history_records": len(history_all),
            "history_tail": history_all[-limit:],
            "closed_trades_count": len(closed_all) if isinstance(closed_all, dict) else 0,
            "closed_trades_tail": dict(list(closed_all.items())[-limit:]) if isinstance(closed_all, dict) else closed_all,
            "sent_flags_count": len(sent_flags) if isinstance(sent_flags, dict) else 0,
            "stats": load_stats(),
        },
        "logs_tail": logs_tail,
        "equity_history": load_json(EQUITY_HISTORY_FILE, []),
        "balance": None,
        "fills": [],
        "income": [],
        "errors": {},
    }
    try:
        result["balance"] = _bingx_balance_data()
    except Exception as exc:
        result["errors"]["balance"] = str(exc)
    try:
        data = _bingx_req("GET", "/openApi/swap/v2/trade/allFillOrders", dict(params))
        result["fills"] = (data.get("data") if isinstance(data, dict) else data) or []
    except Exception as exc:
        result["errors"]["fills"] = str(exc)
    try:
        data = _bingx_req("GET", "/openApi/swap/v2/user/income", dict(params))
        result["income"] = (data.get("data") if isinstance(data, dict) else data) or []
    except Exception as exc:
        result["errors"]["income"] = str(exc)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# EXCHANGE ADAPTER
# ══════════════════════════════════════════════════════════════════════════════
def ex_get_price(ticker: str, exchange: str) -> float:
    return bingx_get_price(ticker) if exchange == "bingx" else bybit_get_price(ticker)


def ex_set_leverage(ticker: str, leverage: int, exchange: str):
    if exchange == "bingx": bingx_set_leverage(ticker, leverage)
    else:                   bybit_set_leverage(ticker, leverage)


def ex_place_market(ticker: str, side: str, qty: float,
                    reduce_only: bool, exchange: str) -> dict:
    if exchange == "bingx": return bingx_place_market(ticker, side, qty, reduce_only)
    return bybit_place_market(ticker, side, qty, reduce_only)


def ex_live_position_qty(ticker: str, direction: str, exchange: str) -> float:
    if exchange == "bingx":
        return bingx_live_position_qty(ticker, direction)
    if exchange == "bybit":
        return bybit_live_position_qty(ticker, direction)
    return 0.0


def ex_place_stop(ticker: str, side: str, qty: float,
                  trigger_price: float, exchange: str) -> dict:
    if exchange == "bingx": return bingx_place_stop(ticker, side, qty, trigger_price)
    return bybit_place_stop(ticker, side, qty, trigger_price)


def ex_cancel_all(ticker: str, exchange: str):
    if exchange == "bingx": bingx_cancel_all(ticker)
    else:                   bybit_cancel_all(ticker)


def ex_calc_qty(ticker: str, size_usdt: float, leverage: int,
                price: float, exchange: str) -> float:
    qty = (size_usdt * leverage) / price
    if exchange == "bybit":
        return max(float(round_qty(ticker, qty)), min_qty(ticker))
    return max(bingx_round_qty(qty), 0.001)


def ex_min_qty(ticker: str, exchange: str) -> float:
    return min_qty(ticker) if exchange == "bybit" else 0.001


# ══════════════════════════════════════════════════════════════════════════════
# TP-ОРДЕРА НА БИРЖЕ
# ══════════════════════════════════════════════════════════════════════════════
def bybit_place_tp_limit(symbol: str, side: str, qty: float, price: float) -> dict:
    """Лимитный TP-ордер на Bybit (reduceOnly, GTC)."""
    resp = bybit().place_order(
        category="linear", symbol=symbol,
        side=side, orderType="Limit",
        price=round_price(symbol, price),
        qty=round_qty(symbol, qty),
        reduceOnly=True, timeInForce="GTC",
    )
    write_log(f"BYBIT_TP_LIMIT | {symbol} {side} qty={qty} price={price} | ret={resp['retCode']}")
    return resp


def bingx_place_tp(ticker: str, side: str, qty: float, trigger_price: float) -> dict:
    """TAKE_PROFIT_MARKET TP-ордер на BingX."""
    sym      = _bingx_to_symbol(ticker)
    bx_side  = side.upper()
    pos_side = "SHORT" if bx_side == "BUY" else "LONG"
    data = _bingx_req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": sym, "side": bx_side, "positionSide": pos_side,
        "type": "TAKE_PROFIT_MARKET", "quantity": str(bingx_round_qty(qty)),
        "stopPrice": str(round(trigger_price, 8)), "workingType": "CONTRACT_PRICE",
    })
    write_log(f"BINGX_TP | {ticker} {side} pos={pos_side} qty={qty} trigger={trigger_price}")
    return data


def place_tp_orders(ticker: str, side: str, opp_side: str, total_qty: float,
                    tp_prices: dict, exchange: str) -> dict:
    """
    Выставляет переданные TP1–TP4 как ордера на бирже.
    tp_prices: {1: price, 2: price, ...}
    Возвращает: {1: order_id, 2: order_id, ...}

    Консолидация: если qty TP < min_qty, объём переносится на следующий TP.
    Решает ErrCode 110017 (orderQty will be truncated to zero) на мелких позициях.
    """
    order_ids: dict = {}
    min_q = min_qty(ticker) if exchange == "bybit" else 0.001

    # Строим план с консолидацией мелких долей
    pending_qty = 0.0
    plan: list[tuple[int, float, float]] = []

    for tp_num, price in sorted(tp_prices.items()):
        pct = TP_CLOSE_PCT.get(tp_num, 0.0)
        if pct <= 0 or not price:
            continue
        raw_qty = round(total_qty * pct, 8)
        qty_use = raw_qty + pending_qty

        if exchange == "bybit":
            qty_use = float(round_qty(ticker, qty_use))
        else:
            qty_use = bingx_round_qty(qty_use)

        if qty_use < min_q:
            pending_qty += raw_qty
            write_log(f"TP_CONSOLIDATE | {ticker} TP{tp_num} | qty {qty_use:.4f} < min {min_q} → carry forward")
            continue

        plan.append((tp_num, price, qty_use))
        pending_qty = 0.0

    # Остаточный pending → добавляем к последнему TP
    if pending_qty > 0 and plan:
        last_tp, last_price, last_qty = plan[-1]
        extra = last_qty + pending_qty
        if exchange == "bybit":
            extra = float(round_qty(ticker, extra))
        else:
            extra = bingx_round_qty(extra)
        plan[-1] = (last_tp, last_price, extra)
        write_log(f"TP_TAIL_MERGE | {ticker} | {pending_qty:.4f} merged into TP{last_tp}")

    for tp_num, price, close_qty in plan:
        try:
            if exchange == "bybit":
                resp = bybit_place_tp_limit(ticker, opp_side, close_qty, price)
                if resp.get("retCode") == 0:
                    order_ids[tp_num] = resp["result"].get("orderId", "")
                else:
                    write_log(f"TP_ORDER_FAIL | {ticker} TP{tp_num} | ret={resp.get('retCode')} {resp.get('retMsg','')}")
            elif exchange == "bingx":
                bx_qty = bingx_round_qty(close_qty)
                if bx_qty < 0.001:
                    continue
                resp = bingx_place_tp(ticker, opp_side, bx_qty, price)
                order_ids[tp_num] = _bingx_order_id(resp) or "ok"
        except Exception as e:
            write_log(f"TP_ORDER_ERR | {ticker} TP{tp_num} ({exchange}) | {e}")
        time.sleep(0.15)

    write_log(f"TP_ORDERS_PLACED | {ticker} [{exchange}] | plan={[(n,round(q,4)) for n,_,q in plan]} | orders={order_ids}")
    return order_ids
def parse_price(text: str, *prefixes: str):
    for prefix in prefixes:
        idx = text.find(prefix)
        if idx == -1:
            continue
        substr = text[idx + len(prefix):].strip()
        m = re.search(r"[\d]+\.?[\d]*", substr)
        if m:
            try: return float(m.group())
            except ValueError: pass
    return None


def infer_entry_price(text: str):
    return parse_price(text, "🎯 Вход:", "⚡ Вход:", "💰 Цена:", "Цена:")


def pos_key(ticker: str, direction: str, exchange: str | None = None) -> str:
    base = f"{str(ticker or '').upper().replace('.P', '')}_{str(direction or '').upper()}"
    ex = str(exchange or "").lower().strip()
    return f"{base}_{ex}" if ex in ("bingx", "bybit") else base


def move_sl(pos: dict, new_sl: float) -> str:
    exchange  = pos.get("exchange", "bybit")
    symbol    = pos["symbol"]
    opp_side  = pos["opp_side"]
    remaining = pos.get("remaining_qty", pos["total_qty"])
    # Не трогаем позиции без биржи или с нулевым объёмом
    if exchange == "none" or remaining <= 0:
        return ""
    old_sl_order_id = pos.get("sl_order_id")
    try:
        # Safe replace: сначала ставим новый SL, потом отменяем старый.
        # Если новая заявка не поставилась — старый SL остаётся жить.
        resp = ex_place_stop(symbol, opp_side, remaining, new_sl, exchange)
        new_order_id = ""
        if exchange == "bybit" and resp.get("retCode") == 0:
            new_order_id = resp["result"].get("orderId", "")
        elif exchange == "bingx":
            new_order_id = _bingx_order_id(resp) or "ok"
        if not new_order_id:
            write_log(f"SL_MOVE_ERR | {symbol} ({exchange}) | new stop not confirmed")
            return ""
        if old_sl_order_id and str(old_sl_order_id) not in ("ok", ""):
            old_pos = dict(pos)
            old_pos["sl_order_id"] = old_sl_order_id
            cancel_sl_order(old_pos)  # TP1..TP4 remain active while only SL is replaced.
        return new_order_id
    except Exception as e:
        write_log(f"SL_MOVE_ERR | {symbol} ({exchange}) | {e}")
    return ""


def _truthy(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _float_or_none(v):
    try:
        if v is None or str(v).strip().lower() in ("", "none", "null", "nan"):
            return None
        x = float(v)
        return x if math.isfinite(x) and x > 0 else None
    except Exception:
        return None


def _sl_improves(direction: str, current_sl, new_sl: float) -> bool:
    cur = _float_or_none(current_sl)
    if cur is None:
        return True
    return new_sl > cur if direction == "BUY" else new_sl < cur


def _bot_fallback_sl_after_tp(pos: dict, highest_tp: int) -> tuple[float | None, str, bool, bool]:
    """Bot-side protection when Pine sl_moved alert is missing.

    TP2 fallback: move SL to TP1 with buffer, never worse than entry.
    TP3 fallback: move SL toward TP2 with ATR buffer, never worse than entry.
    """
    direction = str(pos.get("direction") or "").upper()
    entry = _float_or_none(pos.get("entry_price"))
    if direction not in ("BUY", "SELL") or entry is None:
        return None, "", False, False

    try:
        buffer_pct = float(os.environ.get("BE_TP2_BUFFER_PCT", "0.0") or 0.0)
    except Exception:
        buffer_pct = 0.0
    try:
        atr_pct = float(pos.get("atr_pct") or 0.0)
    except Exception:
        atr_pct = 0.0
    atr_abs = entry * atr_pct / 100.0 if atr_pct > 0 else 0.0

    tp1 = _float_or_none(pos.get("tp1_price"))
    tp2 = _float_or_none(pos.get("tp2_price"))

    target = None
    reason = ""
    be_active = False
    trail_active = False

    if highest_tp >= 3 and _truthy(pos.get("config_trail_tp3")):
        base = tp2 or tp1 or entry
        if direction == "BUY":
            target = max(base - atr_abs, entry)
        else:
            target = min(base + atr_abs, entry)
        reason = "BOT_FALLBACK_TP3_TRAIL_BASE"
        trail_active = True
        be_active = True
    elif highest_tp >= 2 and _truthy(pos.get("config_be_tp2")):
        base = tp1 or entry
        if direction == "BUY":
            target = max(base * (1 - buffer_pct / 100.0), entry)
        else:
            target = min(base * (1 + buffer_pct / 100.0), entry)
        reason = "BOT_FALLBACK_TP2_BE"
        be_active = True

    if target is None:
        return None, "", False, False
    target = round(float(target), 8)
    if not _sl_improves(direction, pos.get("sl_price"), target):
        return None, "", False, False
    return target, reason, be_active, trail_active


def _apply_bot_sl_fallback(pos: dict, highest_tp: int) -> tuple[bool, str]:
    new_sl, reason, be_active, trail_active = _bot_fallback_sl_after_tp(pos, highest_tp)
    if not new_sl:
        return False, ""
    exchange = pos.get("exchange", "bybit")
    oid = ""
    if exchange != "none":
        oid = move_sl(pos, new_sl)
        if not oid:
            write_log(f"BOT_SL_FALLBACK_FAIL | {pos.get('symbol')} {pos.get('direction')} | reason={reason} new_sl={new_sl}")
            log_event("bot_sl_fallback_fail", ticker=pos.get("symbol"), direction=pos.get("direction"), trade_id=pos.get("trade_id"), reason=reason, new_sl=new_sl, exchange=exchange)
            return False, ""
    pos["sl_price"] = new_sl
    pos["sl_order_id"] = oid
    pos["sl_moved_count"] = int(pos.get("sl_moved_count") or 0) + 1
    pos["last_sl_move_reason"] = reason
    pos["last_sl_moved_at"] = int(time.time())
    if be_active:
        pos["be_active"] = True
    if trail_active:
        pos["trail_active"] = True
        pos["trail_sl"] = new_sl
    write_log(f"BOT_SL_FALLBACK_APPLIED | {pos.get('symbol')} {pos.get('direction')} | TP{highest_tp} reason={reason} sl={new_sl} oid={oid or '-'}")
    log_event("bot_sl_fallback_applied", ticker=pos.get("symbol"), direction=pos.get("direction"), trade_id=pos.get("trade_id"), highest_tp=highest_tp, reason=reason, new_sl=new_sl, be_active=bool(pos.get("be_active", False)), trail_active=bool(pos.get("trail_active", False)), sl_moved_count=int(pos.get("sl_moved_count") or 0), exchange=exchange, order_id=oid)
    return True, oid


# ══════════════════════════════════════════════════════════════════════════════
# СТАТИСТИКА И ИСТОРИЯ
# ══════════════════════════════════════════════════════════════════════════════
def _trade_instance_id(key: str = "", trade_data: dict | None = None,
                       pos: dict | None = None, payload: dict | None = None) -> str:
    for source in (trade_data, pos):
        if source and source.get("instance_id"):
            return str(source["instance_id"])

    created_at = 0
    for source in (trade_data, pos):
        if source and source.get("created_at"):
            created_at = int(source["created_at"])
            break

    trade_id = ""
    for source in (trade_data, pos, payload or {}):
        if source and source.get("trade_id"):
            trade_id = str(source["trade_id"]).strip()
            break

    ticker = ""
    direction = ""
    timeframe = ""
    exchange = ""
    for source in (trade_data, pos, payload or {}):
        if source:
            ticker = ticker or str(source.get("ticker") or source.get("symbol") or "").strip()
            direction = direction or str(source.get("direction") or "").strip()
            timeframe = timeframe or str(source.get("timeframe") or "").strip()
            exchange = exchange or str(source.get("exchange") or source.get("target_exchange") or "").strip().lower()

    base = trade_id or key or "_".join(x for x in (ticker, direction, timeframe) if x)
    if exchange in ("bingx", "bybit") and f"|{exchange}" not in str(base):
        base = f"{base}|{exchange}"
    if not base:
        base = "trade"
    if not created_at:
        created_at = int(time.time())
    return f"{base}:{created_at}"


def _highest_tp_hit(pos: dict | None) -> int:
    if not pos:
        return 0
    hits = [n for n in range(1, 5) if pos.get(f"tp{n}_hit")]
    return max(hits, default=0)


def _reverse_shadow_record(record: dict) -> dict:
    """Analytics only: estimate opposite-direction result after close. No reverse orders."""
    pnl = record.get("pnl") if isinstance(record.get("pnl"), dict) else {}
    pnl_pct = pnl.get("pnl_pct", pnl.get("pct"))
    try:
        original_pnl = float(pnl_pct)
        reverse_proxy = round(-original_pnl, 6)
    except Exception:
        original_pnl = None
        reverse_proxy = None
    direction = str(record.get("direction") or "").upper()
    reverse_direction = "SELL" if direction == "BUY" else "BUY" if direction == "SELL" else ""
    return {
        "enabled": True,
        "model": "proxy_v1_pnl_inverse",
        "original_direction": direction,
        "reverse_direction": reverse_direction,
        "original_pnl_pct": original_pnl,
        "reverse_pnl_proxy_pct": reverse_proxy,
        "bucket": "|".join(str(record.get(k) or "") for k in ("signal_class", "amd_phase", "timeframe", "direction")),
    }


ENTRY_METADATA_FIELDS = (
    "signal_class", "aggressiveness_mode", "trading_style_input", "trading_style",
    "strategy", "pattern", "family", "session", "mtf_alignment",
    "bayes_probability", "mfe_pct", "mae_pct", "config_mtf_threshold",
    "config_bayes_threshold", "config_be_tp1", "config_be_tp2",
    "config_trail_tp3", "config_cascade_sl", "config_counter_trend",
    "config_strong_sensitivity",
)


def _entry_metadata(payload: dict | None) -> dict:
    """Normalize schema-v2 entry telemetry without overloading entry_mode."""
    payload = payload or {}
    signal_class = str(payload.get("signal_class") or payload.get("entry_mode") or "")
    out = {field: payload.get(field) for field in ENTRY_METADATA_FIELDS if field in payload}
    out["signal_class"] = signal_class
    # Compatibility alias for pre-v154 dashboard clients.
    out["entry_mode"] = signal_class
    out["aggressiveness_mode"] = str(payload.get("aggressiveness_mode") or "")
    return out


def _normalize_tp_flags(pos: dict, highest_tp: int) -> None:
    """A reached TPn implies TP1..TPn; restore flags after missing webhooks."""
    highest_tp = max(0, min(int(highest_tp or 0), 4))
    for n in range(1, highest_tp + 1):
        pos[f"tp{n}_hit"] = True


def update_stats(result: str, pnl_pct: float | None = None):
    """Update legacy bot counters using net P&L, never the partial label."""
    with _state_lock:
        s = load_stats()
        s["total"] = s.get("total", 0) + 1
        if pnl_pct is not None and math.isfinite(float(pnl_pct)):
            net_result = "win" if float(pnl_pct) > 0 else "loss" if float(pnl_pct) < 0 else "breakeven"
        else:
            net_result = result if result in ("win", "loss") else "unscored"
        if net_result == "win":
            s["wins"] = s.get("wins", 0) + 1
        elif net_result == "loss":
            s["losses"] = s.get("losses", 0) + 1
        elif net_result == "breakeven":
            s["breakeven"] = s.get("breakeven", 0) + 1
        else:
            s["unscored"] = s.get("unscored", 0) + 1
        save_stats(s)


def save_trade_to_history(payload: dict, trade_data: dict | None, result: str, tp_num: int,
                          close_reason: str = "", pos: dict | None = None):
    trade_data = trade_data or {}
    pos = pos or {}
    now = int(time.time())
    entry_time = (
        trade_data.get("created_at")
        or pos.get("created_at")
        or now
    )
    trade_key = (
        trade_data.get("trade_key")
        or pos.get("trade_key")
        or build_trade_key(payload)
    )
    instance_id = _trade_instance_id(trade_key, trade_data, pos, payload)
    record = {
        "strategy_version": (
            trade_data.get("strategy_version")
            or pos.get("strategy_version")
            or (payload.get("strategy_version") if not trade_data and not pos else None)
            or "legacy"
        ),
        "schema_version": int(
            trade_data.get("schema_version")
            or pos.get("schema_version")
            or (payload.get("schema_version") if not trade_data and not pos else None)
            or 1
        ),
        "tp_contract": (
            trade_data.get("tp_contract")
            or pos.get("tp_contract")
            or (payload.get("tp_contract") if not trade_data and not pos else None)
            or "legacy"
        ),
        "instance_id": instance_id,
        "trade_key": trade_key,
        "trade_id": (
            str(payload.get("trade_id") or trade_data.get("trade_id") or pos.get("trade_id") or "")
        ),
        "ticker": (
            payload.get("ticker")
            or trade_data.get("ticker")
            or pos.get("symbol")
            or ""
        ),
        "direction": (
            payload.get("direction")
            or trade_data.get("direction")
            or pos.get("direction")
            or ""
        ),
        "timeframe": (
            payload.get("timeframe")
            or trade_data.get("timeframe")
            or pos.get("timeframe")
            or ""
        ),
        "exchange": (
            payload.get("exchange")
            or trade_data.get("exchange")
            or pos.get("exchange")
            or ""
        ),
        "trade_mode": (
            trade_data.get("trade_mode")
            or pos.get("trade_mode")
            or ("telegram_only" if (payload.get("exchange") or pos.get("exchange")) == "none" else "trade")
        ),
        "is_strong": bool(
            payload.get("is_strong", trade_data.get("is_strong", pos.get("is_strong", False)))
        ),
        "entry_mode": (
            trade_data.get("entry_mode") or pos.get("entry_mode") or payload.get("entry_mode") or ""
        ),
        "signal_class": (
            trade_data.get("signal_class") or pos.get("signal_class")
            or payload.get("signal_class") or trade_data.get("entry_mode")
            or pos.get("entry_mode") or payload.get("entry_mode") or ""
        ),
        "aggressiveness_mode": (
            trade_data.get("aggressiveness_mode") or pos.get("aggressiveness_mode")
            or payload.get("aggressiveness_mode") or ""
        ),
        "trading_style_input": trade_data.get("trading_style_input", pos.get("trading_style_input", payload.get("trading_style_input"))),
        "trading_style": trade_data.get("trading_style", pos.get("trading_style", payload.get("trading_style"))),
        "strategy": trade_data.get("strategy", pos.get("strategy", payload.get("strategy"))),
        "pattern": trade_data.get("pattern", pos.get("pattern", payload.get("pattern"))),
        "family": trade_data.get("family", pos.get("family", payload.get("family"))),
        "session": trade_data.get("session", pos.get("session", payload.get("session"))),
        "mtf_alignment": trade_data.get("mtf_alignment", pos.get("mtf_alignment", payload.get("mtf_alignment"))),
        "bayes_probability": trade_data.get("bayes_probability", pos.get("bayes_probability", payload.get("bayes_probability"))),
        # Excursions evolve during the trade, so terminal payload has priority.
        "mfe_pct": payload.get("mfe_pct", pos.get("mfe_pct", trade_data.get("mfe_pct"))),
        "mae_pct": payload.get("mae_pct", pos.get("mae_pct", trade_data.get("mae_pct"))),
        "entry_config": {
            field: trade_data.get(field, pos.get(field, payload.get(field)))
            for field in ENTRY_METADATA_FIELDS if field.startswith("config_")
        },
        "score": trade_data.get("score", pos.get("score", payload.get("score"))),
        "confirmations": trade_data.get(
            "confirmations", pos.get("confirmations", payload.get("confirmations"))
        ),
        "atr_pct": trade_data.get("atr_pct", pos.get("atr_pct", payload.get("atr_pct"))),
        "amd_phase": (
            trade_data.get("amd_phase") or pos.get("amd_phase") or payload.get("amd_phase") or ""
        ),
        "entry_message_id": trade_data.get("message_id") or pos.get("message_id"),
        "signal_message_id": trade_data.get("signal_message_id") or pos.get("signal_message_id"),
        "entry_price": trade_data.get("entry_price") or pos.get("entry_price"),
        "sl_price": pos.get("sl_price", trade_data.get("sl_price")),
        "total_qty": pos.get("total_qty", trade_data.get("total_qty")),
        "remaining_qty": pos.get("remaining_qty", trade_data.get("remaining_qty")),
        "leverage": pos.get("leverage", trade_data.get("leverage")),
        "notional_usdt": pos.get("notional_usdt", trade_data.get("notional_usdt")),
        "margin_usdt": pos.get("margin_usdt", trade_data.get("margin_usdt")),
        # highest_tp_hit = best TP reached during trade lifetime
        # tp_num = the TP that closed the position (4 for full close, 0 for SL)
        "highest_tp_hit": max(tp_num, _highest_tp_hit(pos)),
        "entry_time": entry_time,
        "close_time": now,
        "duration_sec": max(0, now - entry_time),
        "result": result,
        "tp_num": tp_num,
        "close_reason": close_reason or payload.get("event", ""),
        "date_msk": _msk().strftime("%Y-%m-%d"),
        "week_msk": f"{_msk().year}-W{_msk().isocalendar()[1]:02d}",
        "pnl": payload.get("_bot_pnl", {}),
        # Explicit execution state.  TP1 alone does not prove that SL moved.
        "trail_active": bool(pos.get("trail_active", False)),
        "be_active": bool(pos.get("be_active", False)),
    }
    record["reverse_shadow"] = _reverse_shadow_record(record)
    with _state_lock:
        history = load_history()
        history.append(record)
        if len(history) > 5000:
            history = history[-5000:]
        save_history(history)


def _trade_already_closed(instance_id: str) -> bool:
    if not instance_id:
        return False
    with _state_lock:
        return instance_id in load_closed_trades()


def _mark_trade_closed(instance_id: str, result: str, close_reason: str) -> bool:
    if not instance_id:
        return True
    with _state_lock:
        closed = load_closed_trades()
        if instance_id in closed:
            return False
        closed[instance_id] = {
            "closed_at": int(time.time()),
            "result": result,
            "close_reason": close_reason,
        }
        if len(closed) > 5000:
            closed = dict(list(closed.items())[-5000:])
        save_closed_trades(closed)
        return True


def _wr_icon(wr: float) -> str:
    if wr >= 70: return "🟢"
    if wr >= 50: return "🟡"
    return "🔴"


def _build_report(trades: list, title: str, show_last: int = 5,
                  show_top_tickers: bool = False, date_range: str = "") -> str:
    wins     = [r for r in trades if r["result"] == "win"]
    losses   = [r for r in trades if r["result"] == "loss"]
    partials = [r for r in trades if r["result"] == "partial"]
    manuals  = [r for r in trades if r["result"] == "manual"]
    total    = len(trades)
    pnl_values = [float(r["pnl"]["pnl_pct"]) for r in trades
                  if isinstance(r.get("pnl"), dict) and r["pnl"].get("pnl_pct") is not None]
    wr = calc_winrate(sum(1 for p in pnl_values if p > 0.0), len(pnl_values))
    if not trades:
        return f"{title}\n\n<i>Нет данных за период.</i>"
    avg_dur = int(sum(r.get("duration_sec", 0) for r in trades) / total) if total else 0

    # Средний P&L из сохранённых расчётов бота; zero is valid breakeven.
    avg_pnl = round(sum(pnl_values) / len(pnl_values), 2) if pnl_values else None

    # ── Разбивка по TP: считаем highest_tp_hit для всех закрытых сделок ─
    # Reached targets are independent from the net-P&L classification.
    tp_counts: dict = {}      # {tp_num: count}
    tp_pnl_sums: dict = {}    # {tp_num: [tp_pnl_pct, ...]}  ← BUG #6 FIX: tp_pnl_pct не pnl_pct
    for r in trades:
        # highest_tp_hit — максимальный достигнутый TP в четырёхуровневом контракте.
        n = int(r.get("highest_tp_hit") or r.get("tp_num") or 0)
        if n:
            tp_counts[n] = tp_counts.get(n, 0) + 1
        # BUG #6 FIX: используем tp_pnl_pct (только доход от TP-частей, без SL-хвоста)
        pnl_data = r.get("pnl", {})
        tp_part = pnl_data.get("tp_pnl_pct", None) if pnl_data else None
        if tp_part is not None and tp_part != 0.0 and n:
            if n not in tp_pnl_sums:
                tp_pnl_sums[n] = []
            tp_pnl_sums[n].append(tp_part)
    # Also count pure SL losses (no TP hit) as TP0
    sl_only = [r for r in losses if not (r.get("highest_tp_hit") or r.get("tp_num"))]

    # ── Заголовок ────────────────────────────────────────────────────
    text = f"{title}\n"
    if date_range:
        text += f"<i>{date_range}</i>\n"
    text += "\n"

    # ── Основные цифры ───────────────────────────────────────────────
    text += f"📊 Win Rate: <b>{_wr_icon(wr)} {wr}%</b>\n"
    pure_sl_count = sum(1 for r in trades if r.get("close_reason") == "sl_hit" and not int(r.get("highest_tp_hit") or 0))
    text += (f"🏆 Full TP: {len(wins)}   🔶 Net+ Partial: {len(partials)}   ❌ Net Loss: {len(losses)}   Pure SL: {pure_sl_count}")
    if manuals:
        text += f"   🧯 Manual: {len(manuals)}"
    text += f"\n📈 Всего закрыто: <b>{total}</b>\n"
    text += f"⏱ Ср. время: {fmt_duration(avg_dur)}\n"
    if avg_pnl is not None:
        _pnl_sign = "+" if avg_pnl >= 0 else ""
        text += f"💰 Ср. П&Л: <b>{_pnl_sign}{avg_pnl}%</b> (с плечом)\n"

    # ── Разбивка по TP — количество + средний P&L ───────────────────
    if tp_counts:
        text += "\n<b>📊 Разбивка по TP (включая Partial):</b>\n"
        for n in sorted(tp_counts.keys()):
            cnt = tp_counts[n]
            bar = "█" * min(cnt, 20)
            pnl_list = tp_pnl_sums.get(n, [])
            if pnl_list:
                avg_tp_pnl = round(sum(pnl_list) / len(pnl_list), 1)
                _s = "+" if avg_tp_pnl >= 0 else ""
                pnl_str = f"  <i>avg {_s}{avg_tp_pnl}%</i>"
            else:
                pnl_str = "  <i>avg n/a</i>"
            text += f"  TP{n}: {cnt}x  <code>{bar}</code>{pnl_str}\n"
    # ── SL разбивка по типам ─────────────────────────────────────────
    def _avg_pnl_sl(recs):
        vals = [r["pnl"]["pnl_pct"] for r in recs
                if r.get("pnl") and r["pnl"].get("pnl_pct") != 0.0]
        if not vals: return ""
        avg = round(sum(vals)/len(vals), 1)
        # Note: positive SL avg = BE/Trail stopped out in profit (correct!)
        return f"  <i>avg {'+'if avg>=0 else ''}{avg}%</i>"

    # Classify SL records: new records have trail_active/be_active fields
    # Old records (no fields) → separate "архив" bucket, cannot classify
    all_sl_recs = [r for r in trades if r.get("close_reason") == "sl_hit"
                   or r.get("result") in ("loss","partial")]
    # New records: have trail_active or be_active fields stored
    _sl_new    = [r for r in all_sl_recs if "trail_active" in r or "be_active" in r]
    _sl_pure   = [r for r in _sl_new
                  if not r.get("trail_active") and not r.get("be_active")]
    _sl_be     = [r for r in _sl_new
                  if r.get("be_active") and not r.get("trail_active")]
    _sl_trail  = [r for r in _sl_new if r.get("trail_active")]
    # Old records: cannot classify (mix of pure SL + BE + Trail pre-update)
    _sl_legacy = [r for r in all_sl_recs
                  if "trail_active" not in r and "be_active" not in r]

    has_data = bool(_sl_pure or _sl_be or _sl_trail or _sl_legacy)
    if has_data:
        text += "\n<b>Разбивка по SL:</b>\n"
        if _sl_pure:
            text += f"  ❌ Чистый SL: {len(_sl_pure)}x{_avg_pnl_sl(_sl_pure)}\n"
        if _sl_be:
            text += f"  🔒 BE-стоп: {len(_sl_be)}x{_avg_pnl_sl(_sl_be)}\n"
        if _sl_trail:
            text += f"  📈 Trail-стоп: {len(_sl_trail)}x{_avg_pnl_sl(_sl_trail)}\n"
        if _sl_legacy:
            leg_pnl = [r["pnl"]["pnl_pct"] for r in _sl_legacy
                       if r.get("pnl") and r["pnl"].get("pnl_pct") != 0.0]
            if leg_pnl:
                avg_leg = round(sum(leg_pnl)/len(leg_pnl), 1)
                _s = "+" if avg_leg >= 0 else ""
                text += (f"  📦 Архив (до клас-ции): {len(_sl_legacy)}x"
                         f"  <i>avg {_s}{avg_leg}% (вкл. BE/Trail)</i>\n")
            else:
                text += f"  📦 Архив (до клас-ции): {len(_sl_legacy)}x  <i>avg n/a</i>\n"

    # ── Топ тикеры (для недельного/месячного) ────────────────────────
    if show_top_tickers and total > 0:
        by_tk: dict = {}
        for r in trades:
            tk = r.get("ticker", "?")
            by_tk[tk] = by_tk.get(tk, 0) + 1
        top = sorted(by_tk.items(), key=lambda x: x[1], reverse=True)[:5]
        # Активных дней
        days = len({r.get("date_msk", "") for r in trades if r.get("date_msk")})
        text += f"\n📅 Активных дней: <b>{days}</b>\n"
        text += "\n<b>Топ тикеры:</b>\n"
        for tk, cnt in top:
            text += f"  #{tk.replace('.P','')}: {cnt}\n"

    # ── Последние сделки ─────────────────────────────────────────────
    if show_last and total > 0:
        recent = sorted(trades, key=lambda r: r.get("close_time", 0), reverse=True)[:show_last]
        text += "\n<b>Последние сделки:</b>\n"
        for r in recent:
            tk   = r.get("ticker", "?").replace(".P", "")
            dr   = r.get("direction", "")
            res  = r.get("result", "")
            tp_n = int(r.get("tp_num") or 0)
            dur  = fmt_duration(int(r.get("duration_sec", 0)))
            if res == "win":
                icon = "✅"
                label = f"TP{tp_n}" if tp_n else "TP"
            elif res == "partial":
                icon = "🔶"
                label = f"Partial TP{tp_n}" if tp_n else "Partial"
            elif res == "loss":
                icon = "❌"
                label = "SL"
            else:
                icon = "🧯"
                label = "Manual"
            text += f"{icon} #{tk} {dr} → {label} ({dur})\n"

    return text.rstrip()


# ══════════════════════════════════════════════════════════════════════════════
# АКТИВНЫЕ СДЕЛКИ
# ══════════════════════════════════════════════════════════════════════════════
def build_trade_key(payload: dict) -> str:
    tid = payload.get("trade_id")
    if tid and tid != "null" and str(tid).strip():
        return str(tid).strip()
    ticker    = payload.get("ticker", "").strip()
    direction = payload.get("direction", "").strip()
    tf        = payload.get("timeframe", "").strip()
    return f"{ticker}_{direction}_{tf}" if ticker else ""


def find_trade_entry(key: str = "", trade_id: str = "", ticker: str = "",
                     direction: str = "") -> tuple[str | None, dict | None]:
    ticker = ticker.upper().replace(".P", "") if ticker else ""
    direction = direction.upper() if direction else ""
    trade_id = str(trade_id or "").strip()
    with _state_lock:
        trades = load_trades()
        if key and key in trades:
            return key, trades[key]
        if trade_id and trade_id in trades:
            return trade_id, trades[trade_id]
        if key and "_" in key:
            parts = key.split("_")
            if len(parts) >= 2:
                ticker = ticker or parts[0]
                direction = direction or parts[1]
        if ticker and direction:
            for stored_key, trade in trades.items():
                if trade.get("ticker", "") == ticker and trade.get("direction", "") == direction:
                    return stored_key, trade
    return None, None


def put_trade(key: str, data: dict):
    with _state_lock:
        t = load_trades()
        t[key] = data
        save_trades(t)


def get_trade(key: str) -> dict | None:
    _, trade = find_trade_entry(key=key)
    return trade


def touch_trade(key: str | None, **fields):
    if not key:
        return
    with _state_lock:
        trades = load_trades()
        trade = trades.get(key)
        if not trade:
            return
        trade.update(fields)
        trades[key] = trade
        save_trades(trades)


def remove_trade(key: str):
    if not key:
        return
    with _state_lock:
        t = load_trades()
        if key in t:
            del t[key]
            save_trades(t)


def _remove_trades_for_phantom_positions(phantom_positions: list[dict], reason: str = "sync_phantom") -> int:
    """Remove active trade records whose exchange position was proven absent.

    Sync previously deleted only positions, leaving stale trades like APEX/XMR.
    Match by trade_key/trade_id first, then ticker+direction fallback.
    """
    if not phantom_positions:
        return 0
    with _state_lock:
        trades = load_trades()
        remove_keys = set()
        for pos in phantom_positions:
            ticker = str(pos.get("symbol") or pos.get("ticker") or "").upper().replace(".P", "")
            direction = str(pos.get("direction") or "").upper()
            trade_key = str(pos.get("trade_key") or "").strip()
            trade_id = str(pos.get("trade_id") or "").strip()
            if trade_key and trade_key in trades:
                remove_keys.add(trade_key)
            if trade_id and trade_id in trades:
                remove_keys.add(trade_id)
            if ticker and direction:
                for k, t in trades.items():
                    if not isinstance(t, dict):
                        continue
                    tk = str(t.get("ticker") or t.get("symbol") or "").upper().replace(".P", "")
                    dr = str(t.get("direction") or "").upper()
                    if tk == ticker and dr == direction:
                        remove_keys.add(k)
        for k in remove_keys:
            trades.pop(k, None)
        if remove_keys:
            save_trades(trades)
            write_log(f"SYNC_TRADE_CLEANUP | removed {len(remove_keys)} trades after {reason}: {sorted(remove_keys)[:10]}")
        return len(remove_keys)


# ✅ IMPROVEMENT #2: Redis dedup — 60-сек TTL на входящий алерт
def _alert_dedup_check(trade_id: str, event: str, ticker: str, timeframe: str) -> bool:
    """Возвращает True если алерт — дубль (нужно игнорировать).
    Ключ: alert:dedup:{ticker}:{timeframe}:{event} с TTL 60 сек."""
    r = _get_redis()
    if not r:
        return False  # без Redis дедупликация недоступна
    # Для entry — более строгий ключ (включаем trade_id если есть)
    tid_part = trade_id[:16] if trade_id and trade_id not in ("", "null") else "notid"
    key = f"alert:dedup:{ticker}:{timeframe}:{event}:{tid_part}"
    try:
        result = r.set(key, "1", nx=True, ex=60)
        if result is None:
            write_log(f"DEDUP_BLOCK | {key} — duplicate alert within 60s, ignoring")
            return True   # уже было — блокируем
        return False      # первый раз — пропускаем
    except Exception as e:
        write_log(f"DEDUP_ERR | {e}")
        return False

def dedup_entry(ticker: str, direction: str, current_key: str = "", exchange: str | None = None):
    """Удаляет старые записи по тому же ticker/direction/exchange. Не трогает вторую биржу."""
    now = int(time.time())
    ex_norm = str(exchange or "").lower().strip()
    with _state_lock:
        trades = load_trades()
        keys_to_remove = []
        for k, v in trades.items():
            if not isinstance(v, dict):
                continue
            if v.get("ticker", "") != ticker or v.get("direction", "") != direction or k == current_key:
                continue
            if ex_norm in ("bingx", "bybit") and str(v.get("exchange") or "").lower() != ex_norm:
                continue
            if now - int(v.get("created_at", 0)) > 60:
                keys_to_remove.append(k)
        for k in keys_to_remove:
            del trades[k]
        if keys_to_remove:
            save_trades(trades)
            write_log(f"DEDUP | removed {len(keys_to_remove)} old keys for {ticker}_{direction}_{ex_norm or 'legacy'}")


def cleanup_old_trades() -> int:
    """Удаляет: (1) сделки старше 7 дней, (2) сделки без соответствующей позиции старше 2 часов."""
    with _state_lock:
        trades = load_trades()
        positions = load_positions()
        now = int(time.time())
        cutoff_old   = now - 7 * 86400   # 7 дней
        cutoff_orphan = now - 2 * 3600   # 2 часа

        pos_keys_set = set()
        for pkey, pos in positions.items():
            # Позиция хранит trade_key
            tk = pos.get("trade_key", "")
            if tk:
                pos_keys_set.add(tk)

        live_pairs = {
            (str(pos.get("symbol") or pos.get("ticker") or "").upper().replace(".P", ""),
             str(pos.get("direction") or "").upper())
            for pos in positions.values() if isinstance(pos, dict)
        }
        cutoff_exchange_orphan = now - 15 * 60

        removed = []
        for k, v in list(trades.items()):
            created = int(v.get("created_at") or 0)
            # Never age out a trade that still owns a tracked position.
            if k in pos_keys_set:
                continue
            if created < cutoff_old:
                removed.append(k)
                continue
            ticker = str(v.get("ticker") or v.get("symbol") or "").upper().replace(".P", "")
            direction = str(v.get("direction") or "").upper()
            exchange = str(v.get("exchange") or "none").lower()
            # Exchange orphan: live trade record exists, but matching active position is gone.
            # Keep short grace window to avoid entry-save race, then clean.
            if exchange != "none" and ticker and direction and (ticker, direction) not in live_pairs and created < cutoff_exchange_orphan:
                removed.append(k)
                continue
            # Orphan: trade без активной позиции, висит > 2ч
            if k not in pos_keys_set and created < cutoff_orphan:
                removed.append(k)

        for k in removed:
            del trades[k]
        if removed:
            save_trades(trades)
            write_log(f"CLEANUP | removed {len(removed)} stale/orphaned trades: {removed[:10]}")
        return len(removed)




def finalize_trade(payload: dict, trade_key: str | None, trade_data: dict | None,
                   pos: dict | None, result: str, tp_num: int,
                   close_reason: str) -> bool:
    trade_data = trade_data or {}
    pos = pos or {}
    actual_key = trade_key or trade_data.get("trade_key") or pos.get("trade_key") or build_trade_key(payload)
    instance_id = _trade_instance_id(actual_key, trade_data, pos, payload)
    if not _mark_trade_closed(instance_id, result, close_reason):
        write_log(f"FINALIZE_SKIP | {instance_id} already closed")
        return False
    if result in ("win", "loss", "partial"):
        pnl_value = (payload.get("_bot_pnl") or {}).get("pnl_pct")
        update_stats(result, pnl_value)
    save_trade_to_history(payload, trade_data, result, tp_num, close_reason=close_reason, pos=pos)
    if actual_key:
        remove_trade(actual_key)
    return True


# ══════════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ СИГНАЛОВ
# ══════════════════════════════════════════════════════════════════════════════
def handle_entry(payload: dict):
    ticker = payload.get("ticker", "").upper().replace(".P", "")
    direction = payload.get("direction", "").upper()
    text = payload.get("text", "").strip()
    trade_id = str(payload.get("trade_id") or "")
    timeframe = payload.get("timeframe", "")
    target_exchange = str(payload.get("target_exchange") or "").lower().strip()
    exchange = target_exchange if target_exchange in ("bingx", "bybit") else get_exchange_for(ticker)
    key = build_trade_key(payload)
    if key and exchange != "none":
        key = f"{key}|{exchange}"
    pkey = pos_key(ticker, direction, exchange)
    trade_mode = "trade" if exchange != "none" else "telegram_only"

    with _pos_lock:
        _positions_now = load_positions()
        existing_pos = _positions_now.get(pkey) or _positions_now.get(pos_key(ticker, direction))
    if existing_pos:
        write_log(f"ENTRY_SKIP | {ticker} {direction} | DUPLICATE — active position already exists for {pkey}")
        # BUG 2 FIX: still forward the signal text to Telegram even on duplicate
        # (trader needs to see every signal; exchange execution is skipped)
        _dup_text = text or f"⚠️ Дубль сигнала #{ticker} {direction} — позиция уже открыта"
        send_signals(_dup_text)
        return

    # ✅ IMPROVEMENT #2: Dedup — блокируем одинаковые алерты за 60 сек
    timeframe_str = str(payload.get("timeframe", "")).strip()
    trade_id_str  = str(payload.get("trade_id") or "")
    if exchange != "none":
        # Exchange must be at the beginning because _alert_dedup_check uses
        # the first 16 chars for Redis key compactness. If appended, BingX and
        # Bybit legs share the same dedup key and the second leg is blocked.
        trade_id_str = f"{exchange}|{trade_id_str}"
    if _alert_dedup_check(trade_id_str, "entry", ticker, timeframe_str):
        write_log(f"ENTRY_DEDUP | {ticker} {direction} {timeframe_str} — skipped duplicate")
        return

    if payload.get("_skip_signal_send"):
        source_msg_id = payload.get("_source_msg_id")
    else:
        source_msg = send_signals(text or f"📥 Вход {ticker} {direction}")
        source_msg_id = source_msg.get("message_id")

    side = "Buy" if direction == "BUY" else "Sell"
    opp_side = "Sell" if side == "Buy" else "Buy"

    # Дефолты берутся из ENV: DEFAULT_LEVERAGE / DEFAULT_SIZE_USDT.
    # PAIR_SETTINGS_JSON deprecated: no pair-specific overrides.
    leverage  = int(DEFAULT_LEVERAGE)
    size_usdt = float(DEFAULT_SIZE_USDT)

    # Risk gate: leverage cannot repair a negative edge.  Cap all new entries
    # at 10x and volatile entries (ATR >= 1.5%) at 5x. Pair overrides may lower
    # these values but can no longer silently restore legacy 20x exposure.
    atr_pct_val = float(payload.get("atr_pct", 0) or 0)
    leverage_cap = 5 if atr_pct_val >= 1.5 else 10
    leverage = max(1, min(leverage, leverage_cap))

    # ── Хелпер: безопасный float из payload-поля ──────────────────────
    def _to_float(val) -> float | None:
        """NaN / Inf / null / 'NaN' / пустые строки → None."""
        try:
            if val is None or str(val).strip().lower() in ("", "null", "none", "nan", "undefined", "inf", "-inf"):
                return None
            f = float(val)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except Exception:
            return None

    # ── Парсим TP/SL: сначала из текста, затем fallback на числовые поля JSON ──
    sl_price  = parse_price(text, "SL:", "⛔ SL:")   or _to_float(payload.get("sl"))
    tp1_price = parse_price(text, "TP1:", "✅ TP1:") or _to_float(payload.get("tp1"))
    tp2_price = parse_price(text, "TP2:", "✅ TP2:") or _to_float(payload.get("tp2"))
    tp3_price = parse_price(text, "TP3:", "✅ TP3:") or _to_float(payload.get("tp3"))
    tp4_price = parse_price(text, "TP4:", "✅ TP4:") or _to_float(payload.get("tp4"))

    # ── Цена входа: из текста, затем из JSON-поля entry_price ────────
    # Pine Script sometimes sends NaN in entry_price JSON field — text has the real value
    price = infer_entry_price(text) or _to_float(payload.get("entry_price"))
    # Additional fallback: try common Pine Script text patterns
    if price is None and text:
        price = (parse_price(text, "📍 Вход:", "Вход:") or
                 parse_price(text, "🎯 Вход:", "Entry:") or
                 parse_price(text, "Price:", "Цена:"))
    # Parse SL/TP from text with additional patterns if JSON was null
    if sl_price is None and text:
        sl_price = parse_price(text, "SL:", "⛔ SL:", "Stop:", "Стоп:")
    if tp1_price is None and text:
        tp1_price = parse_price(text, "TP1:", "TP 1:", "✅ TP1:", "Take 1:")
    if tp2_price is None and text:
        tp2_price = parse_price(text, "TP2:", "TP 2:", "✅ TP2:", "Take 2:")
    if tp3_price is None and text:
        tp3_price = parse_price(text, "TP3:", "TP 3:", "✅ TP3:")
    if tp4_price is None and text:
        tp4_price = parse_price(text, "TP4:", "TP 4:", "✅ TP4:")

    # ── Диагностика — видно что распарсилось ──────────────────────────
    # ✅ IMPROVEMENT #1: SL fallback when Pine Script sends null
    if sl_price is None and price and price > 0:
        atr_fallback_pct = atr_pct_val if atr_pct_val > 0 else 2.0
        atr_abs = price * atr_fallback_pct / 100.0
        sl_mult = 2.0  # 2 × ATR как минимальный SL
        if direction == "BUY":
            sl_price = round(price - atr_abs * sl_mult, 8)
        else:
            sl_price = round(price + atr_abs * sl_mult, 8)
        write_log(f"SL_FALLBACK | {ticker} sl=null → ATR({atr_fallback_pct:.2f}%)×{sl_mult} = {sl_price}")

    write_log(f"PARSED | {ticker} entry={price} sl={sl_price} atr_pct={atr_pct_val} "
              f"tp1={tp1_price} tp2={tp2_price} tp3={tp3_price} "
              f"tp4={tp4_price}")

    required_prices = {
        "entry": price,
        "sl": sl_price,
        "tp1": tp1_price,
        "tp2": tp2_price,
        "tp3": tp3_price,
        "tp4": tp4_price,
    }
    missing_prices = [name for name, val in required_prices.items() if val is None or float(val or 0) <= 0]
    if trade_mode == "trade" and missing_prices:
        write_log(f"ENTRY_REJECT_INVALID_CONTRACT | {ticker} {direction} | missing={missing_prices}")
        send_signals(
            f"⛔ <b>Вход отклонён {ticker} {direction}</b>\n"
            f"Неполный контракт v153: отсутствует {', '.join(missing_prices)}.\n"
            f"Биржевой вход не выполнен.",
            reply_to=source_msg_id,
        )
        return

    qty = 1.0
    sl_order_id = ""
    tp_order_ids: dict = {}
    use_exchange_tps = False
    emergency_unprotected = False

    if trade_mode == "trade":
        prepared = False
        last_prepare_err: Exception | None = None
        original_exchange = exchange
        _candidates = [exchange] if target_exchange in ("bingx", "bybit") else _entry_exchange_candidates(exchange)
        for candidate_exchange in _candidates:
            try:
                ex_set_leverage(ticker, leverage, candidate_exchange)
                price = ex_get_price(ticker, candidate_exchange)
                if candidate_exchange != original_exchange:
                    write_log(f"EXCHANGE_FALLBACK | {ticker} {direction} | {original_exchange}→{candidate_exchange} reason={last_prepare_err}")
                    log_event(
                        "exchange_fallback",
                        ticker=ticker,
                        direction=direction,
                        trade_id=trade_id,
                        from_exchange=original_exchange,
                        to_exchange=candidate_exchange,
                        reason=str(last_prepare_err or ""),
                    )
                    send_signals(
                        f"⚠️ <b>{ticker} {direction}</b>\n"
                        f"BingX недоступен, пробую Bybit fallback.",
                        reply_to=source_msg_id,
                    )
                exchange = candidate_exchange
                prepared = True
                break
            except Exception as e:
                last_prepare_err = e
                write_log(f"ENTRY_PREPARE_ERR | {ticker} ({candidate_exchange}) | {e}")
                if candidate_exchange == "bingx" and _is_exchange_offline_error(e) and BYBIT_AVAILABLE:
                    continue
                break
        if not prepared:
            write_log(f"ENTRY_ERR | prepare_exchange | {ticker} ({exchange}) | {last_prepare_err}")
            send_signals(
                f"❌ <b>Ошибка входа {ticker}</b>\n"
                f"Биржа/цена недоступна ({exchange}): {last_prepare_err}",
                reply_to=source_msg_id,
            )
            return

        qty = ex_calc_qty(ticker, size_usdt, leverage, price, exchange)
        write_log(f"ENTRY | {ticker} {direction} [{exchange}] price={price} qty={qty} lev={leverage}x")

        try:
            ex_place_market(ticker, side, qty, False, exchange)
        except Exception as e:
            write_log(f"ENTRY_FAIL | {ticker} ({exchange}) | {e}")
            send_signals(
                f"❌ <b>Ошибка входа {ticker} {direction}</b> [{exchange}]\n{e}",
                reply_to=source_msg_id,
            )
            return

        time.sleep(0.5)

        live_qty = ex_live_position_qty(ticker, direction, exchange)
        if live_qty and live_qty > 0:
            old_qty = qty
            qty = min(float(qty), float(live_qty))
            write_log(f"ENTRY_QTY_SYNC | {ticker} {direction} [{exchange}] qty={old_qty} live={live_qty} used={qty}")

        sl_failed = False
        if sl_price:
            try:
                resp = ex_place_stop(ticker, opp_side, qty, sl_price, exchange)
                if exchange == "bybit" and resp.get("retCode") == 0:
                    sl_order_id = resp["result"].get("orderId", "")
                elif exchange == "bybit":
                    sl_failed = True
                    write_log(f"SL_PLACE_ERR | {ticker} ({exchange}) | ret={resp.get('retCode')} msg={resp.get('retMsg')}")
                elif exchange == "bingx":
                    sl_order_id = _bingx_order_id(resp) or "ok"
            except Exception as e:
                sl_failed = True
                write_log(f"SL_PLACE_ERR | {ticker} ({exchange}) | {e}")
        else:
            sl_failed = True
            write_log(f"SL_PLACE_ERR | {ticker} ({exchange}) | missing sl_price")

        if sl_failed or not sl_order_id:
            emergency_unprotected = True
            close_qty = ex_live_position_qty(ticker, direction, exchange) or qty
            write_log(f"ENTRY_ABORT_UNPROTECTED | {ticker} {direction} [{exchange}] | close_qty={close_qty} sl_order_id={sl_order_id or '-'}")
            try:
                ex_place_market(ticker, opp_side, close_qty, True, exchange)
                send_signals(
                    f"🚨 <b>{ticker} {direction}: SL не поставился</b>\n"
                    f"Позиция закрыта reduce-only. TP не выставлялись.",
                    reply_to=source_msg_id,
                )
            except Exception as e:
                write_log(f"ENTRY_ABORT_CLOSE_ERR | {ticker} ({exchange}) | {e}")
                send_signals(
                    f"🚨 <b>КРИТИЧНО {ticker} {direction}</b>\n"
                    f"SL не поставился, аварийное закрытие не удалось: {e}",
                    reply_to=source_msg_id,
                )
            return

        # ── Выставляем TP-ордера на биржу ──────────────────────────────────────
        _tp_map = {n: p for n, p in [
            (1, tp1_price), (2, tp2_price), (3, tp3_price),
            (4, tp4_price),
        ] if p}
        if _tp_map and not emergency_unprotected:
            time.sleep(0.3)
            tp_order_ids = place_tp_orders(ticker, side, opp_side, qty, _tp_map, exchange)
            use_exchange_tps = bool(tp_order_ids)
    else:
        write_log(f"ENTRY_TELEGRAM_ONLY | {ticker} {direction} | no exchange execution")
        if price is None:
            price = 0.0
        # BUG 4 NOTE: if entry_price is None/NaN, Pine Script sent NaN in the alert.
        # The signal text is forwarded as-is from Pine Script. No fix needed here —
        # raw text (from `text` field) already contains the correct prices as formatted string.

    created_at = int(time.time())
    instance_id = _trade_instance_id(key, {
        "trade_id": trade_id,
        "ticker": ticker,
        "direction": direction,
        "timeframe": timeframe,
        "created_at": created_at,
    })
    arrow = "🟢" if direction == "BUY" else "🔴"
    side_label = "LONG" if direction == "BUY" else "SHORT"
    if trade_mode == "trade":
        exch_tag = "Bybit" if exchange == "bybit" else "BingX"
        net_tag = ("🧪 TEST" if TESTNET else "🔴 LIVE") if exchange == "bybit" \
                  else ("🧪 DEMO" if BINGX_DEMO else "🔴 LIVE")
        details_line = f"#{ticker}  |  {leverage}x  |  {size_usdt} USDT"
        volume_line = f"📦 Объём: {qty} контр."
    else:
        exch_tag = ""
        net_tag = ""
        details_line = f"#{ticker}"
        volume_line = "📦 Исполнение: без биржи"
    entry_price_text = price if price not in (None, 0.0) else "—"
    tag_str = f"  [{exch_tag}] {net_tag}" if exch_tag else ""
    entry_message = send_signals(
        f"{arrow} <b>{side_label} ОТКРЫТ</b>{tag_str}\n"
        f"{details_line}\n"
        f"📍 Вход: <b>{entry_price_text}</b>  |  ⛔ SL: {sl_price or '—'}\n"
        f"✅ TP1: {tp1_price or '—'}  |  TP2: {tp2_price or '—'}\n"
        f"{volume_line}",
        reply_to=source_msg_id,
    )
    trade_message_id = entry_message.get("message_id") or source_msg_id

    trade_record = {
        "strategy_version": str(payload.get("strategy_version") or STRATEGY_VERSION),
        "schema_version": int(payload.get("schema_version") or SCHEMA_VERSION),
        "tp_contract": str(payload.get("tp_contract") or TP_CONTRACT),
        "message_id": trade_message_id,
        "signal_message_id": source_msg_id,
        "chat_id": TG_CHAT,
        "thread_id": int(TG_SIGNALS_TOPIC),
        "event": "entry",
        "ticker": ticker,
        "direction": direction,
        "timeframe": timeframe,
        "exchange": exchange,
        "trade_mode": trade_mode,
        "trade_id": trade_id,
        "trade_key": key,
        "instance_id": instance_id,
        "is_strong": payload.get("is_strong", False),
        "entry_mode": str(payload.get("entry_mode") or ""),
        "score": payload.get("score"),
        "confirmations": payload.get("confirmations"),
        "atr_pct": payload.get("atr_pct"),
        "amd_phase": str(payload.get("amd_phase") or ""),
        **_entry_metadata(payload),
        "created_at": created_at,
        "entry_price": price,
        "total_qty": qty,
        "remaining_qty": qty,
        "emergency_unprotected": emergency_unprotected,
        "leverage": leverage,
        "notional_usdt": round(float(price or 0) * float(qty or 0), 8),
        "margin_usdt": round((float(price or 0) * float(qty or 0)) / leverage, 8) if leverage > 0 else None,
        "sl_price": sl_price,
    }
    dedup_entry(ticker, direction, current_key=key, exchange=exchange)
    if key:
        put_trade(key, trade_record)

    pkey = pos_key(ticker, direction, exchange)
    with _pos_lock:
        positions = load_positions()
        positions[pkey] = {
            "strategy_version": str(payload.get("strategy_version") or STRATEGY_VERSION),
            "schema_version": int(payload.get("schema_version") or SCHEMA_VERSION),
            "tp_contract": str(payload.get("tp_contract") or TP_CONTRACT),
            "symbol": ticker,
            "ticker": ticker,
            "direction": direction,
            "side": side,
            "opp_side": opp_side,
            "exchange": exchange,
            "trade_mode": trade_mode,
            "entry_price": price,
            "total_qty": qty,
            "remaining_qty": qty,
            "emergency_unprotected": emergency_unprotected,
            "leverage": leverage,
            "notional_usdt": round(float(price or 0) * float(qty or 0), 8),
            "margin_usdt": round((float(price or 0) * float(qty or 0)) / leverage, 8) if leverage > 0 else None,
            "sl_price": sl_price,
            "sl_order_id": sl_order_id,
            "tp1_price": tp1_price,
            "tp2_price": tp2_price,
            "tp3_price": tp3_price,
            "tp4_price": tp4_price,
            "tp_order_ids": tp_order_ids,
            "use_exchange_tps": use_exchange_tps,
            "trade_id": trade_id,
            "trade_key": key,
            "instance_id": instance_id,
            "timeframe": timeframe,
            "is_strong": payload.get("is_strong", False),
            "entry_mode": str(payload.get("entry_mode") or ""),
            "score": payload.get("score"),
            "confirmations": payload.get("confirmations"),
            "atr_pct": payload.get("atr_pct"),
            "amd_phase": str(payload.get("amd_phase") or ""),
            **_entry_metadata(payload),
            "message_id": trade_message_id,
            "signal_message_id": source_msg_id,
            "created_at": created_at,
            "trail_active": False,
            "trail_sl": None,
            "be_active": False,
        }
        save_positions(positions)


def handle_tp_hit(payload: dict):
    ticker = payload.get("ticker", "").upper().replace(".P", "")
    direction = (payload.get("direction") or "").upper()
    tp_num = int(payload.get("tp_num") or 0)
    if tp_num not in TP_CLOSE_PCT:
        write_log(f"TP_REJECT | {ticker} TP{tp_num} | valid levels are TP1..TP4")
        return
    text = payload.get("text", "").strip()
    target_exchange = str(payload.get("target_exchange") or "").lower().strip()
    key = build_trade_key(payload)
    if key and target_exchange in ("bingx", "bybit"):
        key = f"{key}|{target_exchange}"
    trade_key, trade = find_trade_entry(
        key=key,
        trade_id=str(payload.get("trade_id") or ""),
        ticker=ticker,
        direction=direction,
    )
    reply_id = (trade or {}).get("message_id")

    pkey = pos_key(ticker, direction, target_exchange)
    final_close = False
    pos_snapshot = None
    highest_tp = tp_num
    _found_pos = True  # Bug 2 Fix flag
    with _pos_lock:
        positions = load_positions()
        pos = positions.get(pkey)
        if not pos:
            instance_id = _trade_instance_id(trade_key or key, trade, None, payload)
            if _trade_already_closed(instance_id):
                write_log(f"TP_DUPLICATE_SKIP | {ticker} TP{tp_num} | already closed")
                return  # genuine duplicate — skip silently
            # Bug 2 Fix: position missing but NOT a duplicate → mark and return
            _found_pos = False
            # Exit early — position logic below requires valid pos, Telegram msg sent after lock
        else:
            # Price reaching TPn implies all lower ordered targets were crossed.
            # Recover missing flags so webhook loss/reordering cannot corrupt P&L.
            newly_hit = [n for n in range(1, tp_num + 1) if not pos.get(f"tp{n}_hit")]
            if not newly_hit:
                write_log(f"TP_DUPLICATE_SKIP | {ticker} TP{tp_num} | already applied")
                return

            # Process TP hit
            exchange = pos.get("exchange", "bybit")
            total_qty = pos["total_qty"]
            remaining = pos["remaining_qty"]
            opp_side = pos["opp_side"]
            close_share = sum(TP_CLOSE_PCT[n] for n in newly_hit)
            close_qty = round(total_qty * close_share, 8)
            close_qty = min(close_qty, remaining)
            min_q = ex_min_qty(ticker, exchange) if exchange != "none" else 0.0

            if exchange != "none":
                if pos.get("use_exchange_tps"):
                    # TP-ордер уже исполнен биржей как лимитная заявка
                    write_log(f"TP_HIT_EXCHANGE | {ticker} TP{tp_num} | limit order filled by exchange")
                else:
                    if close_qty < min_q:
                        return
                    try:
                        ex_place_market(ticker, opp_side, close_qty, True, exchange)
                    except Exception as e:
                        write_log(f"TP_HIT_ERR | {ticker} TP{tp_num} ({exchange}) | {e}")
                        return
            elif close_qty <= 0:
                close_qty = 0.0

            new_remaining = max(0.0, remaining - close_qty)
            for n in newly_hit:
                pos[f"tp{n}_hit"] = True
                tp_ids = pos.get("tp_order_ids")
                if isinstance(tp_ids, dict):
                    tp_ids.pop(n, None)
                    tp_ids.pop(str(n), None)
            pos["remaining_qty"] = new_remaining
            highest_tp = _highest_tp_hit(pos)

            if new_remaining <= min_q or tp_num >= 4:
                if exchange != "none":
                    cancel_own_orders(pos)  # 🔒 только свои ордера, не ручные юзера
                positions.pop(pkey, None)
                final_close = True
            else:
                # Pine может не прислать sl_moved. Bot обязан сам защитить позицию
                # после TP2/TP3, иначе TP2+ сделки могут закрыться минусом.
                _apply_bot_sl_fallback(pos, highest_tp)
                positions[pkey] = pos
            pos_snapshot = dict(pos)
            save_positions(positions)

    # Bug 2 Fix: if no position was found, still forward the signal to Telegram
    if not _found_pos:
        send_signals(text or f"✅ TP{tp_num} {ticker}", reply_to=reply_id)
        return

    if trade_key:
        touch_trade(
            trade_key,
            highest_tp_hit=highest_tp,
            remaining_qty=pos_snapshot.get("remaining_qty"),
            trail_active=pos_snapshot.get("trail_active", False),
            be_active=pos_snapshot.get("be_active", False),
            sl_price=pos_snapshot.get("sl_price"),
            sl_order_id=pos_snapshot.get("sl_order_id"),
            sl_moved_count=int(pos_snapshot.get("sl_moved_count") or 0),
            last_sl_move_reason=pos_snapshot.get("last_sl_move_reason", ""),
            last_sl_moved_at=pos_snapshot.get("last_sl_moved_at"),
        )
    if final_close:
        # Position fully closed at TP4 (or remaining ≤ min_qty).
        if not pos_snapshot.get(f"tp{tp_num}_hit"):
            pos_snapshot[f"tp{tp_num}_hit"] = True
        pnl = calc_trade_pnl(pos_snapshot, None)
        _entry_fc = pos_snapshot.get("entry_price", 0)
        _dirn_fc  = pos_snapshot.get("direction", "?")
        _lev_fc   = pos_snapshot.get("leverage", 1)
        _tps_str  = " → ".join(f"TP{n}" for n in pnl["tps_hit"])
        _pnl_sign = "+" if pnl["pnl_pct"] >= 0 else ""
        tp_close_msg = (
            f"🏆 <b>ПОЗИЦИЯ ЗАКРЫТА ПОЛНОСТЬЮ</b>\n"
            f"━━━━━━━\n"
            f"#{ticker} {_dirn_fc} — TP{tp_num}\n"
            f"📊 Закрыто: {_tps_str}\n"
            f"📈 <b>П&Л (бот): {_pnl_sign}{pnl['pnl_pct']}%</b>"
            f"  <i>(без плеча: {_pnl_sign}{pnl['pnl_pct_no_lev']}%)</i>"
        )
        send_signals(tp_close_msg, reply_to=reply_id or pos_snapshot.get("message_id"))
        payload["_bot_pnl"] = pnl
        finalize_trade(payload, trade_key, trade, pos_snapshot, "win", highest_tp, close_reason=f"tp_hit_{tp_num}")
    else:
        send_signals(text or f"✅ TP{tp_num} {ticker}", reply_to=reply_id or pos_snapshot.get("message_id"))


def handle_sl_hit(payload: dict):
    ticker = payload.get("ticker", "").upper().replace(".P", "")
    direction = (payload.get("direction") or "").upper()
    text = payload.get("text", "").strip()
    target_exchange = str(payload.get("target_exchange") or "").lower().strip()
    key = build_trade_key(payload)
    if key and target_exchange in ("bingx", "bybit"):
        key = f"{key}|{target_exchange}"
    trade_key, trade = find_trade_entry(
        key=key,
        trade_id=str(payload.get("trade_id") or ""),
        ticker=ticker,
        direction=direction,
    )

    pkey = pos_key(ticker, direction, target_exchange)
    pos_snapshot = None
    highest_tp = 0
    _found_pos_sl = True  # Bug 2 Fix flag
    with _pos_lock:
        positions = load_positions()
        pos = positions.get(pkey)
        if not pos:
            instance_id = _trade_instance_id(trade_key or key, trade, None, payload)
            if _trade_already_closed(instance_id):
                write_log(f"SL_DUPLICATE_SKIP | {ticker} | already closed")
                return
            _found_pos_sl = False
        else:
            exchange = pos.get("exchange", "bybit")
            highest_tp = max(_highest_tp_hit(pos), int(payload.get("highest_tp_hit") or 0))
            _normalize_tp_flags(pos, highest_tp)
            pos_snapshot = dict(pos)
            if exchange != "none":
                cancel_own_orders(pos)  # 🔒 только свои ордера, не ручные юзера
            positions.pop(pkey, None)
            save_positions(positions)

    # Bug 2 Fix: always forward sl_hit to Telegram, even with no tracked position
    if not _found_pos_sl:
        send_signals(text or f"🛑 SL {ticker}")
        return

    reply_id = (trade or {}).get("message_id") or pos_snapshot.get("message_id")

    # ── Рассчитываем реальный P&L в боте (не доверяем Pine Script +0%) ──────
    # BUG #3 FIX: Pine Script nulls all price fields in sl_hit alert — we use pos_snapshot
    # (loaded from Redis) which always has entry_price, leverage, tp prices etc.
    # sl_exit_price is extracted from Pine Script text, or from payload "exit_price" field.

    # BUG #7 FIX: убираем "Price:" fallback — он матчит Entry Price / TP Price / Trigger Price
    # и даёт положительный P&L для убыточных SL-сделок (KSMUSDT +6.07%, BANANAS31 +120%, etc.)
    sl_exit_price = None
    if text:
        sl_exit_price = (
            parse_price(text, "Цена выхода:", "💰 Цена выхода:")
            or parse_price(text, "Exit price:", "Exit price:")
            # НЕ добавляем "Exit:" — может матчить "Exit signal" и пр. нечисловой текст
            # НЕ добавляем "Price:" — слишком широкий, ловит Entry Price, TP Price, Trigger Price
        )
    # Also check JSON payload for exit_price field (с санити-чеком диапазона)
    if not sl_exit_price:
        try:
            _ep = payload.get("exit_price") or payload.get("close_price")
            if _ep and str(_ep).lower() not in ("null", "none", "nan", ""):
                _ep_f = float(_ep)
                # BUG #7 FIX: санити-чек — exit_price должен быть в ±50% от entry
                # иначе это, скорее всего, неправильное поле (TP price вместо SL exit)
                _entry_chk = pos_snapshot.get("entry_price", 0) if pos_snapshot else 0
                if _entry_chk > 0 and 0.5 * _entry_chk < _ep_f < 2.0 * _entry_chk:
                    sl_exit_price = _ep_f
                elif _entry_chk <= 0:
                    sl_exit_price = _ep_f  # нет базы для сравнения — берём как есть
        except Exception:
            pass
    # If still missing, use sl_price from position as approximation
    if not sl_exit_price and pos_snapshot:
        _sl_approx = pos_snapshot.get("sl_price") or pos_snapshot.get("trail_sl")
        if _sl_approx:
            sl_exit_price = float(_sl_approx)
            write_log(f"SL_EXIT_APPROX | {ticker} | using stored sl_price={sl_exit_price}")
    write_log(f"PNL_CALC | {ticker} | entry={pos_snapshot.get('entry_price')} sl_exit={sl_exit_price} lev={pos_snapshot.get('leverage')}")
    pnl = calc_trade_pnl(pos_snapshot, sl_exit_price)

    # BUG #7 FIX: Санити-чек P&L после расчёта
    # Для ЧИСТОГО SL (нет ни одного TP hit, нет trail) P&L обязан быть ≤ 0%
    # Положительный — признак того, что sl_exit_price = TP-цена из "Price:" паттерна
    _trail_active_snap = (pos_snapshot or {}).get("trail_active", False)
    if not _trail_active_snap and highest_tp == 0 and pnl.get("pnl_pct", 0) > 1.0:
        write_log(
            f"PNL_SANITY_FAIL | {ticker} | pnl={pnl['pnl_pct']}% при чистом SL без TP — "
            f"sl_exit_price={sl_exit_price} скорее всего неверна → пересчёт по sl_price"
        )
        _sl_fallback = pos_snapshot.get("sl_price") or pos_snapshot.get("trail_sl")
        if _sl_fallback:
            sl_exit_price = float(_sl_fallback)
            pnl = calc_trade_pnl(pos_snapshot, sl_exit_price)
            write_log(f"PNL_SANITY_RESET | {ticker} | пересчитано по sl_price={sl_exit_price} → pnl={pnl['pnl_pct']}%")
        else:
            # Нет данных — обнуляем чтобы не засорять статистику фантомными цифрами
            pnl = {"pnl_pct": 0.0, "pnl_pct_no_lev": 0.0,
                   "tp_pnl_pct": 0.0, "sl_pnl_pct": 0.0,
                   "highest_tp": 0, "tps_hit": [],
                   "remaining_pct": 0.0, "closed_on_tp_pct": 0.0}
            write_log(f"PNL_SANITY_ZERO | {ticker} | sl_price отсутствует → P&L обнулён")
    _exchange_sl  = pos_snapshot.get("exchange", "bybit")
    _lev          = pos_snapshot.get("leverage", 1)
    _dirn         = pos_snapshot.get("direction", "?")
    _entry        = pos_snapshot.get("entry_price", 0)
    _tps_hit_list = pnl["tps_hit"]

    # Формируем обогащённое сообщение (заменяем текст индикатора нашим)
    if _tps_hit_list:
        _tp_str = " → ".join(f"TP{n}" for n in _tps_hit_list)
        _res_label = (
            f"✅ Частичная прибыль ({_tp_str})"
            if pnl["pnl_pct"] > 0
            else f"⚠️ TP достигнут, но итоговый убыток ({_tp_str})"
        )
    else:
        _res_label = "❌ Чистый убыток"

    _pnl_sign = "+" if pnl["pnl_pct"] >= 0 else ""
    _pnl_color = "📈" if pnl["pnl_pct"] >= 0 else "📉"

    # Длительность (из trade_data)
    _dur_str = ""
    _created = (trade or {}).get("created_at") or pos_snapshot.get("created_at")
    if _created:
        _dur_sec = int(time.time()) - int(_created)
        _dur_str = f"\n⏱ Время в сделке: {fmt_duration(_dur_sec)}"

    _exch_label = ""
    if _exchange_sl != "none":
        _demo_flag = "🧪DEMO" if (_exchange_sl == "bingx" and BINGX_DEMO) else ("🧪TEST" if TESTNET else "🔴LIVE")
        _exch_label = f" [{_exchange_sl.upper()} {_demo_flag}]"

    _tp_part_sign = "+" if pnl["tp_pnl_pct"] >= 0 else ""
    _sl_part_sign = "+" if pnl["sl_pnl_pct"] >= 0 else ""
    bot_msg = (
        f"🛑 <b>СТОП ВЫБИТ</b>{_exch_label}\n"
        f"━━━━━━━\n"
        f"#{ticker} {_dirn}"
        f"{_dur_str}\n"
        f"📍 Вход: {_entry}  |  SL-выход: {sl_exit_price or chr(8212)}\n"
        f"📊 {_res_label}\n"
        f"{_pnl_color} <b>П&amp;Л (бот): {_pnl_sign}{pnl['pnl_pct']}%</b>"
        f"  <i>(без плеча: {_pnl_sign}{pnl['pnl_pct_no_lev']}%)</i>\n"
        f"  TP-часть: {_tp_part_sign}{pnl['tp_pnl_pct']}%"
        f"  |  SL-остаток: {_sl_part_sign}{pnl['sl_pnl_pct']}%"
        f"  ({pnl['remaining_pct']}% позиции)"
    )

    # Отправляем НАШЕ сообщение (точный P&L), потом текст из Pine Script если есть
    send_signals(bot_msg, reply_to=reply_id)
    if text and text.strip() != bot_msg.strip():
        # Пересылаем оригинальный текст Pine Script для сравнения
        write_log(f"SL_HIT_PINESCRIPT_TEXT | {ticker} | {text[:120]}")

    # Execution path and profitability are separate dimensions:
    # highest_tp_hit records reached targets; result follows net P&L sign.
    # This prevents a TP1→wide-SL trade with negative net P&L from becoming a win.
    result = "partial" if highest_tp > 0 and pnl.get("pnl_pct", 0.0) > 0.0 else "loss"

    # Store pnl in finalize payload
    payload["_bot_pnl"] = pnl
    finalize_trade(payload, trade_key, trade, pos_snapshot, result, highest_tp, close_reason="sl_hit")


def handle_sl_moved(payload: dict):
    ticker    = payload.get("ticker", "").upper().replace(".P", "")
    direction = (payload.get("direction") or "").upper()
    text      = payload.get("text", "").strip()
    target_exchange = str(payload.get("target_exchange") or "").lower().strip()
    key       = build_trade_key(payload)
    if key and target_exchange in ("bingx", "bybit"):
        key = f"{key}|{target_exchange}"

    trade_key, trade = find_trade_entry(
        key=key,
        trade_id=str(payload.get("trade_id") or ""),
        ticker=ticker,
        direction=direction,
    )

    try:
        new_sl = float(payload.get("new_sl")) if payload.get("new_sl") is not None else None
        if new_sl is not None and (not math.isfinite(new_sl) or new_sl <= 0):
            new_sl = None
    except (TypeError, ValueError):
        new_sl = None
    new_sl = new_sl or parse_price(text, "✅ Стало:", "Стало:")
    if not new_sl:
        write_log(f"SL_MOVED_REJECT | {ticker} | missing/invalid new_sl")
        return
    pkey = pos_key(ticker, direction, target_exchange)
    payload_trade_id = str(payload.get("trade_id") or "").strip()
    with _pos_lock:
        positions = load_positions()
        pos = positions.get(pkey)
        if not pos:
            write_log(f"SL_MOVED_ORPHAN_SKIP | {ticker} {direction} | no active position")
            log_event("sl_moved_orphan_skip", ticker=ticker, direction=direction, trade_id=payload_trade_id, reason="no_active_position")
            return
        pos_trade_id = str(pos.get("trade_id") or "").strip()
        pos_trade_key = str(pos.get("trade_key") or "").strip()
        exact_match = (trade_key and pos_trade_key and trade_key == pos_trade_key) or (payload_trade_id and pos_trade_id and payload_trade_id == pos_trade_id)
        legacy_no_id = not payload_trade_id and not trade_key
        if not exact_match and not legacy_no_id:
            write_log(f"SL_MOVED_STALE_SKIP | {ticker} {direction} | payload_trade_id={payload_trade_id or '-'} pos_trade_id={pos_trade_id or '-'}")
            log_event("sl_moved_stale_skip", ticker=ticker, direction=direction, trade_id=payload_trade_id, pos_trade_id=pos_trade_id)
            return
        reply_id = (trade or {}).get("message_id") or pos.get("message_id")
        send_signals(text or f"🔒 SL сдвинут {ticker}", reply_to=reply_id)
        oid = ""
        if pos.get("exchange", "bybit") != "none":
            oid = move_sl(pos, new_sl)
        entry = float(pos.get("entry_price") or 0.0)
        tolerance = max(abs(entry) * 1e-5, 1e-10)
        reason = str(payload.get("sl_reason") or text or "")
        explicit_be = payload.get("be_active") is True
        legacy_be = "TP1→Entry" in reason or "TP1->Entry" in reason
        moved_to_entry = entry > 0 and abs(float(new_sl) - entry) <= tolerance
        if moved_to_entry and (explicit_be or legacy_be):
            pos["be_active"] = True
        explicit_trail = payload.get("trail_active") is True
        legacy_trail = "trail" in reason.lower() or "трейл" in reason.lower()
        if explicit_trail or legacy_trail:
            pos["trail_active"] = True
            pos["trail_sl"] = new_sl
        pos["sl_price"]    = new_sl
        pos["sl_order_id"] = oid
        pos["sl_moved_count"] = int(pos.get("sl_moved_count") or 0) + 1
        pos["last_sl_move_reason"] = reason[:160]
        pos["last_sl_moved_at"] = int(time.time())
        positions[pkey] = pos
        save_positions(positions)
    if trade_key:
        touch_trade(
            trade_key,
            sl_price=new_sl,
            be_active=bool(pos.get("be_active", False)),
            trail_active=bool(pos.get("trail_active", False)),
            sl_moved_count=int(pos.get("sl_moved_count") or 0),
            last_sl_move_reason=pos.get("last_sl_move_reason", ""),
            last_sl_moved_at=pos.get("last_sl_moved_at"),
        )
    log_event(
        "sl_moved_applied",
        ticker=ticker, direction=direction, trade_id=payload_trade_id, new_sl=new_sl,
        be_active=bool(pos.get("be_active", False)), trail_active=bool(pos.get("trail_active", False)),
        sl_moved_count=int(pos.get("sl_moved_count") or 0), exchange=pos.get("exchange", "none"), order_id=oid,
    )


def close_position_manually(pos: dict, source: str) -> tuple[bool, str]:
    ticker = pos["symbol"]
    direction = pos["direction"]
    remaining = pos.get("remaining_qty", 0)
    exchange = pos.get("exchange", "bybit")
    trade_key, trade = find_trade_entry(
        key=pos.get("trade_key", ""),
        trade_id=str(pos.get("trade_id") or ""),
        ticker=ticker,
        direction=direction,
    )
    if remaining > 0:
        try:
            if exchange != "none":
                cancel_own_orders(pos)  # 🔒 только свои ордера, не ручные юзера
                ex_place_market(ticker, pos["opp_side"], remaining, True, exchange)
        except Exception as e:
            write_log(f"MANUAL_CLOSE_ERR | {ticker} | {e}")
            return False, f"{ticker}[{exchange}]"

    with _pos_lock:
        positions = load_positions()
        positions.pop(pos_key(ticker, direction, exchange), None)
        save_positions(positions)

    reply_id = (trade or {}).get("message_id") or pos.get("message_id")
    send_signals(
        f"🧯 <b>Сделка закрыта вручную</b>\n#{ticker} [{exchange}]",
        reply_to=reply_id,
    )
    payload = {
        "ticker": ticker,
        "direction": direction,
        "timeframe": pos.get("timeframe", ""),
        "trade_id": pos.get("trade_id", ""),
        "exchange": exchange,
        "event": "manual_close",
    }
    finalize_trade(
        payload,
        trade_key,
        trade,
        pos,
        "manual",
        _highest_tp_hit(pos),
        close_reason=source,
    )
    return True, f"{ticker}[{exchange}]"


def process_signal(payload: dict):
    if _emergency_stop.is_set():
        write_log("EMERGENCY_STOP active — signal dropped")
        return

    event = payload.get("event", "unknown")
    if event == "enty":  # алиас опечатки из TradingView
        event = "entry"
    
    # ⏰ Логирование задержки между TradingView и ботом
    alert_time_ms = payload.get("alert_time")
    if alert_time_ms:
        delay_ms = int(time.time() * 1000) - alert_time_ms
        delay_sec = delay_ms / 1000
        write_log(f"PROCESS | event={event} | ticker={payload.get('ticker','?')} | delay={delay_sec:.2f}s")
    else:
        write_log(f"PROCESS | event={event} | ticker={payload.get('ticker','?')}")

    text = payload.get("text", "").strip()
    key  = build_trade_key(payload)

    if event in ("entry", "limit_hit") and not payload.get("target_exchange") and _dual_execution_enabled():
        source_msg = send_signals(text or f"📥 Вход {payload.get('ticker','')} {payload.get('direction','')}")
        source_msg_id = source_msg.get("message_id")
        for ex in _entry_execution_targets():
            p2 = dict(payload)
            p2["target_exchange"] = ex
            p2["_skip_signal_send"] = True
            p2["_source_msg_id"] = source_msg_id
            handle_entry(p2)
        return
    if event == "entry":
        handle_entry(payload)
    elif event == "limit_hit":
        handle_entry(payload)
    elif event == "smart_entry":
        write_log(f"SMART_ENTRY_IGNORED | ticker={payload.get('ticker','?')} direction={payload.get('direction','?')} reason=no_trade_contract")
        return
    elif event == "limit_order":
        send_signals(text or f"📋 Лимитный ордер {payload.get('ticker','')}")
    elif event == "tp_hit":
        if not payload.get("target_exchange") and _dual_execution_enabled():
            targets = _active_exchanges_for(payload.get("ticker", ""), payload.get("direction", ""))
            if targets:
                for ex in targets:
                    p2 = dict(payload)
                    if ex in ("bingx", "bybit"):
                        p2["target_exchange"] = ex
                    handle_tp_hit(p2)
                return
        handle_tp_hit(payload)
    elif event == "sl_hit":
        if not payload.get("target_exchange") and _dual_execution_enabled():
            targets = _active_exchanges_for(payload.get("ticker", ""), payload.get("direction", ""))
            if targets:
                for ex in targets:
                    p2 = dict(payload)
                    if ex in ("bingx", "bybit"):
                        p2["target_exchange"] = ex
                    handle_sl_hit(p2)
                return
        handle_sl_hit(payload)
    elif event == "sl_moved":
        if not payload.get("target_exchange") and _dual_execution_enabled():
            targets = _active_exchanges_for(payload.get("ticker", ""), payload.get("direction", ""))
            if targets:
                for ex in targets:
                    p2 = dict(payload)
                    if ex in ("bingx", "bybit"):
                        p2["target_exchange"] = ex
                    handle_sl_moved(p2)
                return
        handle_sl_moved(payload)
    elif event == "scale_in":
        trade    = get_trade(key) if key else None
        reply_id = trade["message_id"] if trade else None
        send_signals(text or f"📈 Scale in {payload.get('ticker','')}", reply_to=reply_id)
    else:
        send_signals(text or json.dumps(payload, ensure_ascii=False))


# Fast-lane events: process immediately in a thread, don't block the queue.
# Entry signals go through the queue (need ordering, dedup). All others → fast lane.
_FAST_LANE_EVENTS = {"tp_hit", "sl_hit", "sl_moved", "limit_order", "scale_in"}


def enqueue_signal(payload: dict):
    event = payload.get("event", "")
    # FIX 8: non-entry events bypass the queue to avoid being blocked by long
    # entry processing (e.g. UNIUSDT open taking 5s delays GPSUSDT tp_hit).
    if event in _FAST_LANE_EVENTS:
        threading.Thread(
            target=_safe_process_signal,
            args=(payload,),
            daemon=True,
            name=f"fast_{event}_{payload.get('ticker','?')}",
        ).start()
        return
    with _queue_lock:
        _signal_queue.append({"payload": payload, "attempts": 0, "queued_at": time.time()})


def _safe_process_signal(payload: dict):
    try:
        process_signal(payload)
    except Exception as e:
        write_log(f"FAST_LANE_ERR | {payload.get('event','?')} {payload.get('ticker','?')} | {e}")


def _queue_worker():
    write_log("QUEUE_WORKER | start")
    while True:
        with _queue_lock:
            item = _signal_queue.pop(0) if _signal_queue else None
        if item:
            payload  = item["payload"]
            attempts = item["attempts"]
            queued_at = item.get("queued_at", 0)
            retry_after = item.get("retry_after", 0)
            
            # ⏰ Если элемент запланирован на будущее - возвращаем в очередь
            now = time.time()
            if retry_after > now:
                with _queue_lock:
                    _signal_queue.append(item)
                time.sleep(0.5)
                continue
            
            # ⏰ Показываем время ожидания в очереди
            wait_time = now - queued_at
            if wait_time > 5:  # Предупреждаем если ждали больше 5 секунд
                write_log(f"QUEUE_WAIT | ticker={payload.get('ticker','?')} | wait={wait_time:.2f}s | attempts={attempts}")
            
            try:
                process_signal(payload)
            except Exception as e:
                write_log(f"QUEUE_ERR | attempt={attempts} | ticker={payload.get('ticker','?')} | {e}")
                if attempts < MAX_QUEUE_ATTEMPTS - 1:
                    item["attempts"] += 1
                    # 🔁 Экспоненциальная задержка: 1s, 2s, 4s вместо фиксированных 15s
                    retry_delay = min(2 ** attempts, 15)
                    item["retry_after"] = time.time() + retry_delay
                    with _queue_lock:
                        # Кладём в КОНЕЦ очереди, не блокируя другие сигналы
                        _signal_queue.append(item)
                    write_log(f"QUEUE_RETRY | ticker={payload.get('ticker','?')} | delay={retry_delay}s | attempt={attempts+1}")
                else:
                    write_log(f"QUEUE_DEAD | ticker={payload.get('ticker','?')} | payload={json.dumps(payload)[:200]}")
        else:
            time.sleep(0.1)  # Быстрее проверяем если очередь пуста


# ══════════════════════════════════════════════════════════════════════════════
# FEAR & GREED
# ══════════════════════════════════════════════════════════════════════════════
FEAR_GREED_URL = "https://api.alternative.me/fng/?limit=1"
_FG_LABELS = [(0,24,"Extreme Fear"),(25,44,"Fear"),(45,55,"Neutral"),
              (56,74,"Greed"),(75,100,"Extreme Greed")]


def _fg_emoji(v: int) -> str:
    if v <= 24: return "😱"
    if v <= 44: return "😨"
    if v <= 55: return "😐"
    if v <= 74: return "🤑"
    return "🚀"


def _fg_label(v: int) -> str:
    for lo, hi, label in _FG_LABELS:
        if lo <= v <= hi: return label
    return "Unknown"


def _fg_bar(v: int, n: int = 20) -> str:
    filled = round(v / 100 * n)
    return "█" * filled + "░" * (n - filled)


def _fetch_fg() -> dict | None:
    try:
        resp  = requests.get(FEAR_GREED_URL, timeout=8)
        entry = resp.json()["data"][0]
        value = int(entry["value"])
        return {"value": value, "label": _fg_label(value)}
    except Exception as e:
        write_log(f"FG_FETCH_ERR | {e}")
        return None


def _build_fg_message(fg: dict, trigger: str = "scheduled") -> str:
    v    = fg["value"]; label = fg["label"]
    bar  = _fg_bar(v); msk = _msk().strftime("%d.%m.%Y %H:%M")
    header = "🔔 <b>Fear & Greed — смена зоны!</b>" if trigger == "change" \
             else "📊 <b>Fear & Greed Index</b>"
    text  = f"{header}\n\n"
    text += f"{_fg_emoji(v)}  <b>{v} / 100</b>  —  {label}\n"
    text += f"<code>[{bar}]</code>\n\n"
    if v <= 24:   text += "🔻 Рынок в панике. Часто — точка разворота вверх.\n"
    elif v <= 44: text += "📉 Преобладает страх. Возможны покупки на откатах.\n"
    elif v <= 55: text += "➡️ Рынок нейтрален. Ждём определённости.\n"
    elif v <= 74: text += "📈 Жадность растёт. Следим за перекупленностью.\n"
    else:         text += "🔺 Эйфория. Высокий риск разворота вниз.\n"
    text += f"\n🕐 Обновлено: {msk} МСК"
    return text


def _check_fg(force: bool = False) -> bool:
    fg = _fetch_fg()
    if fg is None:
        return False
    state      = load_json(FG_STATE_FILE, {})
    last_label = state.get("label", "")
    trigger    = "change" if (last_label and last_label != fg["label"]) else "scheduled"
    if not force and trigger != "change":
        return False
    try:
        send_fg(_build_fg_message(fg, trigger))
        save_json(FG_STATE_FILE, {
            "value":       fg["value"],
            "label":       fg["label"],
            "sent_at":     int(time.time()),
            "sent_at_msk": _msk().strftime("%Y-%m-%d %H:%M"),
        })
        return True
    except Exception as e:
        write_log(f"FG_SEND_ERR | {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# СЕССИИ
# ══════════════════════════════════════════════════════════════════════════════
SESSIONS = [
    {"name": "🇦🇺 Австралия",    "open": "02:00", "close": "09:00"},
    {"name": "🇯🇵 Азия (Токио)", "open": "03:00", "close": "09:00"},
    {"name": "🇬🇧 Европа",       "open": "10:00", "close": "18:30"},
    {"name": "🇺🇸 Америка",      "open": "16:30", "close": "23:00"},
]


def _sessions_status() -> str:
    msk = _msk()
    cur = msk.hour * 60 + msk.minute
    active = []
    upcoming = []
    lines_all = []
    for s in SESSIONS:
        oh, om = map(int, s["open"].split(":"))
        ch, cm = map(int, s["close"].split(":"))
        o_min  = oh * 60 + om
        c_min  = ch * 60 + cm
        if c_min < o_min:
            is_open = cur >= o_min or cur < c_min
        else:
            is_open = o_min <= cur < c_min
        if is_open:
            active.append(s["name"])
        else:
            # Минут до открытия
            diff = o_min - cur
            if diff < 0:
                diff += 1440
            upcoming.append((diff, s["name"], s["open"]))
        status = "🟢 Открыта" if is_open else "🔴 Закрыта"
        lines_all.append(f"{s['name']}  {s['open']}–{s['close']}  {status}")

    text = f"🕐 <b>Время МСК: {msk.strftime('%H:%M')}</b>\n"
    if active:
        text += f"📊 Активные сессии: <b>{', '.join(a.split()[-1] for a in active)}</b>\n"
    else:
        text += "📊 Активные сессии: —\n"

    if upcoming:
        upcoming.sort()
        mins_left, next_name, next_open = upcoming[0]
        h_left, m_left = divmod(mins_left, 60)
        dur_str = f"{h_left}ч {m_left}м" if h_left else f"{m_left}м"
        short = next_name.split()[-1]
        text += f"⏰ Следующее: <b>{short}</b> в {next_open} МСК (через {dur_str})\n"

    text += f"\n📅 <b>Расписание (МСК):</b>\n"
    text += "\n".join(lines_all)
    return text


# ══════════════════════════════════════════════════════════════════════════════
# ПЛАНИРОВЩИК
# ══════════════════════════════════════════════════════════════════════════════
def _scheduler():
    write_log("SCHEDULER | start")
    while True:
        try:
            msk   = _msk()
            cur   = msk.hour * 60 + msk.minute
            h, m  = msk.hour, msk.minute
            today = msk.strftime("%Y-%m-%d")

            # Portfolio ground truth: one exchange-equity observation per hour.
            # This does not place/cancel orders and is safe in live/demo modes.
            if 0 <= m <= 1:
                equity_key = f"equity_{today}_{h:02d}"
                if not _was_sent(equity_key):
                    _mark_sent(equity_key)
                    _snapshot_equity()

            if h in FG_SCHEDULED_HOURS and 0 <= m <= 1:
                fg_key = f"fg_{today}_{h:02d}"
                if not _was_sent(fg_key):
                    _mark_sent(fg_key)
                    _check_fg(force=True)

            # Авто-очистка зависших сделок каждые 6 часов
            if h % 6 == 0 and 0 <= m <= 1:
                auto_clean_key = f"auto_cleanup_{today}_{h:02d}"
                if not _was_sent(auto_clean_key):
                    _mark_sent(auto_clean_key)
                    n = cleanup_old_trades()
                    if n > 0:
                        write_log(f"AUTO_CLEANUP | removed {n} orphaned trades")

            if h == 23 and 55 <= m <= 59:
                key = f"daily_{today}"
                if not _was_sent(key):
                    _mark_sent(key)
                    history    = load_history()
                    day_trades = [r for r in history if r.get("date_msk") == today]
                    if day_trades:
                        send_signals(_build_report(day_trades, f"📅 <b>Дневной отчёт {today}</b>"))

            if msk.weekday() == 6 and h == 23 and 55 <= m <= 59:
                week_key = f"{msk.year}-W{msk.isocalendar()[1]:02d}"
                key = f"weekly_{week_key}"
                if not _was_sent(key):
                    _mark_sent(key)
                    history     = load_history()
                    week_trades = [r for r in history if r.get("week_msk") == week_key]
                    if week_trades:
                        send_signals(_build_report(week_trades, f"📅 <b>Недельный отчёт {week_key}</b>"))

            last_day = calendar.monthrange(msk.year, msk.month)[1]
            if msk.day == last_day and h == 23 and 55 <= m <= 59:
                month_key = f"{msk.year}-{msk.month:02d}"
                key = f"monthly_{month_key}"
                if not _was_sent(key):
                    _mark_sent(key)
                    history      = load_history()
                    month_trades = [r for r in history if r.get("date_msk","").startswith(month_key)]
                    if month_trades:
                        send_signals(_build_report(month_trades, f"📅 <b>Месячный отчёт {month_key}</b>"))

            # Сессии — только в будни (пн=0 ... пт=4)
            if msk.weekday() < 5:
                for s in SESSIONS:
                    oh, om = map(int, s["open"].split(":"))
                    ch, cm = map(int, s["close"].split(":"))
                    open_min  = oh * 60 + om
                    close_min = ch * 60 + cm

                    if -1 <= open_min - cur <= 1:
                        k = f"sess_open_{today}_{s['open']}"
                        if not _was_sent(k):
                            _mark_sent(k)
                            send_sessions(f"🟢 {s['name']} открылась! ({s['open']} МСК)")

                    is_midnight = (ch == 0 and cm == 0)
                    at_close    = (-1 <= cur <= 1) if is_midnight else (-1 <= close_min - cur <= 1)
                    if at_close:
                        key_date = (msk - datetime.timedelta(days=1)).strftime("%Y-%m-%d") \
                                   if is_midnight else today
                        k = f"sess_close_{key_date}_{s['close']}"
                        if not _was_sent(k):
                            _mark_sent(k)
                            send_sessions(f"🔴 {s['name']} закрылась! ({s['close']} МСК)")

        except Exception as e:
            write_log(f"SCHEDULER_ERR | {e}")
        time.sleep(60)


# ══════════════════════════════════════════════════════════════════════════════
# POSITION MANAGER (Trailing Stop)
# ══════════════════════════════════════════════════════════════════════════════
_last_exchange_sync = 0
_EXCHANGE_SYNC_INTERVAL = 3600  # один раз в час; /sync обходит interval вручную


def _recover_missing_trade_records() -> int:
    """Restore a minimal trade record for every tracked position.

    Position is the execution state used for TP/SL handling.  A missing trade
    record breaks reply threading and terminal history linkage, so parity is
    repaired without changing exchange orders or position size.
    """
    positions = dict(load_positions())
    trades = dict(load_trades())
    recovered = 0
    now = int(time.time())

    for pkey, pos in positions.items():
        ticker = str(pos.get("symbol") or pos.get("ticker") or "").upper().replace(".P", "")
        direction = str(pos.get("direction") or "").upper()
        trade_id = str(pos.get("trade_id") or "").strip()
        trade_key = str(pos.get("trade_key") or trade_id or "").strip()
        already_present = bool(trade_key and trade_key in trades) or any(
            str(t.get("trade_id") or "") == trade_id and trade_id
            or (str(t.get("ticker") or "").upper().replace(".P", "") == ticker
                and str(t.get("direction") or "").upper() == direction)
            for t in trades.values() if isinstance(t, dict)
        )
        if already_present or not ticker or direction not in ("BUY", "SELL"):
            continue

        key = trade_key or f"recovered:{pkey}:{int(pos.get('created_at') or now)}"
        record = {
            "strategy_version": str(pos.get("strategy_version") or "legacy"),
            "schema_version": int(pos.get("schema_version") or 1),
            "tp_contract": str(pos.get("tp_contract") or "legacy"),
            "event": "entry",
            "ticker": ticker,
            "direction": direction,
            "timeframe": pos.get("timeframe", ""),
            "exchange": pos.get("exchange", "none"),
            "trade_mode": pos.get("trade_mode", "telegram_only"),
            "trade_id": trade_id,
            "trade_key": key,
            "instance_id": str(pos.get("instance_id") or trade_id or key),
            "message_id": pos.get("message_id"),
            "signal_message_id": pos.get("signal_message_id"),
            "created_at": int(pos.get("created_at") or now),
            "entry_price": pos.get("entry_price"),
            "sl_price": pos.get("sl_price"),
            "total_qty": pos.get("total_qty"),
            "remaining_qty": pos.get("remaining_qty"),
            "leverage": pos.get("leverage"),
            "is_strong": bool(pos.get("is_strong", False)),
            "entry_mode": pos.get("entry_mode", ""),
            "score": pos.get("score"),
            "confirmations": pos.get("confirmations"),
            "atr_pct": pos.get("atr_pct"),
            "amd_phase": pos.get("amd_phase", ""),
            "be_active": bool(pos.get("be_active", False)),
            "trail_active": bool(pos.get("trail_active", False)),
            "sl_moved_count": int(pos.get("sl_moved_count") or 0),
            "last_sl_move_reason": pos.get("last_sl_move_reason", ""),
            "last_sl_moved_at": pos.get("last_sl_moved_at"),
            "state_recovered": True,
            "recovered_at": now,
        }
        trades[key] = record
        if pos.get("trade_key") != key:
            pos["trade_key"] = key
            positions[pkey] = pos
        recovered += 1

    if recovered:
        save_trades(trades)
        save_positions(positions)
        write_log(f"STATE_RECOVERY | restored {recovered} missing trade records")
    return recovered


def _sync_exchange_positions():
    """
    Сверяет tracked-позиции с реальными позициями на бирже.
    Phantom-позиции (в Redis есть, на бирже нет) — удаляются автоматически.
    Решает проблему STOUSDT/BANANAS31 DUPLICATE после редеплоя.
    """
    global _last_exchange_sync
    now = time.time()
    if now - _last_exchange_sync < _EXCHANGE_SYNC_INTERVAL:
        return
    _last_exchange_sync = now

    with _pos_lock:
        positions = dict(load_positions())

    if not positions:
        return

    # ── Bybit sync ──────────────────────────────────────────────────────────
    bybit_tracked = {pkey: pos for pkey, pos in positions.items()
                     if pos.get("exchange") == "bybit"}
    if bybit_tracked and BYBIT_AVAILABLE:
        try:
            resp = bybit().get_positions(category="linear", settleCoin="USDT")
            live_positions = {
                (p["symbol"], "BUY" if str(p.get("side", "")).lower() == "buy" else "SELL")
                for p in resp.get("result", {}).get("list", [])
                if float(p.get("size", 0)) > 0 and p.get("side") in ("Buy", "Sell")
            }
            phantoms = [
                pkey for pkey, pos in bybit_tracked.items()
                if pos.get("symbol") and
                (pos["symbol"], str(pos.get("direction") or "").upper()) not in live_positions
                and pos.get("exchange") != "none"
            ]
            if phantoms:
                phantom_positions = [bybit_tracked.get(pkey, {}) for pkey in phantoms]
                with _pos_lock:
                    p2 = load_positions()
                    for pkey in phantoms:
                        p2.pop(pkey, None)
                    save_positions(p2)
                _remove_trades_for_phantom_positions(phantom_positions, "sync_bybit_phantom")
                _phantom_list = "\n".join(f"• {pk}" for pk in phantoms)
                send_signals(
                    f"🔄 <b>Синхронизация Bybit</b>\n"
                    f"Удалено призрачных: <b>{len(phantoms)}</b>\n"
                    + _phantom_list
                )
                write_log(f"SYNC_BYBIT | removed {len(phantoms)} phantoms: {phantoms}")
        except Exception as e:
            write_log(f"SYNC_BYBIT_ERR | {e}")

    # ── BingX sync ──────────────────────────────────────────────────────────
    bingx_tracked = {pkey: pos for pkey, pos in positions.items()
                     if pos.get("exchange") == "bingx"}
    if bingx_tracked and BINGX_AVAILABLE:
        try:
            data = _bingx_req("GET", "/openApi/swap/v2/user/positions", {})
            live_bingx = {
                (p.get("symbol", "").replace("-", ""),
                 "BUY" if str(p.get("positionSide") or "").upper() == "LONG"
                 else "SELL" if str(p.get("positionSide") or "").upper() == "SHORT"
                 else "BUY" if float(p.get("positionAmt", 0) or 0) > 0 else "SELL")
                for p in (data.get("data") or [])
                if float(p.get("positionAmt", 0) or 0) != 0
            }
            phantoms_bx = [
                pkey for pkey, pos in bingx_tracked.items()
                if pos.get("symbol") and
                (_bingx_to_symbol(pos["symbol"]).replace("-", ""),
                 str(pos.get("direction") or "").upper()) not in live_bingx
            ]
            if phantoms_bx:
                phantom_positions = [bingx_tracked.get(pkey, {}) for pkey in phantoms_bx]
                with _pos_lock:
                    p2 = load_positions()
                    for pkey in phantoms_bx:
                        p2.pop(pkey, None)
                    save_positions(p2)
                _remove_trades_for_phantom_positions(phantom_positions, "sync_bingx_phantom")
                write_log(f"SYNC_BINGX | removed {len(phantoms_bx)} phantoms: {phantoms_bx}")
        except Exception as e:
            write_log(f"SYNC_BINGX_ERR | {e}")

    recovered = _recover_missing_trade_records()
    removed_orphans = cleanup_old_trades()
    write_log(
        f"SYNC_STATE | positions={len(load_positions())} trades={len(load_trades())} "
        f"recovered={recovered} removed_orphans={removed_orphans}"
    )


def _position_manager():
    write_log("POSITION_MANAGER | start")
    while True:
        try:
            # Direction-aware сверка с биржами один раз в час.
            _sync_exchange_positions()

            with _pos_lock:
                snapshot = dict(load_positions())

            for pkey, pos in snapshot.items():
                exchange = pos.get("exchange", "bybit")
                remaining = pos.get("remaining_qty", 0)

                # Удаляем мёртвые позиции (нулевой объём или без биржи без trail)
                # Bug 3 Fix: only remove positions with zero remaining qty.
                # Previously exchange=="none" positions were removed immediately,
                # causing tp_hit/sl_hit signals to have no position to match → lost stats.
                if remaining <= 0:
                    with _pos_lock:
                        p2 = load_positions()
                        if pkey in p2:
                            p2.pop(pkey)
                            save_positions(p2)
                            write_log(f"POS_MGR_CLEANUP | {pkey} | exchange={exchange} remaining={remaining}")
                    continue

                # Позиции без биржи — только telegram-only, trailing stop не нужен
                if exchange == "none":
                    continue

                # Safety guard: если TP2/TP3 уже достигнуты, но TradingView sl_moved
                # не пришёл/не сохранился, bot сам переносит SL по конфигу BE/TRAIL.
                # Это чинит активные сделки, которые уже имеют tp2_hit/tp3_hit.
                highest_tp = _highest_tp_hit(pos)
                needs_be = highest_tp >= 2 and _truthy(pos.get("config_be_tp2")) and not _truthy(pos.get("be_active"))
                needs_trail = highest_tp >= 3 and _truthy(pos.get("config_trail_tp3")) and not _truthy(pos.get("trail_active"))
                if needs_be or needs_trail:
                    applied, _oid = _apply_bot_sl_fallback(pos, highest_tp)
                    if applied:
                        with _pos_lock:
                            p2 = load_positions()
                            if pkey in p2:
                                p2[pkey].update(pos)
                                save_positions(p2)
                        if pos.get("trade_key"):
                            touch_trade(
                                pos.get("trade_key"),
                                sl_price=pos.get("sl_price"),
                                sl_order_id=pos.get("sl_order_id", ""),
                                be_active=bool(pos.get("be_active", False)),
                                trail_active=bool(pos.get("trail_active", False)),
                                trail_sl=pos.get("trail_sl"),
                                sl_moved_count=int(pos.get("sl_moved_count") or 0),
                                last_sl_move_reason=pos.get("last_sl_move_reason", ""),
                                last_sl_moved_at=pos.get("last_sl_moved_at"),
                            )
                        continue

                # Legacy percentage trail. Default disabled; TP2/TP3 fallback above remains active.
                if not pos.get("trail_active") or TRAIL_PCT == 0.0:
                    continue
                ticker   = pos["symbol"]
                cur_sl   = pos.get("trail_sl") or pos.get("sl_price") or 0
                try:
                    price = ex_get_price(ticker, exchange)
                except Exception:
                    continue

                if pos["direction"] == "BUY":
                    new_trail = price * (1 - TRAIL_PCT)
                    if new_trail <= cur_sl:
                        continue
                else:
                    new_trail = price * (1 + TRAIL_PCT)
                    if cur_sl != 0 and new_trail >= cur_sl:
                        continue

                oid = move_sl(pos, new_trail)
                with _pos_lock:
                    p2 = load_positions()
                    if pkey in p2:
                        p2[pkey]["trail_sl"]    = new_trail
                        p2[pkey]["sl_price"]    = new_trail
                        p2[pkey]["sl_order_id"] = oid
                        save_positions(p2)

        except Exception as e:
            write_log(f"POS_MGR_ERR | {e}")
        time.sleep(120)


# ══════════════════════════════════════════════════════════════════════════════
# KEEPALIVE
# ══════════════════════════════════════════════════════════════════════════════
def _keepalive():
    time.sleep(60)
    while True:
        if RENDER_URL:
            try:
                r = requests.get(f"{RENDER_URL.rstrip('/')}/health", timeout=10)
                write_log(f"KEEPALIVE | {r.status_code}")
            except Exception as e:
                write_log(f"KEEPALIVE_ERR | {e}")
        time.sleep(600)


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM КОМАНДЫ
# ══════════════════════════════════════════════════════════════════════════════
if bot:
    def _reply(m, text: str):
        # Bug 1 Fix: use send_tg() (3 retries, requests.post) instead of
        # bot.send_message() which has no retry and fails on RemoteDisconnected.
        thread_id = getattr(m, "message_thread_id", None)
        result = send_tg(text, thread_id=thread_id, chat_id=str(m.chat.id))
        if not result:
            write_log(f"REPLY_ERR | send_tg returned empty for chat={m.chat.id}")

    @bot.message_handler(commands=["start"])
    def cmd_start(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        _reply(m, (
            "🚀 <b>Statham Trading Bot v2</b>\n\n"
            "Принимаю сигналы от TradingView,\n"
            "публикую в группу и исполняю на Bybit / BingX.\n\n"
            "👉 /help — список команд"
        ))

    @bot.message_handler(commands=["help"])
    def cmd_help(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        _reply(m, (
            "📋 <b>Команды Statham Bot v2</b>\n\n"
            "<b>📊 Статистика:</b>\n"
            "/pnl — Live P&L открытых позиций\n"
            "/stats — Win Rate\n"
            "/daily_report — дневной отчёт\n"
            "/weekly_report — недельный отчёт\n"
            "/monthly_report — месячный отчёт\n"
            "/stats_by_ticker BTCUSDT — статистика по паре\n"
            "/leaders — топ пар по WR\n"
            "/active — открытые позиции\n\n"
            "<b>🧭 Рынок:</b>\n"
            "/market — F&G + сессии одним сообщением\n"
            "/fear_greed — F&G индекс\n"
            "/sessions — торговые сессии\n"
            "/pairs — разбивка пар по биржам\n\n"
            "<b>🔧 Admin:</b>\n"
            "/balance — баланс Bybit + BingX\n"
            "/close_all — закрыть всё\n"
            "/emergency_stop — стоп сигналов\n"
            "/resume — снять стоп\n"
            "/reset_stats — сбросить статистику\n"
            "/recalc_stats — пересчёт stats из истории ← BUG#8 FIX\n"
            "/retro_fix — коррекция старых win→partial ← BUG#1 FIX\n"
            "/cleanup — удалить зависшие сделки\n"
            "/clean — очистить trades+positions\n"
            "/logs — последние строки лога\n/diagnostics — проверка всех API\n/redis_fix — переподключить Redis\n/redis_info — статус Redis + ключи\n/redis_clear_history — очистить историю\n/redis_clear_all — очистить всё Redis\n/sync — сверить позиции с биржами\n/pnl — Live P&L открытых позиций"
        ))

    @bot.message_handler(commands=["stats"])
    def cmd_stats(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        s      = load_stats()
        wins   = s.get("wins", 0)
        losses = s.get("losses", 0)
        total  = s.get("total", 0)
        # FIX: WR = (wins + partials) / (wins + partials + losses)
        history    = load_history()
        _partials_cnt = sum(1 for r in history if r.get("result") == "partial")
        wr         = calc_winrate(wins + _partials_cnt, wins + _partials_cnt + losses)
        # TP ≥ 3 из истории
        tp3plus = sum(1 for r in history if r.get("result") in ("win","partial")
                      and int(r.get("highest_tp_hit") or r.get("tp_num") or 0) >= 3)
        # P&L среднее — пропускаем нули (entry_price был null)
        pnl_vals = [r["pnl"]["pnl_pct"] for r in history
                    if r.get("pnl") and r["pnl"].get("pnl_pct") != 0.0]
        avg_pnl_all = round(sum(pnl_vals) / len(pnl_vals), 2) if pnl_vals else None
        pnl_line = ""
        if avg_pnl_all is not None:
            _s = "+" if avg_pnl_all >= 0 else ""
            pnl_line = f"\n💰 Ср. П&Л на сделку: <b>{_s}{avg_pnl_all}%</b>"
        # TP breakdown for all time
        _partials_all = [r for r in history if r.get("result") == "partial"]
        _wins_all     = [r for r in history if r.get("result") == "win"]
        _tp_all: dict = {}
        for r in _wins_all + _partials_all:
            n = int(r.get("highest_tp_hit") or r.get("tp_num") or 0)
            if n:
                _tp_all[n] = _tp_all.get(n, 0) + 1
        # ── TP разбивка ──────────────────────────────────────────────────────
        tp_breakdown = ""
        if _tp_all:
            tp_breakdown = "\n<b>Разбивка по TP:</b>\n"
            for n in sorted(_tp_all.keys()):
                tp_breakdown += f"  TP{n}: {_tp_all[n]}x\n"

        # ── SL разбивка по типу ───────────────────────────────────────────────
        _all_sl = [r for r in history if r.get("close_reason") == "sl_hit"
                   or r.get("result") in ("loss", "partial")]
        # New records have trail_active/be_active fields; old don't
        _sl_new    = [r for r in _all_sl if "trail_active" in r or "be_active" in r]
        sl_pure    = [r for r in _sl_new
                      if not r.get("trail_active") and not r.get("be_active")]
        sl_be      = [r for r in _sl_new
                      if r.get("be_active") and not r.get("trail_active")]
        sl_trail   = [r for r in _sl_new if r.get("trail_active")]
        sl_legacy  = [r for r in _all_sl
                      if "trail_active" not in r and "be_active" not in r]

        def _sl_avg(recs):
            vals = [r["pnl"]["pnl_pct"] for r in recs
                    if r.get("pnl") and r["pnl"].get("pnl_pct") != 0.0]
            if not vals: return ""
            avg = round(sum(vals)/len(vals), 1)
            s = "+" if avg >= 0 else ""
            # Positive avg for SL = BE/Trail exit in profit (correct!)
            return f"  <i>avg {s}{avg}%</i>"

        sl_breakdown = "\n<b>Разбивка по SL:</b>\n"
        if sl_pure:
            sl_breakdown += f"  ❌ Чистый SL: {len(sl_pure)}x{_sl_avg(sl_pure)}\n"
        if sl_be:
            sl_breakdown += f"  🔒 BE-стоп: {len(sl_be)}x{_sl_avg(sl_be)}\n"
        if sl_trail:
            sl_breakdown += f"  📈 Trail-стоп: {len(sl_trail)}x{_sl_avg(sl_trail)}\n"
        if sl_legacy:
            leg_v = [r["pnl"]["pnl_pct"] for r in sl_legacy
                     if r.get("pnl") and r["pnl"].get("pnl_pct") != 0.0]
            if leg_v:
                avg_leg = round(sum(leg_v)/len(leg_v), 1)
                _sg = "+" if avg_leg >= 0 else ""
                sl_breakdown += (f"  📦 Архив (до клас-ции): {len(sl_legacy)}x"
                                 f"  <i>avg {_sg}{avg_leg}% (вкл. BE/Trail)</i>\n")
            else:
                sl_breakdown += f"  📦 Архив: {len(sl_legacy)}x  <i>avg n/a</i>\n"

        _reply(m, (
            f"📊 <b>Статистика Statham Strategy</b>\n\n"
            f"{_wr_icon(wr)} Win Rate: <b>{wr}%</b>"
            f"  <i>({wins + _partials_cnt}/{wins + _partials_cnt + losses})</i>\n"
            f"🏆 Full TP закрыты: {wins}\n"
            f"🔶 Partial (TP+SL): {len(_partials_all)}\n"
            f"✅ Из них TP ≥3 хит: {tp3plus}\n"
            f"❌ Pure SL: {losses}\n"
            f"📈 Всего закрыто: <b>{total}</b>"
            f"{pnl_line}"
            f"{tp_breakdown}"
            f"{sl_breakdown}"
        ))

    @bot.message_handler(commands=["daily_report"])
    def cmd_daily(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        today = _msk().strftime("%Y-%m-%d")
        ts    = [r for r in load_history() if r.get("date_msk") == today]
        if not ts:
            _reply(m, f"📅 Сегодня ({today}) закрытых сделок нет.")
            return
        _reply(m, _build_report(ts, f"📅 <b>Дневной отчёт {today}</b>",
                                 show_last=5, show_top_tickers=False))

    @bot.message_handler(commands=["weekly_report"])
    def cmd_weekly(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        msk  = _msk()
        wk   = f"{msk.year}-W{msk.isocalendar()[1]:02d}"
        # Дата начала/конца недели (пн–вс)
        week_day   = msk.weekday()
        week_start = (msk - datetime.timedelta(days=week_day)).strftime("%d.%m")
        week_end   = (msk + datetime.timedelta(days=6 - week_day)).strftime("%d.%m.%Y")
        ts = [r for r in load_history() if r.get("week_msk") == wk]
        if not ts:
            _reply(m, f"📅 На этой неделе ({wk}) закрытых сделок нет.")
            return
        _reply(m, _build_report(
            ts,
            f"📅 <b>Недельный отчёт</b>",
            date_range=f"с {week_start} по {week_end}",
            show_last=0,
            show_top_tickers=True,
        ))

    @bot.message_handler(commands=["monthly_report"])
    def cmd_monthly(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        msk = _msk()
        mo  = f"{msk.year}-{msk.month:02d}"
        import calendar as _cal
        month_name_ru = {1:"Январь",2:"Февраль",3:"Март",4:"Апрель",
                         5:"Май",6:"Июнь",7:"Июль",8:"Август",
                         9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"}
        ts = [r for r in load_history() if r.get("date_msk","").startswith(mo)]
        if not ts:
            _reply(m, f"📅 В этом месяце ({mo}) закрытых сделок нет.")
            return
        title = f"📅 <b>Месячный отчёт — {month_name_ru.get(msk.month, mo)} {msk.year}</b>"
        _reply(m, _build_report(ts, title, show_last=0, show_top_tickers=True))

    @bot.message_handler(commands=["stats_by_ticker"])
    def cmd_stats_by_ticker(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        parts = m.text.split()
        if len(parts) < 2:
            _reply(m, "❌ Укажи тикер: /stats_by_ticker BTCUSDT")
            return
        ticker = parts[1].upper().replace(".P", "")
        ts = [r for r in load_history()
              if r.get("ticker", "").replace(".P", "").upper() == ticker]
        if not ts:
            _reply(m, f"Нет данных по {ticker}")
            return
        _reply(m, _build_report(ts, f"📊 <b>Статистика #{ticker}</b>",
                                 show_last=5, show_top_tickers=False))

    @bot.message_handler(commands=["leaders"])
    def cmd_leaders(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        by_ticker: dict = {}
        for r in load_history():
            tk = r.get("ticker", "?").replace(".P", "")
            if tk not in by_ticker:
                by_ticker[tk] = {"wins": 0, "losses": 0, "tp_counts": {}}
            d      = by_ticker[tk]
            result = r.get("result", "")
            tp_num = int(r.get("tp_num") or 0)
            highest= int(r.get("highest_tp_hit") or 0)
            if result == "win":
                d["wins"] += 1
                best = max(tp_num, highest)
                if best > 0:
                    d["tp_counts"][best] = d["tp_counts"].get(best, 0) + 1
            elif result == "loss":
                d["losses"] += 1

        if not by_ticker:
            _reply(m, "Нет данных.")
            return

        def _wr_val(d):
            t = d["wins"] + d["losses"]
            return calc_winrate(d["wins"], t) if t else -1

        scored = [(tk, d) for tk, d in by_ticker.items()
                  if d["wins"] + d["losses"] > 0]
        scored_by_wr = sorted(scored, key=lambda x: (_wr_val(x[1]), x[1]["wins"]), reverse=True)

        best  = scored_by_wr[:5]
        worst = list(reversed(scored_by_wr[-5:])) if len(scored_by_wr) > 5 else []

        medals_best  = ["🥇","🥈","🥉","4️⃣","5️⃣"]
        medals_worst = ["🔻","🔻","🔻","🔻","🔻"]

        total_wins   = sum(d["wins"]   for _, d in by_ticker.items())
        total_losses = sum(d["losses"] for _, d in by_ticker.items())
        total_all    = total_wins + total_losses
        overall_wr   = calc_winrate(total_wins, total_all)
        tickers_cnt  = len(scored)

        msk = _msk()
        week_day   = msk.weekday()
        week_start = (msk - datetime.timedelta(days=week_day)).strftime("%d.%m")
        week_end   = msk.strftime("%d.%m")

        lines = [f"📊 <b>Топ пар по Win Rate</b>",
                 f"<i>с {week_start} по {week_end}</i>\n"]

        lines.append("🏆 <b>Лучшие тикеры:</b>")
        for i, (tk, d) in enumerate(best):
            wr  = _wr_val(d)
            tot = d["wins"] + d["losses"]
            tp_str = " ".join(f"TP{n}×{c}" for n, c in sorted(d["tp_counts"].items()) if c)
            lines.append(
                f"{medals_best[i]} <b>#{tk}</b> — WR {wr:.1f}% "
                f"({d['wins']}W/{d['losses']}L из {tot})"
                + (f"\n   <i>{tp_str}</i>" if tp_str else "")
            )

        if worst:
            lines.append("\n📉 <b>Худшие тикеры:</b>")
            for i, (tk, d) in enumerate(worst):
                wr  = _wr_val(d)
                tot = d["wins"] + d["losses"]
                lines.append(
                    f"{medals_worst[i]} <b>#{tk}</b> — WR {wr:.1f}% "
                    f"({d['wins']}W/{d['losses']}L из {tot})"
                )

        lines.append(
            f"\nВсего тикеров: <b>{tickers_cnt}</b> | Сделок: <b>{total_all}</b>\n"
            f"📊 Общий WR: <b>{_wr_icon(overall_wr)} {overall_wr}%</b>  "
            f"({total_wins}✅ / {total_losses}❌)"
        )
        _reply(m, "\n".join(lines))

    @bot.message_handler(commands=["active", "trades", "positions"])
    def cmd_active(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        positions = load_positions()
        if not positions:
            _reply(m, "🔵 Нет открытых позиций."); return

        longs  = {k:v for k,v in positions.items() if v.get("direction","").upper()=="BUY"}
        shorts = {k:v for k,v in positions.items() if v.get("direction","").upper()=="SELL"}

        def _get_live_price(sym, exch):
            try:
                if exch == "bybit" and BYBIT_AVAILABLE: return bybit_get_price(sym)
                if exch == "bingx" and BINGX_AVAILABLE: return bingx_get_price(sym)
                # telegram-only: try market data without auth
                try: return bybit_get_price(sym)
                except Exception: pass
                try: return bingx_get_price(sym)
                except Exception: pass
            except Exception: pass
            return 0.0

        def _fmt_pos(pkey, pos):
            sym      = pos.get("symbol", pkey.split("_")[0])
            dirn     = pos.get("direction","?").upper()
            exch     = pos.get("exchange","?")
            lev      = int(pos.get("leverage", 10))
            entry    = float(pos.get("entry_price") or 0)
            sl       = float(pos.get("sl_price") or pos.get("trail_sl") or 0)
            tp1      = float(pos.get("tp1_price") or 0)
            remaining= float(pos.get("remaining_qty") or 0)
            created  = int(pos.get("created_at") or 0)
            trail    = pos.get("trail_active", False)

            cur = _get_live_price(sym, exch)

            pnl_pct = 0.0; pnl_usd = 0.0
            if entry > 0 and cur > 0:
                pnl_pct = ((cur-entry)/entry if dirn=="BUY" else (entry-cur)/entry)*lev*100
                # notional = entry * qty, uPNL ≈ notional * move_pct
                notional = entry * remaining
                pnl_usd  = notional * ((cur-entry)/entry if dirn=="BUY" else (entry-cur)/entry)

            dur_str  = fmt_duration(int(time.time())-created) if created else "?"
            pnl_icon = "✅" if pnl_pct>0.5 else ("❌" if pnl_pct<-1 else "➖")
            dir_icon = "🟢" if dirn=="BUY" else "🔴"
            dir_lbl  = "LONG" if dirn=="BUY" else "SHORT"
            exch_tag = f"[{exch.upper()}]" if exch!="none" else "[TG]"
            pnl_sign = "+" if pnl_pct>=0 else ""
            usd_sign = "+" if pnl_usd>=0 else ""

            flags = []
            if pos.get("tp1_hit"): flags.append("🔒BE")
            if trail:              flags.append("📈Trail")
            highest = max([n for n in range(1,7) if pos.get(f"tp{n}_hit")], default=0)
            if highest > 0:        flags.append(f"TP{highest}✓")
            flag_str = "  " + " ".join(flags) if flags else ""

            sl_dist = ""
            if sl>0 and cur>0:
                pct = abs(cur-sl)/cur*100
                sl_dist = f" | до SL: {pct:.1f}%"

            size_str = f"{remaining:.4g}" if remaining>0 else "—"
            entry_str = f"{entry:.6g}" if entry>0 else "—"
            cur_str   = f"{cur:.6g}"   if cur>0   else "—"
            sl_str    = f"{sl:.6g}"    if sl>0    else "—"
            tp1_str   = f"{tp1:.6g}"   if tp1>0   else "—"
            pnl_usd_s = f"  uPNL: <b>{usd_sign}{pnl_usd:.2f}$</b>" if entry>0 and cur>0 else ""

            return (
                f"{dir_icon} <b>#{sym}</b> {dir_lbl} {exch_tag} {lev}x{flag_str}\n"
                f"   Вход: <code>{entry_str}</code>  |  Размер: {size_str}  |  Текущая: <code>{cur_str}</code>\n"
                f"   {pnl_icon} P&L: <b>{pnl_sign}{pnl_pct:.1f}%</b>{pnl_usd_s}  |  Время: {dur_str}\n"
                f"   ⛔ SL: {sl_str}{sl_dist}  |  ✅ TP1: {tp1_str}"
            )

        lines = [f"📌 <b>Открытых позиций: {len(positions)}</b>\n"]
        if shorts:
            lines.append(f"📉 <b>SHORT позиции ({len(shorts)}):</b>")
            for pk,pos in sorted(shorts.items(), key=lambda x: x[1].get("created_at",0)):
                lines.append(_fmt_pos(pk, pos))
        if longs:
            if shorts: lines.append("")
            lines.append(f"📈 <b>LONG позиции ({len(longs)}):</b>")
            for pk,pos in sorted(longs.items(), key=lambda x: x[1].get("created_at",0)):
                lines.append(_fmt_pos(pk, pos))

        full = "\n".join(lines)
        if len(full) > 3800:
            # Split on position boundaries
            header = lines[0] + "\n"
            chunks = [header]
            cur_chunk = ""
            for l in lines[1:]:
                if len(cur_chunk)+len(l)+1 > 3600:
                    chunks[-1] += cur_chunk
                    chunks.append("")
                    cur_chunk = l + "\n"
                else:
                    cur_chunk += l + "\n"
            if cur_chunk: chunks[-1] += cur_chunk
            for ch in chunks:
                if ch.strip():
                    send_tg(ch, thread_id=getattr(m,"message_thread_id",None), chat_id=str(m.chat.id))
        else:
            _reply(m, full)

    @bot.message_handler(commands=["pairs"])
    def cmd_pairs(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        # Bybit статус
        if not BYBIT_AVAILABLE:
            bybit_status = "❌ не настроен (нет API ключей)"
        else:
            bybit_status = "🧪 TESTNET / all webhook pairs" if TESTNET else "🔴 LIVE / all webhook pairs"
        # BingX статус
        if not BINGX_AVAILABLE:
            bingx_status = "❌ не настроен (нет API ключей)"
        else:
            bingx_status = "🧪 DEMO / all webhook pairs" if BINGX_DEMO else "🔴 LIVE / all webhook pairs"

        bybit_pairs_str = "ALL webhook pairs (BYBIT_PAIRS ignored)"
        bingx_pairs_str = "ALL webhook pairs (BINGX_PAIRS ignored)"
        ps_str = "ignored/deprecated; use DEFAULT_LEVERAGE + DEFAULT_SIZE_USDT"

        _reply(m, (
            f"💱 <b>Биржи и пары</b>\n\n"
            f"<b>Bybit</b> [{bybit_status}]\n"
            f"{bybit_pairs_str}\n\n"
            f"<b>BingX</b> [{bingx_status}]\n"
            f"{bingx_pairs_str}\n\n"
            f"<b>PAIR_SETTINGS</b> (кастомные плечи/объём):\n"
            f"{ps_str}"
        ))

    @bot.message_handler(commands=["fear_greed"])
    def cmd_fear_greed(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        fg = _fetch_fg()
        if fg is None: _reply(m, "❌ Не удалось получить F&G."); return
        _reply(m, _build_fg_message(fg, "manual"))

    @bot.message_handler(commands=["sessions"])
    def cmd_sessions(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        _reply(m, _sessions_status())

    @bot.message_handler(commands=["market"])
    def cmd_market(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        fg = _fetch_fg()
        fg_line = ""
        if fg:
            v = fg["value"]
            label = fg["label"]
            fg_line = f"{_fg_emoji(v)} F&G: <b>{v}</b> — {label}\n"
        else:
            fg_line = "😐 F&G: недоступен\n"
        text = f"🌐 <b>Рынок</b>\n\n{fg_line}\n{_sessions_status()}"
        _reply(m, text)

    @bot.message_handler(commands=["balance"])
    def cmd_balance(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        parts = []
        if BYBIT_AVAILABLE:  parts.append(get_bybit_balance())
        if BINGX_AVAILABLE:  parts.append(get_bingx_balance())
        if not parts:        _reply(m, "❌ Ни одна биржа не настроена."); return
        _reply(m, "\n\n".join(parts))

    @bot.message_handler(commands=["close_all"])
    def cmd_close_all(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        with _pos_lock:
            positions = dict(load_positions())
        closed = []
        for _, pos in positions.items():
            ok, label = close_position_manually(pos, "telegram_close_all")
            if ok:
                closed.append(label)
        _reply(m, f"🚨 <b>Закрыто:</b> {', '.join(closed) or 'нет'}")
        send_signals(f"🚨 <b>АВАРИЙНОЕ ЗАКРЫТИЕ</b>\n{', '.join(closed) or 'нет'}")

    @bot.message_handler(commands=["emergency_stop"])
    def cmd_emergency_stop(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        _emergency_stop.set()
        _reply(m, "🛑 Emergency stop активирован.")

    @bot.message_handler(commands=["resume"])
    def cmd_resume(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        _emergency_stop.clear()
        _reply(m, "✅ Блокировка снята.")

    @bot.message_handler(commands=["reset_stats"])
    def cmd_reset_stats(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        save_stats({"wins": 0, "losses": 0, "total": 0})
        _reply(m, "🔄 Статистика сброшена.")

    # ══════════════════════════════════════════════════════════════════
    # BUG #8 FIX: /recalc_stats — пересчёт trade_stats из реальной истории
    # Решает проблему некорректного Redis-счётчика (partial → loss → wins=36 вместо ~90)
    # ══════════════════════════════════════════════════════════════════
    @bot.message_handler(commands=["recalc_stats"])
    def cmd_recalc_stats(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        history = load_history()
        wins_full    = 0  # result == "win" (полное TP-закрытие)
        wins_partial = 0  # result == "partial" (TP + SL)
        losses       = 0  # result == "loss" (чистый SL)
        manuals      = 0  # result == "manual"
        for r in history:
            res = r.get("result", "")
            if res == "win":
                wins_full += 1
            elif res == "partial":
                wins_partial += 1
            elif res == "loss":
                losses += 1
            elif res == "manual":
                manuals += 1
        total = wins_full + wins_partial + losses + manuals
        new_stats = {
            "wins":           wins_full + wins_partial,
            "losses":         losses,
            "total":          total,
            "_wins_full":     wins_full,
            "_wins_partial":  wins_partial,
            "_losses":        losses,
            "_manuals":       manuals,
        }
        save_stats(new_stats)
        wr = calc_winrate(wins_full + wins_partial, total)
        write_log(
            f"RECALC_STATS | from {len(history)} records | "
            f"wins_full={wins_full} partial={wins_partial} losses={losses} total={total} WR={wr}%"
        )
        _reply(m, (
            f"✅ <b>Статистика пересчитана</b> из {len(history)} записей истории\n\n"
            f"🏆 Full TP: <b>{wins_full}</b>\n"
            f"🔶 Partial (TP+SL): <b>{wins_partial}</b>\n"
            f"❌ Pure SL: <b>{losses}</b>\n"
            f"🧯 Manual: <b>{manuals}</b>\n"
            f"📈 Всего: <b>{total}</b>\n\n"
            f"📊 Win Rate: {_wr_icon(wr)} <b>{wr}%</b>"
        ))

    # ══════════════════════════════════════════════════════════════════
    # BUG #1 RETRO FIX: /retro_fix — коррекция старых "win" в истории
    # Меняет записи close_reason=sl_hit + result="win" → "partial"
    # Запускать ОДИН РАЗ после деплоя патча, затем /recalc_stats
    # ══════════════════════════════════════════════════════════════════
    @bot.message_handler(commands=["retro_fix"])
    def cmd_retro_fix(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        with _state_lock:
            history = load_history()
            fixed = 0
            for r in history:
                if (r.get("close_reason") == "sl_hit"
                        and r.get("result") == "win"
                        and int(r.get("highest_tp_hit") or r.get("tp_num") or 0) > 0):
                    r["result"] = "partial"
                    fixed += 1
            save_history(history)
        write_log(f"RETRO_FIX | fixed {fixed} records win→partial")
        _reply(m, (
            f"✅ <b>Ретро-коррекция завершена</b>\n\n"
            f"Исправлено win→partial: <b>{fixed}</b>\n"
            f"<i>(close_reason=sl_hit + highest_tp_hit>0)</i>\n\n"
            f"Теперь запусти /recalc_stats для обновления Redis-счётчика."
        ))

    @bot.message_handler(commands=["cleanup"])
    def cmd_cleanup(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        n = cleanup_old_trades()
        trades_left = len(load_trades())
        pos_left    = len(load_positions())
        _reply(m, f"🧹 Удалено <b>{n}</b> зависших/устаревших сделок.\nОсталось: trades={trades_left}, positions={pos_left}")

    @bot.message_handler(commands=["clean"])
    def cmd_clean(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        save_trades({}); save_positions({})
        _reply(m, "🧹 Активные сделки и позиции очищены.")

    @bot.message_handler(commands=["diagnostics"])
    def cmd_diagnostics(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        lines = ["🔧 <b>Диагностика API</b>\n"]
        # Telegram
        try:
            r = requests.get(f"https://api.telegram.org/bot{TG_TOKEN}/getMe", timeout=8)
            d = r.json()
            bot_name = d.get("result", {}).get("username", "?")
            lines.append(f"{'✅' if d.get('ok') else '❌'} Telegram: @{bot_name}")
        except Exception as e:
            lines.append(f"❌ Telegram: {e}")
        # Redis
        try:
            rc = _get_redis()
            if rc:
                rc.ping()
                keys = rc.keys(_REDIS_PREFIX + "*")
                lines.append(f"✅ Redis: {len(keys)} keys")
            else:
                lines.append("❌ Redis: не подключён")
        except Exception as e:
            _redis_invalidate()
            lines.append(f"❌ Redis: {e}")
        # Bybit
        if BYBIT_AVAILABLE:
            try:
                resp = bybit().get_server_time()
                lines.append(f"{'✅' if resp['retCode']==0 else '❌'} Bybit: server_time={resp.get('result',{}).get('timeSecond','?')}")
            except Exception as e:
                lines.append(f"❌ Bybit: {e}")
        else:
            lines.append("⚪ Bybit: не настроен")
        # BingX
        if BINGX_AVAILABLE:
            try:
                d = _bingx_req("GET", "/openApi/swap/v2/quote/ticker", {"symbol": "BTC-USDT"})
                lines.append(f"✅ BingX: ok, BTC={d.get('data',{}).get('lastPrice','?')}")
            except Exception as e:
                lines.append(f"❌ BingX: {e}")
        else:
            lines.append("⚪ BingX: не настроен")
        # Queue and state
        lines.append(f"\n📊 Позиций: {len(load_positions())} | Сделок: {len(load_trades())}")
        lines.append(f"📬 Очередь: {len(_signal_queue)} | 🛑 Стоп: {_emergency_stop.is_set()}")
        _reply(m, "\n".join(lines))

    @bot.message_handler(commands=["sync"])
    def cmd_sync(m):
        """Принудительная сверка позиций бота с биржами (удаляет phantom-позиции)."""
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        global _last_exchange_sync
        _last_exchange_sync = 0  # сброс → sync запустится немедленно
        before = len(load_positions())
        trades_before = len(load_trades())
        _sync_exchange_positions()
        after = len(load_positions())
        trades_after = len(load_trades())
        removed = before - after
        _reply(m, (
            f"🔄 <b>Синхронизация завершена</b>\n"
            f"Позиций до: {before} | После: {after}\n"
            f"Trades до: {trades_before} | После: {trades_after}\n"
            f"Удалено призрачных: <b>{removed}</b>"
        ))

    @bot.message_handler(commands=["redis_fix"])
    def cmd_redis_fix(m):
        """Принудительно сбрасывает Redis-клиент и переподключается."""
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        _redis_invalidate()
        rc = _get_redis()
        if rc:
            _reply(m, "✅ Redis переподключён успешно.")
        else:
            _reply(m, "❌ Redis переподключение не удалось — проверь REDIS_URL.")

    @bot.message_handler(commands=["pnl"])
    def cmd_pnl(m):
        """Быстрый Live P&L всех открытых позиций."""
        if not is_admin_user(m.from_user.id):
            _reply(m, "⛔ Нет доступа."); return
        positions = load_positions()
        if not positions:
            _reply(m, "💹 Нет открытых позиций."); return

        lines = ["💹 <b>Live P&L — Открытые позиции</b>\n"]
        pnl_list = []

        for pkey, pos in positions.items():
            sym   = pos.get("symbol", "?")
            dirn  = pos.get("direction","?").upper()
            exch  = pos.get("exchange","?")
            lev   = int(pos.get("leverage",10))
            entry = float(pos.get("entry_price") or 0)
            sl    = float(pos.get("sl_price") or pos.get("trail_sl") or 0)
            tp1   = float(pos.get("tp1_price") or 0)
            remaining = float(pos.get("remaining_qty") or 0)
            created   = int(pos.get("created_at") or 0)

            cur = 0.0
            try:
                if exch=="bybit" and BYBIT_AVAILABLE: cur = bybit_get_price(sym)
                elif exch=="bingx" and BINGX_AVAILABLE: cur = bingx_get_price(sym)
                else:
                    try: cur = bybit_get_price(sym)
                    except Exception:
                        try: cur = bingx_get_price(sym)
                        except Exception: pass
            except Exception: pass

            pnl_pct = 0.0; pnl_usd = 0.0
            if entry>0 and cur>0:
                pnl_pct = ((cur-entry)/entry if dirn=="BUY" else (entry-cur)/entry)*lev*100
                pnl_usd = entry*remaining*((cur-entry)/entry if dirn=="BUY" else (entry-cur)/entry)
                pnl_list.append(pnl_pct)

            dir_icon  = "🟢" if dirn=="BUY" else "🔴"
            pnl_icon  = "✅" if pnl_pct>0.5 else ("❌" if pnl_pct<-1 else "➖")
            pnl_sign  = "+" if pnl_pct>=0 else ""
            usd_sign  = "+" if pnl_usd>=0 else ""
            dur       = fmt_duration(int(time.time())-created) if created else "?"
            exch_tag  = exch.upper() if exch!="none" else "TG"

            sl_dist = ""
            if sl>0 and cur>0:
                sl_dist = f" (до SL: {abs(cur-sl)/cur*100:.1f}%)"

            trail_be = ""
            highest = max([n for n in range(1,7) if pos.get(f"tp{n}_hit")], default=0)
            if pos.get("trail_active"): trail_be = " 📈"
            elif pos.get("tp1_hit"):    trail_be = " 🔒"

            cur_str = f"{cur:.6g}" if cur>0 else "—"
            usd_str = f" ({usd_sign}{pnl_usd:.2f}$)" if entry>0 and cur>0 else ""
            sl_str  = f"{sl:.6g}" if sl>0 else "—"
            tp1_str = f"{tp1:.6g}" if tp1>0 else "—"

            lines.append(
                f"{dir_icon} <b>#{sym}</b> {dirn} [{exch_tag}] {lev}x{trail_be}\n"
                f"  Вход: {entry:.6g} → {cur_str}  |  {pnl_icon} <b>{pnl_sign}{pnl_pct:.1f}%</b>{usd_str}  |  {dur}\n"
                f"  ⛔ SL: {sl_str}{sl_dist}  |  ✅ TP1: {tp1_str}"
                + (f"  |  TP{highest}✓" if highest>0 else "")
            )

        if pnl_list:
            avg  = sum(pnl_list)/len(pnl_list)
            sign = "+" if avg>=0 else ""
            lines.append(f"\n📊 Позиций с ценой: {len(pnl_list)} | Ср. P&L: <b>{sign}{avg:.1f}%</b>")

        _reply(m, "\n".join(lines))

    @bot.message_handler(commands=["redis_info"])
    def cmd_redis_info(m):
        """Показывает статус Redis, список ключей и размеры данных."""
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        rc = _get_redis()
        if rc is None:
            _reply(m, "❌ Redis не подключён."); return
        try:
            rc.ping()
            keys = rc.keys(_REDIS_PREFIX + "*")
            lines = [f"✅ <b>Redis подключён</b>\n"]
            lines.append(f"🔑 Ключей: <b>{len(keys)}</b>\n")
            total_bytes = 0
            for k in sorted(keys):
                val = rc.get(k) or ""
                size = len(val)
                total_bytes += size
                # Parse counts
                try:
                    parsed = json.loads(val)
                    if isinstance(parsed, dict):
                        count = len(parsed)
                    elif isinstance(parsed, list):
                        count = len(parsed)
                    else:
                        count = 1
                    lines.append(f"  <code>{k.replace(_REDIS_PREFIX,'')}</code>: {count} записей ({size//1024 if size>1024 else size}{'KB' if size>1024 else 'B'})")
                except Exception:
                    lines.append(f"  <code>{k.replace(_REDIS_PREFIX,'')}</code>: {size}B")
            lines.append(f"\n💾 Всего данных: {total_bytes//1024}KB")
            _reply(m, "\n".join(lines))
        except Exception as e:
            _redis_invalidate()
            _reply(m, f"❌ Redis ошибка: {e}")

    @bot.message_handler(commands=["redis_clear_history"])
    def cmd_redis_clear_history(m):
        """Очищает только историю сделок (не статистику и не позиции)."""
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        rc = _get_redis()
        count = len(load_history())
        save_history([])
        _reply(m, f"🗑 История очищена. Было записей: <b>{count}</b>.\nСтатистика и позиции сохранены.")

    @bot.message_handler(commands=["redis_clear_all"])
    def cmd_redis_clear_all(m):
        """ПОЛНАЯ очистка Redis (все данные бота). Требует подтверждения."""
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        rc = _get_redis()
        if rc is None:
            _reply(m, "❌ Redis не подключён."); return
        try:
            keys = rc.keys(_REDIS_PREFIX + "*")
            if not keys:
                _reply(m, "ℹ️ Redis уже пуст."); return
            for k in keys:
                rc.delete(k)
            # Also reset in-memory
            save_stats({"wins": 0, "losses": 0, "total": 0})
            save_trades({})
            save_positions({})
            save_history([])
            _reply(m, f"🗑 <b>Redis полностью очищен.</b>\nУдалено ключей: {len(keys)}")
        except Exception as e:
            _reply(m, f"❌ Ошибка: {e}")

    @bot.message_handler(commands=["logs"])
    def cmd_logs(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
            last = "".join(lines[-30:]) if lines else "(пусто)"
            _reply(m, f"<pre>{last[:3500]}</pre>")
        except Exception as e:
            _reply(m, f"❌ {e}")


# ══════════════════════════════════════════════════════════════════════════════
# WEBHOOK — РЕГИСТРАЦИЯ TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════
_RENDER_DOMAIN = os.environ.get("RENDER_URL", "").rstrip("/")


def register_webhook():
    if not TG_TOKEN or not _RENDER_DOMAIN:
        write_log("WEBHOOK_SETUP | TG_TOKEN или RENDER_URL не заданы")
        return False
    wh_url = f"{_RENDER_DOMAIN}/webhook/telegram"
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/setWebhook",
            json={
                "url":                  wh_url,
                "drop_pending_updates": False,
                "allowed_updates":      ["message", "edited_message", "callback_query"],
            },
            timeout=10,
        )
        result = r.json()
        write_log(f"WEBHOOK_SETUP | url={wh_url} | result={result}")
        return result.get("ok", False)
    except Exception as e:
        write_log(f"WEBHOOK_SETUP_ERR | {e}")
        return False


def _register_bg():
    time.sleep(5)
    register_webhook()


def start_background_threads():
    global _bg_started
    with _bg_lock:
        if _bg_started:
            return
        threading.Thread(target=_queue_worker, daemon=True, name="queue_worker").start()
        threading.Thread(target=_scheduler, daemon=True, name="scheduler").start()
        threading.Thread(target=_keepalive, daemon=True, name="keepalive").start()
        threading.Thread(target=_register_bg, daemon=True, name="webhook_reg").start()
        # Start position manager always — needed for telegram-only positions too
        threading.Thread(target=_position_manager, daemon=True, name="pos_mgr").start()
        _bg_started = True


# ══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/")
@app.route("/health")
def health():
    return jsonify({
        "status":        "ok",
        "strategy_version": STRATEGY_VERSION,
        "schema_version": SCHEMA_VERSION,
        "tp_contract": TP_CONTRACT,
        "testnet":       TESTNET,
        "bybit":         BYBIT_AVAILABLE,
        "bingx":         BINGX_AVAILABLE,
        "bingx_demo":    BINGX_DEMO,
        "active_trades": len(load_trades()),
        "positions":     len(load_positions()),
        "emergency":     _emergency_stop.is_set(),
        "pair_gate":     "disabled",
        "execution_preference": "dual" if _dual_execution_enabled() else ("bingx" if BINGX_AVAILABLE else ("bybit" if BYBIT_AVAILABLE else "none")),
        "execution_mode": _execution_mode(),
        "execution_targets": _entry_execution_targets(),
        "bybit_pairs":   "ignored",
        "bingx_pairs":   "ignored",
        "time_msk":      _msk().strftime("%Y-%m-%d %H:%M"),
        "betterstack": {
            "enabled": BETTERSTACK_ENABLED,
            "configured": bool(BETTERSTACK_SOURCE_TOKEN and BETTERSTACK_INGEST_URL),
        },
    })


@app.route("/webhook/telegram", methods=["POST"])
def tg_webhook():
    if not bot:
        return "no bot", 400
    try:
        update = telebot.types.Update.de_json(request.get_data().decode("utf-8"))
        bot.process_new_updates([update])
    except Exception as e:
        write_log(f"TG_WEBHOOK_ERR | {e}")
    return "!", 200


@app.route("/webhook/bybit", methods=["POST"])
def bybit_webhook():
    client_ip = (request.headers.get("X-Forwarded-For","") or
                 request.remote_addr or "").split(",")[0].strip()
    if not _rate_ok(client_ip):
        write_log(f"WEBHOOK_RATE_LIMIT | ip={client_ip}")
        return jsonify({"error": "rate limit"}), 429

    raw = request.get_data(as_text=True).strip()
    if not raw:
        return jsonify({"status": "ok", "event": "ping"}), 200

    try:
        payload = request.get_json(force=True, silent=True) or json.loads(raw)
    except Exception as e:
        write_log(f"WEBHOOK_PARSE_ERR | {e}")
        return jsonify({"error": "invalid json"}), 400

    if not isinstance(payload, dict):
        return jsonify({"error": "expected json object"}), 400

    secret_from_body   = str(payload.get("secret","") or payload.get("secret_key",""))
    secret_from_header = request.headers.get("X-Secret", "")
    incoming_secret    = secret_from_body or secret_from_header

    # Проверка WEBHOOK_SECRET отключена — принимаем все POST запросы

    # Pair gate disabled: every valid entry/limit_hit webhook is eligible for exchange execution.
    write_log(f"WEBHOOK | event={payload.get('event','?')} ticker={payload.get('ticker','?')}")
    enqueue_signal(payload)
    return jsonify({"status": "queued", "event": payload.get("event","?")}), 200


@app.route("/webhook/<secret>", methods=["POST"])
def bybit_webhook_compat(secret: str):
    # Проверка секрета отключена
    raw = request.get_data(as_text=True).strip()
    if not raw:
        return jsonify({"status": "ok", "event": "ping"}), 200
    try:
        payload = request.get_json(force=True, silent=True) or json.loads(raw)
    except Exception:
        return jsonify({"error": "invalid json"}), 400
    write_log(f"WEBHOOK_COMPAT | event={payload.get('event','?')}")
    enqueue_signal(payload)
    return jsonify({"status": "queued"}), 200


@app.route("/setup")
def setup():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    return jsonify({"webhook_registered": register_webhook()})


@app.route("/debug")
def debug():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        return "<pre>" + "".join(lines[-100:]) + "</pre>", 200, {"Content-Type": "text/html"}
    except Exception as e:
        return f"<pre>Error: {e}</pre>", 200, {"Content-Type": "text/html"}


@app.route("/trades")
def trades_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    return jsonify(load_trades())


@app.route("/stats")
def stats_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    s    = load_stats()
    wins = s.get("wins", 0); losses = s.get("losses", 0); total = s.get("total", 0)
    return jsonify({"wins": wins, "losses": losses, "total": total,
                    "winrate": calc_winrate(wins, total)})


@app.route("/history")
def history_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    h = load_history()
    return jsonify({"records": len(h), "last_50": h[-50:]})


@app.route("/positions")
def positions_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    return jsonify(load_positions())


@app.route("/audit/export")
def audit_export_route():
    """Protected read-only P&L ground truth: BingX ledger + equity snapshots."""
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    if not BINGX_AVAILABLE:
        return jsonify({"error": "bingx unavailable"}), 503
    try:
        days = int(request.args.get("days", 30))
        limit = int(request.args.get("limit", 1000))
    except (TypeError, ValueError):
        return jsonify({"error": "days/limit must be integers"}), 400
    return jsonify(_bingx_audit_export(days, limit))


@app.route("/audit/schema")
def audit_schema_route():
    """Show the telemetry contract and configs already captured in Redis."""
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    positions = list(load_positions().values())
    history = load_history()
    latest = sorted(
        [r for r in history if isinstance(r, dict)],
        key=lambda r: r.get("close_time", 0), reverse=True,
    )[:20]
    return jsonify({
        "strategy_version": STRATEGY_VERSION,
        "schema_version": SCHEMA_VERSION,
        "required_entry_fields": list(ENTRY_METADATA_FIELDS),
        "open_positions": [{k: p.get(k) for k in ENTRY_METADATA_FIELDS} for p in positions],
        "latest_closed": [{k: r.get(k) for k in ENTRY_METADATA_FIELDS} for r in latest],
    })


@app.route("/close_all", methods=["POST"])
def close_all_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    with _pos_lock:
        positions = dict(load_positions())
    closed = []
    for _, pos in positions.items():
        ok, label = close_position_manually(pos, "http_close_all")
        if ok:
            closed.append(label)
    send_signals(f"🚨 <b>АВАРИЙНОЕ ЗАКРЫТИЕ</b>\nЗакрыто: {', '.join(closed) or 'нет'}")
    return jsonify({"closed": closed})


@app.route("/test_bybit")
def test_bybit_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    if not BYBIT_AVAILABLE:
        return jsonify({"error": "Bybit не настроен"}), 400
    return jsonify({"result": get_bybit_balance()})


@app.route("/test_bingx")
def test_bingx_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    if not BINGX_AVAILABLE:
        return jsonify({"error": "BingX не настроен"}), 400
    return jsonify({"result": get_bingx_balance()})


@app.route("/diagnostics")
def diagnostics_route():
    """Проверяет связь с Telegram, Bybit, BingX, Redis."""
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    result = {"time_utc": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}

    # Telegram
    try:
        r = requests.get(f"https://api.telegram.org/bot{TG_TOKEN}/getMe", timeout=8)
        d = r.json()
        result["telegram"] = {"ok": d.get("ok"), "bot": d.get("result", {}).get("username", "?")}
    except Exception as e:
        result["telegram"] = {"ok": False, "error": str(e)}

    # Redis
    try:
        rc = _get_redis()
        if rc:
            rc.ping()
            result["redis"] = {"ok": True, "keys": len(rc.keys(_REDIS_PREFIX + "*"))}
        else:
            result["redis"] = {"ok": False, "error": "not configured or connection failed"}
    except Exception as e:
        result["redis"] = {"ok": False, "error": str(e)}

    # Bybit
    if BYBIT_AVAILABLE:
        try:
            resp = bybit().get_server_time()
            result["bybit"] = {"ok": resp["retCode"] == 0,
                               "server_time": resp.get("result", {}).get("timeSecond", "?")}
        except Exception as e:
            result["bybit"] = {"ok": False, "error": str(e)}
    else:
        result["bybit"] = {"ok": False, "error": "not configured"}

    # BingX
    if BINGX_AVAILABLE:
        try:
            d = _bingx_req("GET", "/openApi/swap/v2/quote/ticker", {"symbol": "BTC-USDT"})
            result["bingx"] = {"ok": True, "price": d.get("data", {}).get("lastPrice", "?")}
        except Exception as e:
            result["bingx"] = {"ok": False, "error": str(e)}
    else:
        result["bingx"] = {"ok": False, "error": "not configured"}

    result["queue_len"]      = len(_signal_queue)
    result["active_trades"]  = len(load_trades())
    result["active_positions"] = len(load_positions())
    result["emergency_stop"] = _emergency_stop.is_set()
    write_log(f"DIAGNOSTICS | tg={result['telegram']['ok']} redis={result['redis']['ok']} bybit={result.get('bybit',{}).get('ok','n/a')} bingx={result.get('bingx',{}).get('ok','n/a')}")
    return jsonify(result)


@app.route("/redis_status")
def redis_status_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    r = _get_redis()
    if r is None:
        return jsonify({"redis": "not configured", "REDIS_URL_set": bool(os.environ.get("REDIS_URL"))}), 200
    try:
        r.ping()
        keys = r.keys(_REDIS_PREFIX + "*")
        sizes = {k: len(r.get(k) or "") for k in keys}
        return jsonify({"redis": "ok", "keys": sizes})
    except Exception as e:
        return jsonify({"redis": "error", "detail": str(e)}), 200


# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    start_background_threads()
    app.run(host="0.0.0.0", port=port, debug=False)
