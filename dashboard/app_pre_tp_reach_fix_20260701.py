"""
Statham Bot — Trading Dashboard v1.0
=====================================
Self-contained Flask app. Reads data directly from Upstash Redis.
Deploy as a separate service on Render.

Environment variables:
  REDIS_URL          — rediss://default:...@...upstash.io:6379
  DASHBOARD_SECRET   — secret token for access (set in Render)
  PORT               — port (default 10000, Render sets this automatically)
  TG_TOKEN           — Telegram bot token (for admin notifications)
  TG_ADMIN_ID        — Telegram user ID to send PnL alerts
  PNL_ALERT_THRESHOLD — daily P&L drop % to trigger alert (default -5.0)
"""

from __future__ import annotations
import json, os, time, math, io, csv
from datetime import datetime, timezone, timedelta
from functools import wraps
from flask import Flask, request, jsonify, Response, abort

# ── Redis ──────────────────────────────────────────────────────────────────────
try:
    import redis as _redis_lib
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

app = Flask(__name__)

REDIS_URL          = os.environ.get("REDIS_URL", "").strip()
DASHBOARD_SECRET   = os.environ.get("DASHBOARD_SECRET", "").strip()
TG_TOKEN           = os.environ.get("TG_TOKEN", "").strip()
TG_ADMIN_ID        = os.environ.get("TG_ADMIN_ID", "").strip()
PNL_ALERT_THRESHOLD = float(os.environ.get("PNL_ALERT_THRESHOLD", "-5.0"))
_REDIS_PREFIX      = "statham:"

_redis_client = None
MSK_TZ = timezone(timedelta(hours=3))

def _get_redis():
    global _redis_client
    if not _REDIS_AVAILABLE or not REDIS_URL:
        return None
    if _redis_client is not None:
        try:
            _redis_client.ping()
            return _redis_client
        except Exception:
            _redis_client = None
    try:
        client = _redis_lib.from_url(
            REDIS_URL, decode_responses=True,
            socket_timeout=10, socket_connect_timeout=10,
            retry_on_timeout=True,
        )
        client.ping()
        _redis_client = client
    except Exception as e:
        _redis_client = None
    return _redis_client

def redis_get(key: str):
    r = _get_redis()
    if not r:
        return None
    try:
        val = r.get(_REDIS_PREFIX + key)
        return json.loads(val) if val else None
    except Exception:
        return None

# ── Auth ───────────────────────────────────────────────────────────────────────
def require_secret(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if DASHBOARD_SECRET:
            token = request.args.get("secret") or request.headers.get("X-Dashboard-Secret", "")
            if token != DASHBOARD_SECRET:
                abort(403)
        return f(*args, **kwargs)
    return decorated

# ── Helpers ────────────────────────────────────────────────────────────────────
def _msk_now():
    return datetime.now(MSK_TZ)

def _ts_to_msk(ts):
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(MSK_TZ).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""

def _period_cutoff(period: str) -> float:
    now = time.time()
    mapping = {
        "1d": now - 86400,
        "2d": now - 2 * 86400,
        "3d": now - 3 * 86400,
        "1w": now - 7 * 86400,
        "1m": now - 30 * 86400,
        "all": 0,
    }
    return mapping.get(period, 0)

def _filter_history(history: list, period: str, direction: str,
                    strategy_version: str = "", entry_mode: str = "") -> list:
    cutoff = _period_cutoff(period)
    result = []
    for r in history if isinstance(history, list) else []:
        if not isinstance(r, dict):
            continue
        try:
            ct = float(r.get("close_time", 0) or 0)
        except (TypeError, ValueError):
            continue
        if ct < cutoff:
            continue
        dr = (r.get("direction") or "").upper()
        if direction == "LONG" and dr != "BUY":
            continue
        if direction == "SHORT" and dr != "SELL":
            continue
        if strategy_version and str(r.get("strategy_version") or "legacy") != strategy_version:
            continue
        if entry_mode and str(r.get("entry_mode") or "").upper() != entry_mode.upper():
            continue
        result.append(r)
    return result

def _today_msk_start() -> float:
    now = _msk_now()
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight.timestamp()

def _safe_pnl(record: dict) -> float | None:
    """Return a finite leveraged trade return, including valid zero values."""
    try:
        value = (record.get("pnl") or {}).get("pnl_pct")
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError, AttributeError):
        return None

def _record_highest_tp(record: dict) -> int:
    """Combine execution and summary evidence without trusting legacy tp_num."""
    candidates = []
    pnl = record.get("pnl")
    if isinstance(pnl, dict):
        hits = pnl.get("tps_hit")
        if isinstance(hits, list):
            for value in hits:
                try:
                    candidates.append(int(value))
                except (TypeError, ValueError):
                    pass
        if "highest_tp" in pnl:
            try:
                candidates.append(int(pnl.get("highest_tp") or 0))
            except (TypeError, ValueError):
                pass
    if "highest_tp_hit" in record:
        try:
            candidates.append(int(record.get("highest_tp_hit") or 0))
        except (TypeError, ValueError):
            pass
    elif "tp_num" in record:
        try:
            candidates.append(int(record.get("tp_num") or 0))
        except (TypeError, ValueError):
            pass
    return max(0, max(candidates, default=0))

def _position_items(positions) -> list[tuple[str, dict]]:
    """Accept the live Redis mapping and exported list representation."""
    if isinstance(positions, dict):
        return [(str(k), v) for k, v in positions.items() if isinstance(v, dict)]
    if isinstance(positions, list):
        return [(str(i), v) for i, v in enumerate(positions) if isinstance(v, dict)]
    return []

def _calc_stats(trades: list) -> dict:
    wins     = [r for r in trades if r.get("result") == "win"]
    losses   = [r for r in trades if r.get("result") == "loss"]
    partials = [r for r in trades if r.get("result") == "partial"]
    manuals  = [r for r in trades if r.get("result") == "manual"]
    total = len(wins) + len(losses) + len(partials)
    pnl_pairs = [(r, _safe_pnl(r)) for r in trades]
    pnl_vals = [p for _, p in pnl_pairs if p is not None]
    profitable = sum(1 for _, p in pnl_pairs if p is not None and p > 0.0)
    net_losses = sum(1 for _, p in pnl_pairs if p is not None and p < 0.0)
    breakeven = sum(1 for _, p in pnl_pairs if p == 0.0)
    pnl_scored = len(pnl_vals)
    wr = round(profitable / pnl_scored * 100, 1) if pnl_scored else 0.0

    total_pnl = round(sum(pnl_vals), 2) if pnl_vals else 0.0
    avg_pnl = round(sum(pnl_vals) / len(pnl_vals), 2) if pnl_vals else 0.0

    # tp_counts means "reached this level" (cumulative), not an exclusive
    # highest-TP bucket. This makes TP1 >= TP2 >= TP3 >= TP4 by definition.
    tp_counts = {n: 0 for n in range(1, 5)}
    highest_tp_counts = {}
    sl_records = [r for r in trades if r.get("close_reason") == "sl_hit"]
    sl_count = len(sl_records)
    pure_sl_count = sum(1 for r in sl_records if _record_highest_tp(r) == 0)
    be_count = sum(1 for r in sl_records if r.get("be_active"))

    for r in wins + partials:
        highest = min(4, _record_highest_tp(r))
        if highest > 0:
            highest_tp_counts[highest] = highest_tp_counts.get(highest, 0) + 1
            for n in range(1, highest + 1):
                tp_counts[n] += 1

    # P&L by day for chart
    daily_pnl: dict = {}
    for r in trades:
        ct = r.get("close_time", 0)
        if not ct:
            continue
        day = datetime.fromtimestamp(ct, tz=timezone.utc).astimezone(MSK_TZ).strftime("%Y-%m-%d")
        pnl = _safe_pnl(r)
        if pnl is not None:
            daily_pnl[day] = round(daily_pnl.get(day, 0) + pnl, 2)

    # Today stats
    today_cutoff = _today_msk_start()
    today_trades = [r for r in trades if (r.get("close_time") or 0) >= today_cutoff]
    today_pnl_vals = [p for p in (_safe_pnl(r) for r in today_trades) if p is not None]
    today_pnl = round(sum(today_pnl_vals), 2) if today_pnl_vals else 0.0
    today_wins = sum(1 for r in today_trades if (_safe_pnl(r) or 0.0) > 0.0)
    today_losses = sum(1 for r in today_trades if (_safe_pnl(r) or 0.0) < 0.0)
    today_tp = {n: 0 for n in range(1, 5)}
    today_sl_records = [r for r in today_trades if r.get("close_reason") == "sl_hit"]
    today_sl = len(today_sl_records)
    today_be = sum(1 for r in today_sl_records if r.get("be_active"))
    for r in today_trades:
        if r.get("result") in ("win", "partial"):
            highest = min(4, _record_highest_tp(r))
            for n in range(1, highest + 1):
                today_tp[n] += 1

    negative_partials = sum(1 for r, p in pnl_pairs if r.get("result") == "partial" and p is not None and p <= 0.0)
    positive_losses = sum(1 for r, p in pnl_pairs if r.get("result") == "loss" and p is not None and p > 0.0)
    legacy_tp_above_4 = sum(1 for r in trades if int(r.get("highest_tp_hit") or 0) > 4)
    tp_summary_disagreements = sum(
        1 for r in trades
        if int(r.get("highest_tp_hit") or 0) != int((r.get("pnl") or {}).get("highest_tp") or 0)
        and isinstance(r.get("pnl"), dict)
    )
    terminal_ids = [
        str(r.get("instance_id") or r.get("trade_id") or "")
        for r in trades
        if r.get("instance_id") or r.get("trade_id")
    ]
    duplicate_terminal_events = len(terminal_ids) - len(set(terminal_ids))
    version_counts = {}
    entry_mode_counts = {}
    amd_phase_counts = {}
    for r in trades:
        version = str(r.get("strategy_version") or "legacy")
        version_counts[version] = version_counts.get(version, 0) + 1
        mode = str(r.get("entry_mode") or "unknown")
        entry_mode_counts[mode] = entry_mode_counts.get(mode, 0) + 1
        phase = str(r.get("amd_phase") or "unknown")
        amd_phase_counts[phase] = amd_phase_counts.get(phase, 0) + 1
    quality = {
        "pnl_known": pnl_scored,
        "pnl_missing": len(trades) - pnl_scored,
        "negative_partials": negative_partials,
        "positive_losses": positive_losses,
        "legacy_tp_above_4": legacy_tp_above_4,
        "tp_summary_disagreements": tp_summary_disagreements,
        "duplicate_terminal_events": duplicate_terminal_events,
        "strategy_versions": version_counts,
        "entry_modes": entry_mode_counts,
        "amd_phases": amd_phase_counts,
    }

    return {
        "total": total + len(manuals),
        "wins": len(wins),
        "losses": len(losses),
        "partials": len(partials),
        "manuals": len(manuals),
        "profitable": profitable,
        "net_losses": net_losses,
        "breakeven": breakeven,
        "pnl_scored": pnl_scored,
        "win_rate": wr,
        "pnl_sum_pct": total_pnl,
        "total_pnl": total_pnl,
        "avg_pnl": avg_pnl,
        "tp_counts": tp_counts,
        "highest_tp_counts": highest_tp_counts,
        "sl_count": sl_count,
        "pure_sl_count": pure_sl_count,
        "be_count": be_count,
        "daily_pnl": dict(sorted(daily_pnl.items())),
        "today_pnl": today_pnl,
        "today_wins": today_wins,
        "today_losses": today_losses,
        "today_tp": today_tp,
        "today_sl": today_sl,
        "today_be": today_be,
        "today_total": len(today_trades),
        "quality": quality,
    }

def _build_breakdown(history: list, positions=None) -> list:
    """Build Long/Short/Both breakdown rows."""
    rows = []
    for label, dr_filter in [("LONG", "BUY"), ("SHORT", "SELL"), ("BOTH", None)]:
        filtered = [r for r in history if dr_filter is None or (r.get("direction") or "").upper() == dr_filter]
        s = _calc_stats(filtered)
        open_count = sum(
            1 for _, p in _position_items(positions)
            if dr_filter is None or (p.get("direction") or "").upper() == dr_filter
        )
        rows.append({"label": label, "open_count": open_count, **s})
    return rows

# ── API Endpoints ──────────────────────────────────────────────────────────────
@app.route("/stats")
@app.route("/api/stats")
@require_secret
def api_stats():
    period    = request.args.get("period", "all")
    direction = request.args.get("direction", "BOTH").upper()
    strategy_version = request.args.get("strategy_version", "").strip()
    entry_mode = request.args.get("entry_mode", "").strip()

    history = redis_get("trade_history") or []
    filtered = _filter_history(history, period, direction, strategy_version, entry_mode)
    stats = _calc_stats(filtered)
    positions = redis_get("positions") or {}
    position_items = _position_items(positions)
    breakdown = _build_breakdown(filtered, positions)
    open_longs  = sum(1 for _, p in position_items if (p.get("direction") or "").upper() == "BUY")
    open_shorts = sum(1 for _, p in position_items if (p.get("direction") or "").upper() == "SELL")

    return jsonify({
        "stats": stats,
        "breakdown": breakdown,
        "open_longs": open_longs,
        "open_shorts": open_shorts,
        "open_total": len(position_items),
        "strategy_version_filter": strategy_version or None,
        "entry_mode_filter": entry_mode or None,
    })

@app.route("/api/positions")
@require_secret
def api_positions():
    positions = redis_get("positions") or {}
    result = []
    for pkey, pos in _position_items(positions):
        result.append({
            "pkey": pkey,
            "ticker": pos.get("symbol") or pos.get("ticker", ""),
            "direction": (pos.get("direction") or "").upper(),
            "entry_price": pos.get("entry_price"),
            "sl_price": pos.get("sl_price"),
            "tp1": pos.get("tp1_price"),
            "tp2": pos.get("tp2_price"),
            "tp3": pos.get("tp3_price"),
            "tp4": pos.get("tp4_price"),
            "leverage": pos.get("leverage"),
            "timeframe": pos.get("timeframe"),
            "exchange": pos.get("exchange"),
            "trade_mode": pos.get("trade_mode"),
            "is_strong": pos.get("is_strong", False),
            "trail_active": pos.get("trail_active", False),
            "trail_sl": pos.get("trail_sl"),
            "tp1_hit": pos.get("tp1_hit", False),
            "be_active": pos.get("be_active", False),
            "strategy_version": pos.get("strategy_version", "legacy"),
            "schema_version": pos.get("schema_version", 1),
            "tp_contract": pos.get("tp_contract", "legacy"),
            "created_at": pos.get("created_at"),
            "created_at_fmt": _ts_to_msk(pos.get("created_at")),
            "remaining_qty": pos.get("remaining_qty"),
            "total_qty": pos.get("total_qty"),
            # Extended metadata (if patched)
            "entry_mode": pos.get("entry_mode"),
            "style": pos.get("style"),
            "pattern": pos.get("pattern"),
            "margin": pos.get("margin"),
            "fib_level": pos.get("fib_level"),
            "wyckoff_phase": pos.get("wyckoff_phase"),
            "amd_phase": pos.get("amd_phase"),
            "score": pos.get("score"),
            "confirmations": pos.get("confirmations"),
            "atr_pct": pos.get("atr_pct"),
            "tbs": pos.get("tbs"),
            "smc": pos.get("smc"),
            "ict": pos.get("ict"),
            "strategy": pos.get("strategy"),
        })
    # Sort: BUY first, then SELL, then by created_at desc
    result.sort(key=lambda x: (x["direction"] != "BUY", -(x.get("created_at") or 0)))
    return jsonify({"positions": result})

@app.route("/api/history")
@require_secret
def api_history():
    period    = request.args.get("period", "1w")
    direction = request.args.get("direction", "BOTH").upper()
    page      = int(request.args.get("page", 1))
    per_page  = int(request.args.get("per_page", 50))
    strategy_version = request.args.get("strategy_version", "").strip()
    entry_mode = request.args.get("entry_mode", "").strip()

    history = redis_get("trade_history") or []
    filtered = _filter_history(history, period, direction, strategy_version, entry_mode)
    # Sort newest first
    filtered.sort(key=lambda r: r.get("close_time", 0), reverse=True)

    total = len(filtered)
    paginated = filtered[(page - 1) * per_page: page * per_page]

    rows = []
    for r in paginated:
        pnl_data = r.get("pnl") or {}
        rows.append({
            "strategy_version": r.get("strategy_version", "legacy"),
            "schema_version": r.get("schema_version", 1),
            "tp_contract": r.get("tp_contract", "legacy"),
            "ticker": (r.get("ticker") or "").replace(".P", ""),
            "direction": (r.get("direction") or "").upper(),
            "result": r.get("result", ""),
            "close_reason": r.get("close_reason", ""),
            "highest_tp_hit": r.get("highest_tp_hit", 0),
            "tp_num": r.get("tp_num", 0),
            "entry_price": r.get("entry_price"),
            "sl_price": r.get("sl_price"),
            "pnl_pct": pnl_data.get("pnl_pct"),
            "tp_pnl_pct": pnl_data.get("tp_pnl_pct"),
            "sl_pnl_pct": pnl_data.get("sl_pnl_pct"),
            "duration_sec": r.get("duration_sec", 0),
            "entry_time": r.get("entry_time"),
            "close_time": r.get("close_time"),
            "entry_time_fmt": _ts_to_msk(r.get("entry_time")),
            "close_time_fmt": _ts_to_msk(r.get("close_time")),
            "timeframe": r.get("timeframe", ""),
            "exchange": r.get("exchange", ""),
            "trade_mode": r.get("trade_mode", ""),
            "is_strong": r.get("is_strong", False),
            "trail_active": r.get("trail_active", False),
            "be_active": r.get("be_active", False),
            # Extended metadata
            "entry_mode": r.get("entry_mode"),
            "style": r.get("style"),
            "pattern": r.get("pattern"),
            "timeframe_tf": r.get("timeframe"),
            "leverage": r.get("leverage"),
            "margin": r.get("margin"),
            "fib_level": r.get("fib_level"),
            "wyckoff_phase": r.get("wyckoff_phase"),
            "amd_phase": r.get("amd_phase"),
            "score": r.get("score"),
            "confirmations": r.get("confirmations"),
            "atr_pct": r.get("atr_pct"),
            "tbs": r.get("tbs"),
            "smc": r.get("smc"),
            "ict": r.get("ict"),
            "strategy": r.get("strategy"),
        })

    return jsonify({"rows": rows, "total": total, "page": page, "per_page": per_page})

@app.route("/api/export")
@require_secret
def api_export():
    fmt       = request.args.get("fmt", "csv")
    tab       = request.args.get("tab", "history")
    period    = request.args.get("period", "all")
    direction = request.args.get("direction", "BOTH").upper()
    strategy_version = request.args.get("strategy_version", "").strip()

    if tab == "positions":
        positions = redis_get("positions") or {}
        data = [p for _, p in _position_items(positions)]
    else:
        history = redis_get("trade_history") or []
        data = _filter_history(history, period, direction, strategy_version)
        data.sort(key=lambda r: r.get("close_time", 0), reverse=True)

    if fmt == "json":
        resp = Response(
            json.dumps(data, ensure_ascii=False, indent=2),
            mimetype="application/json",
        )
        resp.headers["Content-Disposition"] = f"attachment; filename=statham_{tab}_{period}.json"
        return resp

    # CSV
    if not data:
        return Response("No data", mimetype="text/plain")

    output = io.StringIO()
    # Flatten pnl sub-dict
    flat_data = []
    for r in data:
        row = dict(r)
        pnl = row.pop("pnl", {}) or {}
        for k, v in pnl.items():
            row[f"pnl_{k}"] = v
        flat_data.append(row)

    all_keys = []
    seen = set()
    for row in flat_data:
        for k in row.keys():
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    writer = csv.DictWriter(output, fieldnames=all_keys, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(flat_data)

    resp = Response(
        "\ufeff" + output.getvalue(),  # BOM for Excel
        mimetype="text/csv; charset=utf-8-sig",
    )
    resp.headers["Content-Disposition"] = f"attachment; filename=statham_{tab}_{period}.csv"
    return resp

@app.route("/health")
def health():
    return jsonify({"ok": True, "redis": _get_redis() is not None})

@app.route("/")
def index():
    secret = DASHBOARD_SECRET or ""
    return DASHBOARD_HTML.replace("__SECRET__", secret)

# ── HTML Dashboard ─────────────────────────────────────────────────────────────
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Statham Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root {
    --bg:       #0a0c10;
    --bg2:      #111318;
    --bg3:      #181b22;
    --border:   #22262f;
    --text:     #e2e8f0;
    --muted:    #64748b;
    --accent:   #6366f1;
    --accent2:  #818cf8;
    --green:    #10b981;
    --red:      #ef4444;
    --yellow:   #f59e0b;
    --cyan:     #06b6d4;
    --purple:   #a855f7;
    --font:     'Inter', 'Segoe UI', system-ui, sans-serif;
    --mono:     'JetBrains Mono', 'Fira Code', monospace;
    --radius:   10px;
    --radius-sm:6px;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--font);
    font-size: 14px;
    min-height: 100vh;
  }
  /* ── Layout ── */
  .shell { display: flex; flex-direction: column; min-height: 100vh; }
  .topbar {
    display: flex; align-items: center; gap: 16px;
    padding: 12px 24px;
    background: var(--bg2);
    border-bottom: 1px solid var(--border);
    position: sticky; top: 0; z-index: 100;
  }
  .logo { font-size: 18px; font-weight: 700; color: var(--accent2); letter-spacing: -0.5px; flex: 1; }
  .logo span { color: var(--muted); font-weight: 400; font-size: 12px; margin-left: 6px; }
  .refresh-info { font-size: 11px; color: var(--muted); }
  .main { display: flex; flex: 1; }
  .sidebar {
    width: 200px; flex-shrink: 0;
    background: var(--bg2);
    border-right: 1px solid var(--border);
    padding: 20px 0;
    display: flex; flex-direction: column; gap: 2px;
  }
  .nav-item {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 20px;
    cursor: pointer; border-radius: 0;
    color: var(--muted);
    font-size: 13px; font-weight: 500;
    transition: all 0.15s;
    border-left: 3px solid transparent;
  }
  .nav-item:hover { color: var(--text); background: var(--bg3); }
  .nav-item.active { color: var(--accent2); background: var(--bg3); border-left-color: var(--accent); }
  .nav-icon { font-size: 16px; width: 20px; text-align: center; }
  .content { flex: 1; padding: 24px; overflow: auto; }

  /* ── Filters bar ── */
  .filters {
    display: flex; align-items: center; gap: 10px;
    margin-bottom: 20px; flex-wrap: wrap;
  }
  .filter-group { display: flex; gap: 4px; }
  .filter-btn {
    padding: 6px 12px; border-radius: var(--radius-sm);
    border: 1px solid var(--border);
    background: var(--bg2); color: var(--muted);
    cursor: pointer; font-size: 12px; font-weight: 500;
    transition: all 0.15s;
  }
  .filter-btn:hover { border-color: var(--accent); color: var(--text); }
  .filter-btn.active { background: var(--accent); border-color: var(--accent); color: #fff; }
  .filter-label { font-size: 11px; color: var(--muted); margin-right: 4px; text-transform: uppercase; letter-spacing: 0.5px; }
  .filter-sep { width: 1px; background: var(--border); height: 28px; margin: 0 6px; }

  /* ── KPI cards ── */
  .kpi-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 12px; margin-bottom: 20px; }
  .kpi-card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 16px;
  }
  .kpi-label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
  .kpi-value { font-size: 26px; font-weight: 700; font-family: var(--mono); line-height: 1; }
  .kpi-sub { font-size: 11px; color: var(--muted); margin-top: 4px; }
  .green { color: var(--green); }
  .red   { color: var(--red); }
  .yellow{ color: var(--yellow); }
  .cyan  { color: var(--cyan); }
  .purple{ color: var(--purple); }
  .accent{ color: var(--accent2); }

  /* ── TP badges ── */
  .tp-row { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 20px; }
  .tp-badge {
    padding: 6px 14px;
    border-radius: 20px;
    font-size: 12px; font-weight: 600;
    display: flex; align-items: center; gap: 6px;
  }
  .tp-badge.tp1 { background: #052e16; color: #34d399; border: 1px solid #065f46; }
  .tp-badge.tp2 { background: #022c22; color: #10b981; border: 1px solid #064e3b; }
  .tp-badge.tp3 { background: #0f2a1a; color: #6ee7b7; border: 1px solid #065f46; }
  .tp-badge.tp4 { background: #1a3a20; color: #a7f3d0; border: 1px solid #065f46; }
  .tp-badge.tp5 { background: #163020; color: #d1fae5; border: 1px solid #065f46; }
  .tp-badge.tp6 { background: #0d2010; color: #ecfdf5; border: 1px solid #065f46; }
  .tp-badge.sl  { background: #2d0a0a; color: #fca5a5; border: 1px solid #7f1d1d; }
  .tp-badge.be  { background: #1c1a05; color: #fde68a; border: 1px solid #78350f; }

  /* ── Chart ── */
  .chart-card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
    margin-bottom: 20px;
  }
  .chart-title { font-size: 13px; font-weight: 600; color: var(--muted); margin-bottom: 16px; text-transform: uppercase; letter-spacing: 0.5px; }
  .chart-wrap { position: relative; height: 220px; }

  /* ── Breakdown table ── */
  .section-title { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px; font-weight: 600; }
  .breakdown-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 20px; }
  .breakdown-card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 16px;
  }
  .breakdown-title {
    font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px;
    margin-bottom: 14px; padding-bottom: 10px;
    border-bottom: 1px solid var(--border);
  }
  .breakdown-title.long { color: var(--green); }
  .breakdown-title.short { color: var(--red); }
  .breakdown-title.both { color: var(--accent2); }
  .brow { display: flex; justify-content: space-between; align-items: center; padding: 5px 0; font-size: 12px; border-bottom: 1px solid #1a1d24; }
  .brow:last-child { border-bottom: none; }
  .brow-label { color: var(--muted); }
  .brow-val { font-weight: 600; font-family: var(--mono); }

  /* ── Positions table ── */
  .table-wrap { overflow-x: auto; border-radius: var(--radius); border: 1px solid var(--border); }
  table { width: 100%; border-collapse: collapse; }
  th {
    background: var(--bg3); text-align: left;
    padding: 10px 12px; font-size: 11px; font-weight: 600;
    color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px;
    border-bottom: 1px solid var(--border); white-space: nowrap;
  }
  td {
    padding: 10px 12px; border-bottom: 1px solid var(--border);
    font-size: 12px; white-space: nowrap;
    background: var(--bg2);
  }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: var(--bg3); }
  .dir-long { color: var(--green); font-weight: 700; }
  .dir-short { color: var(--red); font-weight: 700; }
  .tag {
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.4px;
  }
  .tag-strong { background: #1e1b4b; color: #a5b4fc; }
  .tag-normal { background: #1e2a1e; color: #86efac; }
  .tag-be { background: #1c1a05; color: #fde68a; }
  .tag-trail { background: #1a0f2e; color: #c084fc; }
  .tag-sl { background: #2d0a0a; color: #fca5a5; }
  .tag-win { background: #052e16; color: #34d399; }
  .tag-partial { background: #1a2e10; color: #86efac; }
  .tag-loss { background: #2d0a0a; color: #fca5a5; }
  .tag-manual { background: #1a1a2e; color: #818cf8; }
  .mono { font-family: var(--mono); }
  .pnl-pos { color: var(--green); font-family: var(--mono); font-weight: 600; }
  .pnl-neg { color: var(--red);   font-family: var(--mono); font-weight: 600; }
  .pnl-zero{ color: var(--muted); font-family: var(--mono); }

  /* ── Pagination ── */
  .pagination { display: flex; gap: 8px; align-items: center; margin-top: 16px; justify-content: flex-end; }
  .page-btn {
    padding: 6px 14px; border-radius: var(--radius-sm);
    border: 1px solid var(--border); background: var(--bg2);
    color: var(--text); cursor: pointer; font-size: 12px;
    transition: all 0.15s;
  }
  .page-btn:hover { border-color: var(--accent); }
  .page-btn:disabled { opacity: 0.4; cursor: default; }
  .page-info { font-size: 12px; color: var(--muted); }

  /* ── Export bar ── */
  .export-bar { display: flex; gap: 8px; margin-bottom: 16px; justify-content: flex-end; }
  .export-btn {
    padding: 6px 14px; border-radius: var(--radius-sm);
    border: 1px solid var(--border); background: var(--bg2);
    color: var(--muted); cursor: pointer; font-size: 12px;
    transition: all 0.15s;
    text-decoration: none; display: inline-flex; align-items: center; gap: 5px;
  }
  .export-btn:hover { border-color: var(--accent); color: var(--text); }

  /* ── Loading / Empty ── */
  .loading { text-align: center; color: var(--muted); padding: 40px; font-size: 13px; }
  .empty { text-align: center; color: var(--muted); padding: 40px; font-size: 13px; }
  .spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--border); border-top-color: var(--accent); border-radius: 50%; animation: spin 0.8s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* ── Tabs ── */
  .tab-content { display: none; }
  .tab-content.active { display: block; }

  /* ── Duration ── */
  .dur { color: var(--muted); font-size: 11px; }

  /* ── Scrollbar ── */
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: var(--bg); }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

  @media (max-width: 900px) {
    .breakdown-grid { grid-template-columns: 1fr; }
    .sidebar { width: 56px; }
    .nav-item span { display: none; }
  }
</style>
</head>
<body>
<div class="shell">
  <!-- Top bar -->
  <div class="topbar">
    <div class="logo">⚡ Statham <span>Trading Dashboard</span></div>
    <div class="refresh-info" id="lastRefresh">—</div>
    <button class="filter-btn" onclick="refreshAll()" style="padding:6px 14px;">↻ Обновить</button>
  </div>

  <div class="main">
    <!-- Sidebar -->
    <div class="sidebar">
      <div class="nav-item active" onclick="switchTab('stats')" id="nav-stats">
        <span class="nav-icon">📊</span><span>Statistics</span>
      </div>
      <div class="nav-item" onclick="switchTab('positions')" id="nav-positions">
        <span class="nav-icon">📈</span><span>Positions</span>
      </div>
      <div class="nav-item" onclick="switchTab('history')" id="nav-history">
        <span class="nav-icon">📜</span><span>History</span>
      </div>
    </div>

    <!-- Content -->
    <div class="content">

      <!-- ══ TAB: STATISTICS ══ -->
      <div class="tab-content active" id="tab-stats">
        <!-- Filters -->
        <div class="filters">
          <span class="filter-label">Period</span>
          <div class="filter-group" id="period-btns">
            <button class="filter-btn" data-val="1d" onclick="setPeriod('1d',this)">1д</button>
            <button class="filter-btn" data-val="2d" onclick="setPeriod('2d',this)">2д</button>
            <button class="filter-btn" data-val="3d" onclick="setPeriod('3d',this)">3д</button>
            <button class="filter-btn" data-val="1w" onclick="setPeriod('1w',this)">1н</button>
            <button class="filter-btn" data-val="1m" onclick="setPeriod('1m',this)">1м</button>
            <button class="filter-btn active" data-val="all" onclick="setPeriod('all',this)">ALL</button>
          </div>
          <div class="filter-sep"></div>
          <span class="filter-label">Direction</span>
          <div class="filter-group" id="dir-btns">
            <button class="filter-btn" data-val="LONG" onclick="setDirection('LONG',this)">LONG</button>
            <button class="filter-btn" data-val="SHORT" onclick="setDirection('SHORT',this)">SHORT</button>
            <button class="filter-btn active" data-val="BOTH" onclick="setDirection('BOTH',this)">BOTH</button>
          </div>
        </div>

        <!-- KPI Cards -->
        <div class="kpi-grid" id="kpi-grid">
          <div class="loading"><span class="spinner"></span>Загрузка...</div>
        </div>

        <!-- TP/SL badges -->
        <div id="tp-row" class="tp-row"></div>

        <!-- P&L Chart -->
        <div class="chart-card">
          <div class="chart-title">📉 Σ доходностей сделок по дням (не portfolio equity)</div>
          <div class="chart-wrap"><canvas id="pnlChart"></canvas></div>
        </div>

        <!-- Breakdown -->
        <div class="section-title">📋 Long / Short / Both breakdown</div>
        <div class="breakdown-grid" id="breakdown-grid">
          <div class="loading"><span class="spinner"></span></div>
        </div>
      </div>

      <!-- ══ TAB: POSITIONS ══ -->
      <div class="tab-content" id="tab-positions">
        <div class="filters">
          <span class="filter-label">Direction</span>
          <div class="filter-group" id="pos-dir-btns">
            <button class="filter-btn" data-val="LONG" onclick="setPosDir('LONG',this)">LONG</button>
            <button class="filter-btn" data-val="SHORT" onclick="setPosDir('SHORT',this)">SHORT</button>
            <button class="filter-btn active" data-val="BOTH" onclick="setPosDir('BOTH',this)">BOTH</button>
          </div>
          <div style="flex:1"></div>
          <a class="export-btn" onclick="exportData('positions','csv')">⬇ CSV</a>
          <a class="export-btn" onclick="exportData('positions','json')">⬇ JSON</a>
        </div>
        <div class="table-wrap" id="positions-table">
          <div class="loading"><span class="spinner"></span>Загрузка позиций...</div>
        </div>
      </div>

      <!-- ══ TAB: HISTORY ══ -->
      <div class="tab-content" id="tab-history">
        <div class="filters">
          <span class="filter-label">Period</span>
          <div class="filter-group" id="hist-period-btns">
            <button class="filter-btn" data-val="1d" onclick="setHistPeriod('1d',this)">1д</button>
            <button class="filter-btn" data-val="2d" onclick="setHistPeriod('2d',this)">2д</button>
            <button class="filter-btn" data-val="3d" onclick="setHistPeriod('3d',this)">3д</button>
            <button class="filter-btn active" data-val="1w" onclick="setHistPeriod('1w',this)">1н</button>
            <button class="filter-btn" data-val="1m" onclick="setHistPeriod('1m',this)">1м</button>
            <button class="filter-btn" data-val="all" onclick="setHistPeriod('all',this)">ALL</button>
          </div>
          <div class="filter-sep"></div>
          <span class="filter-label">Direction</span>
          <div class="filter-group" id="hist-dir-btns">
            <button class="filter-btn" data-val="LONG" onclick="setHistDir('LONG',this)">LONG</button>
            <button class="filter-btn" data-val="SHORT" onclick="setHistDir('SHORT',this)">SHORT</button>
            <button class="filter-btn active" data-val="BOTH" onclick="setHistDir('BOTH',this)">BOTH</button>
          </div>
          <div style="flex:1"></div>
          <a class="export-btn" onclick="exportData('history','csv')">⬇ CSV</a>
          <a class="export-btn" onclick="exportData('history','json')">⬇ JSON</a>
        </div>
        <div class="table-wrap" id="history-table">
          <div class="loading"><span class="spinner"></span>Загрузка истории...</div>
        </div>
        <div class="pagination" id="hist-pagination"></div>
      </div>

    </div><!-- /content -->
  </div><!-- /main -->
</div><!-- /shell -->

<script>
const SECRET = '__SECRET__';
const API_BASE = '/api';

let state = {
  tab: 'stats',
  period: 'all',
  direction: 'BOTH',
  posDir: 'BOTH',
  histPeriod: '1w',
  histDir: 'BOTH',
  histPage: 1,
  histTotal: 0,
  pnlChart: null,
  allPositions: [],
};

function apiUrl(path, params = {}) {
  const u = new URL(API_BASE + path, window.location.origin);
  if (SECRET) u.searchParams.set('secret', SECRET);
  for (const [k, v] of Object.entries(params)) u.searchParams.set(k, v);
  return u.toString();
}

async function fetchJSON(path, params = {}) {
  const resp = await fetch(apiUrl(path, params));
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.json();
}

// ── Tab switching ──────────────────────────────────────────────────────────────
function switchTab(tab) {
  state.tab = tab;
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + tab).classList.add('active');
  document.getElementById('nav-' + tab).classList.add('active');
  if (tab === 'stats') loadStats();
  if (tab === 'positions') loadPositions();
  if (tab === 'history') loadHistory();
}

// ── Filter helpers ─────────────────────────────────────────────────────────────
function setActive(groupId, val) {
  document.querySelectorAll('#' + groupId + ' .filter-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.val === val);
  });
}
function setPeriod(val) { state.period = val; setActive('period-btns', val); loadStats(); }
function setDirection(val) { state.direction = val; setActive('dir-btns', val); loadStats(); }
function setPosDir(val, btn) { state.posDir = val; setActive('pos-dir-btns', val); renderPositions(); }
function setHistPeriod(val) { state.histPeriod = val; state.histPage = 1; setActive('hist-period-btns', val); loadHistory(); }
function setHistDir(val) { state.histDir = val; state.histPage = 1; setActive('hist-dir-btns', val); loadHistory(); }

// ── Stats ──────────────────────────────────────────────────────────────────────
async function loadStats() {
  try {
    const data = await fetchJSON('/stats', { period: state.period, direction: state.direction });
    renderKPIs(data);
    renderBreakdown(data.breakdown);
    renderChart(data.stats.daily_pnl);
  } catch(e) {
    document.getElementById('kpi-grid').innerHTML = `<div class="loading">Ошибка: ${e.message}</div>`;
  }
}

function renderKPIs(data) {
  const s = data.stats;
  const pnlClass = s.total_pnl > 0 ? 'green' : s.total_pnl < 0 ? 'red' : 'yellow';
  const avgClass = s.avg_pnl > 0 ? 'green' : s.avg_pnl < 0 ? 'red' : 'yellow';
  const wrClass  = s.win_rate >= 50 ? 'green' : s.win_rate >= 35 ? 'yellow' : 'red';

  document.getElementById('kpi-grid').innerHTML = `
    <div class="kpi-card">
      <div class="kpi-label">Σ Trade P&L %</div>
      <div class="kpi-value ${pnlClass}">${fmtPnl(s.total_pnl)}</div>
      <div class="kpi-sub">Avg: ${fmtPnl(s.avg_pnl)} · n=${s.pnl_scored}/${s.total}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Win Rate</div>
      <div class="kpi-value ${wrClass}">${s.win_rate}%</div>
      <div class="kpi-sub">Net+: ${s.profitable} · Net−: ${s.net_losses} · BE: ${s.breakeven}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Total Trades</div>
      <div class="kpi-value accent">${s.total}</div>
      <div class="kpi-sub">Full TP: ${s.wins} Partial: ${s.partials} Status Loss: ${s.losses}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Открытые</div>
      <div class="kpi-value cyan">${data.open_total}</div>
      <div class="kpi-sub">Long: ${data.open_longs} Short: ${data.open_shorts}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Today P&L %</div>
      <div class="kpi-value ${s.today_pnl > 0 ? 'green' : s.today_pnl < 0 ? 'red' : 'yellow'}">${fmtPnl(s.today_pnl)}</div>
      <div class="kpi-sub">W:${s.today_wins} L:${s.today_losses} T:${s.today_total}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">SL events today</div>
      <div class="kpi-value red">${s.today_sl}</div>
      <div class="kpi-sub">BE-стоп сегодня: ${s.today_be}</div>
    </div>
  `;

  // TP badges
  const tpRow = document.getElementById('tp-row');
  let badges = '';
  for (let i = 1; i <= 4; i++) {
    const cnt = s.tp_counts[i] || 0;
    badges += `<span class="tp-badge tp${i}">✅ Reached TP${i}: ${cnt}</span>`;
  }
  badges += `<span class="tp-badge sl">❌ SL: ${s.sl_count}</span>`;
  badges += `<span class="tp-badge be">🔒 BE: ${s.be_count}</span>`;
  // Today TP
  let todayTpStr = '';
  for (let i = 1; i <= 4; i++) {
    const cnt = (s.today_tp || {})[i] || 0;
    if (cnt > 0) todayTpStr += ` TP${i}:${cnt}`;
  }
  if (todayTpStr) badges += `<span class="tp-badge tp1" style="opacity:0.7; font-size:11px;">Today${todayTpStr}</span>`;
  const q = s.quality || {};
  if ((q.pnl_missing || 0) + (q.negative_partials || 0) + (q.positive_losses || 0) + (q.legacy_tp_above_4 || 0) + (q.tp_summary_disagreements || 0) + (q.duplicate_terminal_events || 0) > 0) {
    badges += `<span class="tp-badge be">⚠ Data: missing P&L ${q.pnl_missing||0}, partial≤0 ${q.negative_partials||0}, loss&gt;0 ${q.positive_losses||0}, TP mismatch ${q.tp_summary_disagreements||0}, legacy TP5+ ${q.legacy_tp_above_4||0}, duplicate terminal ${q.duplicate_terminal_events||0}</span>`;
  }
  tpRow.innerHTML = badges;
}

function renderBreakdown(rows) {
  const labels = { LONG: { cls: 'long', icon: '🟢' }, SHORT: { cls: 'short', icon: '🔴' }, BOTH: { cls: 'both', icon: '⚡' }};
  let html = '';
  for (const r of rows) {
    const l = labels[r.label] || { cls: 'both', icon: '•' };
    const pnlClass = r.total_pnl > 0 ? 'green' : r.total_pnl < 0 ? 'red' : 'yellow';
    const todayClass = r.today_pnl > 0 ? 'green' : r.today_pnl < 0 ? 'red' : 'yellow';
    html += `
      <div class="breakdown-card">
        <div class="breakdown-title ${l.cls}">${l.icon} ${r.label}</div>
        <div class="brow"><span class="brow-label">Σ Trade P&L %</span><span class="brow-val ${pnlClass}">${fmtPnl(r.total_pnl)}</span></div>
        <div class="brow"><span class="brow-label">Today P&L %</span><span class="brow-val ${todayClass}">${fmtPnl(r.today_pnl)}</span></div>
        <div class="brow"><span class="brow-label">Win Rate</span><span class="brow-val">${r.win_rate}%</span></div>
        <div class="brow"><span class="brow-label">Net+ / Net− / BE</span><span class="brow-val">${r.profitable} / ${r.net_losses} / ${r.breakeven}</span></div>
        <div class="brow"><span class="brow-label">Открытые</span><span class="brow-val cyan">${r.open_count}</span></div>
        <div class="brow"><span class="brow-label">Всего сделок</span><span class="brow-val">${r.total}</span></div>
        <div class="brow"><span class="brow-label">TP Today</span><span class="brow-val green">${r.today_wins}</span></div>
        <div class="brow"><span class="brow-label">SL Today</span><span class="brow-val red">${r.today_sl}</span></div>
        <div class="brow"><span class="brow-label">BE Today</span><span class="brow-val yellow">${r.today_be}</span></div>
      </div>`;
  }
  document.getElementById('breakdown-grid').innerHTML = html || '<div class="empty">Нет данных</div>';
}

function renderChart(dailyPnl) {
  const days = Object.keys(dailyPnl).sort();
  let cumulative = 0;
  const cumData = days.map(d => { cumulative += dailyPnl[d]; return { x: d, y: +cumulative.toFixed(2) }; });
  const barData  = days.map(d => dailyPnl[d]);
  const colors   = barData.map(v => v >= 0 ? 'rgba(16,185,129,0.5)' : 'rgba(239,68,68,0.5)');

  const canvas = document.getElementById('pnlChart');
  if (state.pnlChart) { state.pnlChart.destroy(); }

  state.pnlChart = new Chart(canvas, {
    data: {
      labels: days,
      datasets: [
        {
          type: 'bar',
          label: 'Σ trade P&L % день',
          data: barData,
          backgroundColor: colors,
          borderColor: colors.map(c => c.replace('0.5', '0.9')),
          borderWidth: 1,
          yAxisID: 'y',
        },
        {
          type: 'line',
          label: 'Кум. Σ trade P&L %',
          data: cumData.map(d => d.y),
          borderColor: '#6366f1',
          backgroundColor: 'rgba(99,102,241,0.1)',
          borderWidth: 2,
          pointRadius: 3,
          fill: true,
          tension: 0.3,
          yAxisID: 'y2',
        }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: '#64748b', font: { size: 11 } } },
        tooltip: {
          backgroundColor: '#181b22',
          borderColor: '#22262f',
          borderWidth: 1,
          titleColor: '#e2e8f0',
          bodyColor: '#94a3b8',
          callbacks: {
            label: ctx => ` ${ctx.dataset.label}: ${ctx.parsed.y > 0 ? '+' : ''}${ctx.parsed.y.toFixed(2)}%`
          }
        }
      },
      scales: {
        x: { grid: { color: '#22262f' }, ticks: { color: '#64748b', font: { size: 10 }, maxTicksLimit: 14 } },
        y: { grid: { color: '#22262f' }, ticks: { color: '#64748b', font: { size: 10 }, callback: v => v+'%' } },
        y2: { position: 'right', grid: { display: false }, ticks: { color: '#818cf8', font: { size: 10 }, callback: v => v+'%' } }
      }
    }
  });
}

// ── Positions ──────────────────────────────────────────────────────────────────
async function loadPositions() {
  document.getElementById('positions-table').innerHTML = '<div class="loading"><span class="spinner"></span>Загрузка...</div>';
  try {
    const data = await fetchJSON('/positions');
    state.allPositions = data.positions;
    renderPositions();
  } catch(e) {
    document.getElementById('positions-table').innerHTML = `<div class="loading">Ошибка: ${e.message}</div>`;
  }
}

function renderPositions() {
  const dir = state.posDir;
  const rows = state.allPositions.filter(p => {
    if (dir === 'LONG') return p.direction === 'BUY';
    if (dir === 'SHORT') return p.direction === 'SELL';
    return true;
  });

  if (!rows.length) {
    document.getElementById('positions-table').innerHTML = '<div class="empty">Нет открытых позиций</div>';
    return;
  }

  let html = `<table>
    <thead><tr>
      <th>Пара</th><th>Направление</th><th>Вход</th><th>SL</th>
      <th>TP1</th><th>TP2</th><th>TP3</th><th>TP4</th>
      <th>Сигнал</th><th>Режим входа</th><th>Score / ATR</th><th>BE/Trail</th>
      <th>ТФ</th><th>Биржа</th><th>Открыта</th>
    </tr></thead><tbody>`;

  for (const p of rows) {
    const dirClass = p.direction === 'BUY' ? 'dir-long' : 'dir-short';
    const dirLabel = p.direction === 'BUY' ? '⬆ LONG' : '⬇ SHORT';
    const sigTag = p.is_strong
      ? '<span class="tag tag-strong">STRONG</span>'
      : '<span class="tag tag-normal">NORMAL</span>';
    const modeParts = [p.entry_mode, p.wyckoff_phase ? 'Wyckoff' : '', p.ict ? 'ICT' : '', p.amd_phase ? `AMD:${p.amd_phase}` : ''].filter(Boolean);
    const modeStr = modeParts.length ? modeParts.join(' + ') : '—';
    const analyticsStr = p.score == null
      ? '—'
      : `${Number(p.score).toFixed(1)} / C${p.confirmations ?? '—'} / ATR ${p.atr_pct ?? '—'}%`;
    let beTrail = '';
    if (p.trail_active) beTrail = '<span class="tag tag-trail">TRAIL</span>';
    else if (p.be_active) beTrail = '<span class="tag tag-be">BE</span>';
    else beTrail = '<span style="color:var(--muted)">—</span>';

    html += `<tr>
      <td class="mono" style="font-weight:600">${p.ticker}</td>
      <td class="${dirClass}">${dirLabel}</td>
      <td class="mono">${fmt(p.entry_price)}</td>
      <td class="mono red">${fmt(p.sl_price)}</td>
      <td class="mono green">${fmt(p.tp1)}</td>
      <td class="mono green">${fmt(p.tp2)}</td>
      <td class="mono green">${fmt(p.tp3)}</td>
      <td class="mono green">${fmt(p.tp4)}</td>
      <td>${sigTag}</td>
      <td style="color:var(--cyan)">${modeStr || '—'}</td>
      <td style="color:var(--muted);font-size:11px">${analyticsStr}</td>
      <td>${beTrail}</td>
      <td style="color:var(--muted)">${p.timeframe || '—'}</td>
      <td style="color:var(--muted)">${p.exchange || '—'}</td>
      <td style="color:var(--muted);font-size:11px">${p.created_at_fmt || '—'}</td>
    </tr>`;
  }
  html += '</tbody></table>';
  document.getElementById('positions-table').innerHTML = html;
}

// ── History ────────────────────────────────────────────────────────────────────
async function loadHistory() {
  document.getElementById('history-table').innerHTML = '<div class="loading"><span class="spinner"></span>Загрузка...</div>';
  try {
    const data = await fetchJSON('/history', {
      period: state.histPeriod,
      direction: state.histDir,
      page: state.histPage,
      per_page: 50,
    });
    state.histTotal = data.total;
    renderHistory(data.rows, data.total);
    renderPagination(data.total, data.page, data.per_page);
  } catch(e) {
    document.getElementById('history-table').innerHTML = `<div class="loading">Ошибка: ${e.message}</div>`;
  }
}

function renderHistory(rows, total) {
  if (!rows.length) {
    document.getElementById('history-table').innerHTML = '<div class="empty">Нет сделок за период</div>';
    return;
  }

  let html = `<table>
    <thead><tr>
      <th>Дата закрытия</th><th>Пара</th><th>Направление</th><th>Результат</th>
      <th>Вход</th><th>SL</th><th>TP#</th><th>P&L %</th><th>TP P&L</th>
      <th>Длит.</th><th>Режим</th><th>Score / ATR</th><th>Стиль</th><th>ТФ</th><th>Паттерн</th>
      <th>Плечо</th><th>Биржа</th><th>Детали</th>
    </tr></thead><tbody>`;

  for (const r of rows) {
    const dirClass = r.direction === 'BUY' ? 'dir-long' : 'dir-short';
    const dirLabel = r.direction === 'BUY' ? '⬆ LONG' : '⬇ SHORT';
    let resTag = '';
    if (r.result === 'win') resTag = `<span class="tag tag-win">✅ WIN</span>`;
    else if (r.result === 'partial') resTag = `<span class="tag tag-partial">🔶 PARTIAL</span>`;
    else if (r.result === 'loss') resTag = `<span class="tag tag-sl">❌ LOSS</span>`;
    else resTag = `<span class="tag tag-manual">🧯 MANUAL</span>`;

    const pnlClass = r.pnl_pct == null ? 'pnl-zero' : r.pnl_pct > 0 ? 'pnl-pos' : r.pnl_pct < 0 ? 'pnl-neg' : 'pnl-zero';
    const pnlStr  = r.pnl_pct == null ? '—' : (r.pnl_pct > 0 ? '+' : '') + r.pnl_pct.toFixed(2) + '%';
    const tpPnlStr = r.tp_pnl_pct != null && r.tp_pnl_pct !== 0 ? (r.tp_pnl_pct > 0 ? '+' : '') + r.tp_pnl_pct.toFixed(2) + '%' : '—';
    const tpNum = r.highest_tp_hit ? `TP${r.highest_tp_hit}` : (r.result === 'loss' ? 'SL' : '—');
    const dur = fmtDuration(r.duration_sec || 0);
    const analyticsStr = r.score == null
      ? '—'
      : `${Number(r.score).toFixed(1)} / C${r.confirmations ?? '—'} / ATR ${r.atr_pct ?? '—'}%`;

    // Detail tags
    let details = '';
    if (r.is_strong) details += '<span class="tag tag-strong" style="font-size:10px">STRONG</span> ';
    if (r.trail_active) details += '<span class="tag tag-trail" style="font-size:10px">TRAIL</span> ';
    if (r.be_active) details += '<span class="tag tag-be" style="font-size:10px">BE</span> ';
    if (r.smc) details += '<span class="tag" style="background:#1e2a3a;color:#7dd3fc;font-size:10px">SMC</span> ';
    if (r.ict) details += '<span class="tag" style="background:#1e1b4b;color:#c084fc;font-size:10px">ICT</span> ';
    if (r.wyckoff_phase) details += `<span class="tag" style="background:#2a1f10;color:#fbbf24;font-size:10px">W:${r.wyckoff_phase}</span> `;
    if (r.amd_phase) details += `<span class="tag" style="background:#1a2a10;color:#86efac;font-size:10px">AMD:${r.amd_phase}</span> `;

    html += `<tr>
      <td style="color:var(--muted);font-size:11px">${r.close_time_fmt || '—'}</td>
      <td class="mono" style="font-weight:600">${r.ticker}</td>
      <td class="${dirClass}">${dirLabel}</td>
      <td>${resTag}</td>
      <td class="mono">${fmt(r.entry_price)}</td>
      <td class="mono red">${fmt(r.sl_price)}</td>
      <td style="font-weight:600;color:var(--cyan)">${tpNum}</td>
      <td class="${pnlClass}">${pnlStr}</td>
      <td class="pnl-pos">${tpPnlStr}</td>
      <td class="dur">${dur}</td>
      <td style="color:var(--cyan);font-size:11px">${r.entry_mode || '—'}</td>
      <td style="color:var(--muted);font-size:11px">${analyticsStr}</td>
      <td style="color:var(--muted);font-size:11px">${r.style || '—'}</td>
      <td style="color:var(--muted);font-size:11px">${r.timeframe || '—'}</td>
      <td style="color:var(--muted);font-size:11px">${r.pattern || '—'}</td>
      <td style="color:var(--muted);font-size:11px">${r.leverage ? r.leverage+'x' : '—'}</td>
      <td style="color:var(--muted);font-size:11px">${r.exchange || '—'}</td>
      <td>${details || '<span style="color:var(--muted)">—</span>'}</td>
    </tr>`;
  }
  html += `</tbody></table>`;
  document.getElementById('history-table').innerHTML = html;
}

function renderPagination(total, page, perPage) {
  const pages = Math.ceil(total / perPage);
  const pg = document.getElementById('hist-pagination');
  if (pages <= 1) { pg.innerHTML = ''; return; }
  pg.innerHTML = `
    <span class="page-info">Записей: ${total}</span>
    <button class="page-btn" onclick="histPage(${page-1})" ${page <= 1 ? 'disabled' : ''}>← Пред.</button>
    <span class="page-info">${page} / ${pages}</span>
    <button class="page-btn" onclick="histPage(${page+1})" ${page >= pages ? 'disabled' : ''}>След. →</button>
  `;
}

function histPage(p) { state.histPage = p; loadHistory(); }

// ── Export ─────────────────────────────────────────────────────────────────────
function exportData(tab, fmt) {
  const params = new URLSearchParams({ tab, fmt });
  if (SECRET) params.set('secret', SECRET);
  if (tab === 'history') {
    params.set('period', state.histPeriod);
    params.set('direction', state.histDir);
  }
  window.location.href = `/api/export?${params.toString()}`;
}

// ── Formatters ─────────────────────────────────────────────────────────────────
function fmt(v) {
  if (v == null) return '<span style="color:var(--muted)">—</span>';
  const n = parseFloat(v);
  if (isNaN(n)) return String(v);
  if (n >= 1000) return n.toLocaleString('ru-RU', { maximumFractionDigits: 2 });
  if (n >= 1) return n.toFixed(4);
  return n.toFixed(6);
}
function fmtPnl(v) {
  if (v == null || isNaN(v)) return '—';
  return (v > 0 ? '+' : '') + v.toFixed(2) + '%';
}
function fmtDuration(sec) {
  if (!sec) return '—';
  if (sec < 60) return sec + 'с';
  if (sec < 3600) return Math.floor(sec/60) + 'м';
  if (sec < 86400) return Math.floor(sec/3600) + 'ч ' + Math.floor((sec%3600)/60) + 'м';
  return Math.floor(sec/86400) + 'д ' + Math.floor((sec%86400)/3600) + 'ч';
}

// ── Auto-refresh ───────────────────────────────────────────────────────────────
function refreshAll() {
  const tab = state.tab;
  if (tab === 'stats') loadStats();
  if (tab === 'positions') loadPositions();
  if (tab === 'history') loadHistory();
  const now = new Date();
  document.getElementById('lastRefresh').textContent =
    'Обновлено: ' + now.toLocaleTimeString('ru-RU');
}

// Init
document.addEventListener('DOMContentLoaded', () => {
  loadStats();
  setInterval(refreshAll, 30 * 60 * 1000); // every 30 min
});
</script>
</body>
</html>
"""

# ── Telegram P&L alert (optional) ────────────────────────────────────────────
_last_alert_day = None

def _check_pnl_alert():
    """Send daily P&L alert to admin if threshold breached."""
    global _last_alert_day
    if not TG_TOKEN or not TG_ADMIN_ID:
        return
    today = datetime.now(timezone.utc).date().isoformat()
    if _last_alert_day == today:
        return
    try:
        history = redis_get("trade_history") or []
        today_cutoff = (datetime.now(timezone.utc).replace(hour=0, minute=0, second=0).timestamp())
        today_trades = [r for r in history if (r.get("close_time") or 0) >= today_cutoff]
        pnl_vals = [r["pnl"]["pnl_pct"] for r in today_trades if r.get("pnl") and r["pnl"].get("pnl_pct") != 0.0]
        today_pnl = sum(pnl_vals) if pnl_vals else 0.0
        if today_pnl <= PNL_ALERT_THRESHOLD:
            msg = (f"⚠️ <b>Statham Dashboard Alert</b>\n"
                   f"Today P&L: <b>{today_pnl:+.2f}%</b> (порог: {PNL_ALERT_THRESHOLD}%)\n"
                   f"Сделок сегодня: {len(today_trades)}")
            import requests as _req
            _req.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={"chat_id": TG_ADMIN_ID, "text": msg, "parse_mode": "HTML"},
                timeout=10,
            )
            _last_alert_day = today
    except Exception:
        pass

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
