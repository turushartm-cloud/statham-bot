#!/usr/bin/env python3
"""
retro_fix.py — Ретроспективная коррекция истории Statham Bot
=============================================================
Запускать ОДИН РАЗ после деплоя патча.

Что делает:
  1. Загружает trade_history из Redis (Upstash HTTP API)
  2. Исправляет записи: close_reason=sl_hit + result="win" → result="partial"
  3. Пересчитывает trade_stats: partial=win в WR
  4. Сохраняет обратно в Redis

Использование:
  export REDIS_URL="rediss://default:<password>@<host>:<port>"
  python3 retro_fix.py [--dry-run]

  Или с прямым Upstash HTTP API (не требует redis-py):
  export UPSTASH_URL="https://fitting-walleye-74509.upstash.io"
  export UPSTASH_TOKEN="<your_token>"
  python3 retro_fix.py

Флаги:
  --dry-run   Показать что будет изменено, не сохранять
  --stats-only  Только пересчитать stats без правки истории
"""

from __future__ import annotations
import json
import os
import sys
import time
from typing import Any

DRY_RUN    = "--dry-run"    in sys.argv
STATS_ONLY = "--stats-only" in sys.argv

REDIS_URL     = os.environ.get("REDIS_URL", "").strip()
UPSTASH_URL   = os.environ.get("UPSTASH_URL", "").strip()
UPSTASH_TOKEN = os.environ.get("UPSTASH_TOKEN", "").strip()
_REDIS_PREFIX = "statham:"


# ══════════════════════════════════════════════════════════════════════════════
# REDIS BACKENDS
# ══════════════════════════════════════════════════════════════════════════════

def _redis_get(key: str) -> Any:
    """Получить значение из Redis. Поддерживает redis-py и Upstash HTTP API."""
    full_key = _REDIS_PREFIX + key

    # 1) redis-py (если доступен и задан REDIS_URL)
    if REDIS_URL:
        try:
            import redis
            r = redis.from_url(REDIS_URL, decode_responses=True,
                               socket_timeout=15, socket_connect_timeout=15)
            val = r.get(full_key)
            return json.loads(val) if val else None
        except ImportError:
            pass
        except Exception as e:
            print(f"[redis-py] get {key}: {e}")

    # 2) Upstash HTTP REST API
    if UPSTASH_URL and UPSTASH_TOKEN:
        try:
            import urllib.request
            req = urllib.request.Request(
                f"{UPSTASH_URL.rstrip('/')}/get/{full_key}",
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            val = data.get("result")
            return json.loads(val) if val else None
        except Exception as e:
            print(f"[upstash-http] get {key}: {e}")

    raise RuntimeError("Нет доступного Redis-транспорта. Задайте REDIS_URL или UPSTASH_URL+UPSTASH_TOKEN.")


def _redis_set(key: str, value: Any) -> bool:
    full_key = _REDIS_PREFIX + key
    serialized = json.dumps(value, ensure_ascii=False)

    # 1) redis-py
    if REDIS_URL:
        try:
            import redis
            r = redis.from_url(REDIS_URL, decode_responses=True,
                               socket_timeout=15, socket_connect_timeout=15)
            r.set(full_key, serialized)
            return True
        except ImportError:
            pass
        except Exception as e:
            print(f"[redis-py] set {key}: {e}")

    # 2) Upstash HTTP REST API
    if UPSTASH_URL and UPSTASH_TOKEN:
        try:
            import urllib.request
            body = json.dumps(["SET", full_key, serialized]).encode()
            req = urllib.request.Request(
                f"{UPSTASH_URL.rstrip('/')}/",
                data=body,
                headers={
                    "Authorization": f"Bearer {UPSTASH_TOKEN}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            return data.get("result") == "OK"
        except Exception as e:
            print(f"[upstash-http] set {key}: {e}")

    raise RuntimeError("Нет доступного Redis-транспорта.")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("Statham Bot — Retro Fix Script")
    print(f"Mode: {'DRY RUN (no save)' if DRY_RUN else ('STATS ONLY' if STATS_ONLY else 'LIVE')}")
    print("=" * 60)

    # ── 1. Загружаем историю ─────────────────────────────────────────
    print("\n[1/4] Загружаем trade_history из Redis...")
    try:
        history: list = _redis_get("trade_history") or []
    except Exception as e:
        print(f"  ОШИБКА: {e}")
        sys.exit(1)
    print(f"  Записей: {len(history)}")

    if not STATS_ONLY:
        # ── 2. Исправляем win→partial ────────────────────────────────
        print("\n[2/4] Ищем записи close_reason=sl_hit + result=win для исправления...")
        fixed = 0
        examples = []
        for r in history:
            if (r.get("close_reason") == "sl_hit"
                    and r.get("result") == "win"
                    and int(r.get("highest_tp_hit") or r.get("tp_num") or 0) > 0):
                if len(examples) < 5:
                    examples.append(
                        f"  {r.get('ticker','?')} {r.get('direction','?')} "
                        f"date={r.get('date_msk','?')} highest_tp={r.get('highest_tp_hit','?')}"
                    )
                r["result"] = "partial"
                fixed += 1

        print(f"  Найдено для исправления: {fixed}")
        if examples:
            print("  Примеры:")
            for ex in examples:
                print(ex)

        if fixed > 0 and not DRY_RUN:
            print("\n[3/4] Сохраняем исправленную историю в Redis...")
            ok = _redis_set("trade_history", history)
            print(f"  {'OK' if ok else 'ОШИБКА!'}")
        else:
            print("\n[3/4] Пропускаем сохранение истории (dry-run или нечего исправлять).")
    else:
        print("\n[2/4] STATS_ONLY — пропускаем правку истории.")
        print("[3/4] STATS_ONLY — пропускаем сохранение истории.")

    # ── 4. Пересчитываем trade_stats ────────────────────────────────
    print("\n[4/4] Пересчитываем trade_stats из актуальной истории...")
    wins_full    = sum(1 for r in history if r.get("result") == "win")
    wins_partial = sum(1 for r in history if r.get("result") == "partial")
    losses       = sum(1 for r in history if r.get("result") == "loss")
    manuals      = sum(1 for r in history if r.get("result") == "manual")
    total        = wins_full + wins_partial + losses + manuals
    wr           = round((wins_full + wins_partial) / total * 100, 1) if total else 0.0

    new_stats = {
        "wins":          wins_full + wins_partial,
        "losses":        losses,
        "total":         total,
        "_wins_full":    wins_full,
        "_wins_partial": wins_partial,
        "_losses":       losses,
        "_manuals":      manuals,
    }

    print(f"\n  Результат пересчёта:")
    print(f"  🏆 Full TP:   {wins_full}")
    print(f"  🔶 Partial:   {wins_partial}")
    print(f"  ❌ Pure SL:   {losses}")
    print(f"  🧯 Manual:    {manuals}")
    print(f"  📈 Total:     {total}")
    print(f"  📊 Win Rate:  {wr}%")
    print(f"  Redis wins={wins_full + wins_partial}, losses={losses}")

    if not DRY_RUN:
        ok = _redis_set("trade_stats", new_stats)
        print(f"\n  Сохранение trade_stats в Redis: {'OK' if ok else 'ОШИБКА!'}")
    else:
        print("\n  [DRY RUN] Сохранение пропущено.")

    print("\n" + "=" * 60)
    print("ГОТОВО." if not DRY_RUN else "DRY RUN ЗАВЕРШЁН. Данные не изменены.")
    print("Следующий шаг: /recalc_stats в Telegram для синхронизации.")
    print("=" * 60)


if __name__ == "__main__":
    main()
