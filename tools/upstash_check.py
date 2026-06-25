#!/usr/bin/env python3
"""
upstash_check.py — Inspect Statham Bot Redis state via Upstash HTTP REST API
=============================================================================
Upstash не поддерживает redis-cli. Используй этот скрипт вместо него.

Использование:
  export UPSTASH_URL="https://fitting-walleye-74509.upstash.io"
  export UPSTASH_TOKEN="<your_token_from_upstash_dashboard>"
  python3 upstash_check.py

  # Или передать как аргументы:
  python3 upstash_check.py <UPSTASH_URL> <UPSTASH_TOKEN>

  # Показать конкретный ключ:
  python3 upstash_check.py --key trade_stats

  # Через curl (альтернатива):
  curl https://fitting-walleye-74509.upstash.io/get/statham:trade_stats \
       -H "Authorization: Bearer <token>"
"""

from __future__ import annotations
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone, timedelta

_PREFIX = "statham:"
_KEYS   = ["trade_stats", "trade_history", "active_trades", "positions", "closed_trades"]

def _msk(ts: int) -> str:
    return (datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M МСК")


def _req(url: str, token: str, path: str) -> dict:
    full_url = f"{url.rstrip('/')}/{path}"
    req = urllib.request.Request(
        full_url,
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def _get(url: str, token: str, key: str):
    data = _req(url, token, f"get/{_PREFIX}{key}")
    val  = data.get("result")
    return json.loads(val) if val else None


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    url   = args[0] if len(args) > 0 else os.environ.get("UPSTASH_URL", "").strip()
    token = args[1] if len(args) > 1 else os.environ.get("UPSTASH_TOKEN", "").strip()

    # Check for specific key flag
    specific_key = None
    for i, a in enumerate(sys.argv[1:]):
        if a == "--key" and i + 1 < len(sys.argv) - 1:
            specific_key = sys.argv[i + 2]

    if not url or not token:
        print("Ошибка: UPSTASH_URL и UPSTASH_TOKEN обязательны.")
        print("  export UPSTASH_URL=https://fitting-walleye-74509.upstash.io")
        print("  export UPSTASH_TOKEN=<token_from_upstash_dashboard>")
        sys.exit(1)

    print(f"Upstash: {url}")
    print("=" * 70)

    keys_to_check = [specific_key] if specific_key else _KEYS

    for key in keys_to_check:
        print(f"\n{'─'*60}")
        print(f"KEY: {_PREFIX}{key}")
        try:
            val = _get(url, token, key)
            if val is None:
                print("  (пусто / ключ не существует)")
                continue

            if key == "trade_stats":
                print(f"  wins:           {val.get('wins', '?')}")
                print(f"  losses:         {val.get('losses', '?')}")
                print(f"  total:          {val.get('total', '?')}")
                print(f"  _wins_full:     {val.get('_wins_full', 'нет (старая версия)')}")
                print(f"  _wins_partial:  {val.get('_wins_partial', 'нет (старая версия)')}")
                if val.get("total", 0):
                    wr = round(val.get("wins", 0) / val["total"] * 100, 1)
                    print(f"  → Win Rate: {wr}%")

            elif key == "trade_history":
                history = val if isinstance(val, list) else []
                print(f"  Записей: {len(history)}")
                by_result = {}
                for r in history:
                    res = r.get("result", "?")
                    by_result[res] = by_result.get(res, 0) + 1
                for res, cnt in sorted(by_result.items()):
                    print(f"  {res}: {cnt}")
                if history:
                    last = sorted(history, key=lambda r: r.get("close_time", 0))[-5:]
                    print("  Последние 5:")
                    for r in reversed(last):
                        ts = r.get("close_time", 0)
                        print(f"    {r.get('ticker','?'):15} {r.get('direction','?'):4} "
                              f"{r.get('result','?'):8} {_msk(ts) if ts else '?'}")

            elif key == "active_trades":
                trades = val if isinstance(val, dict) else {}
                print(f"  Активных сделок: {len(trades)}")
                for k, t in trades.items():
                    print(f"    {t.get('ticker','?')} {t.get('direction','?')} — key={k[:40]}")

            elif key == "positions":
                positions = val if isinstance(val, dict) else {}
                print(f"  Открытых позиций: {len(positions)}")
                for k, p in positions.items():
                    print(f"    {p.get('symbol','?'):15} {p.get('direction','?'):4} "
                          f"remaining={p.get('remaining_qty','?')}")

            elif key == "closed_trades":
                closed = val if isinstance(val, dict) else {}
                print(f"  Закрытых (dedup) записей: {len(closed)}")

            else:
                raw = json.dumps(val, ensure_ascii=False)
                print(f"  {raw[:300]}{'...' if len(raw) > 300 else ''}")

        except Exception as e:
            print(f"  ОШИБКА: {e}")

    print("\n" + "=" * 70)
    print("Готово. Для изменения данных используй retro_fix.py")
    print("\nAlternative — curl:")
    print(f"  curl {url}/get/{_PREFIX}trade_stats \\")
    print(f"    -H 'Authorization: Bearer {token[:10]}...'")


if __name__ == "__main__":
    main()
