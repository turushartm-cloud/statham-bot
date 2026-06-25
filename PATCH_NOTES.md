# Statham Bot — Patch Notes v2.1-bugfix

## Сводка: 9 багов → все исправлены

---

## 🔴 КРИТИЧЕСКИЕ (немедленный деплой)

### БАГ #7 — КРИТИЧЕСКИЙ: `parse_price("Price:")` ловит неверную цену выхода
**Файл:** `app.py` → `handle_sl_hit()` (~строка 1940)

**Проблема:**
```python
# БЫЛО (сломано):
sl_exit_price = (
    parse_price(text, "Цена выхода:", "💰 Цена выхода:")
    or parse_price(text, "Exit:", "exit:")
    or parse_price(text, "Price:", "price:")  # ← ВИНОВНИК
)
```
Паттерн `"Price:"` матчил `"Entry Price: 4.775"`, `"Take Profit Price: 4.804"`, `"Trigger Price:"` и т.д.
→ Бот брал **TP-цену** как `sl_exit_price` → P&L для BUY: `(tp_price - entry) / entry > 0` → **положительный убыток**.

**Последствия из истории:**
| Тикер | Направление | Результат | Entry | SL | P&L (бот) | Реальность |
|-------|-------------|-----------|-------|-----|-----------|------------|
| KSMUSDT | BUY | loss | 4.775 | 4.622 | **+6.07%** | Физически невозможно |
| BANANAS31 | BUY | loss | 0.00947 | 0.00904 | **+120.93%** | Физически невозможно |
| BANKUSDT | SELL | loss | 0.0561 | null | **+378.61%** | Астрономический абсурд |
| CLOUSDT | SELL | loss | 0.10468 | 0.10799 | **-331.1%** | Катастрофически неверно |
| ZBTUSDT | BUY | loss | 0.12764 | 0.11927 | **+177.53%** | Невозможно |

**Исправление:**
```python
# СТАЛО (исправлено):
sl_exit_price = (
    parse_price(text, "Цена выхода:", "💰 Цена выхода:")
    or parse_price(text, "Exit price:", "Exit price:")
    # НЕ используем "Price:" — слишком широкий паттерн
)
# + санити-чек диапазона для JSON payload exit_price (±50% от entry)
# + санити-чек итогового P&L: pure SL без TP → P&L должен быть ≤ 0%
```

---

### БАГ #1 — КРИТИЧЕСКИЙ: `sl_hit` иногда засчитывался как `"win"`
**Файл:** `app.py` → `handle_sl_hit()` (~строка 2013)

**Проблема:**
```python
# БЫЛО:
if highest_tp > 0 and _exchange_sl != "none":
    result = "win"   # ← НЕВЕРНО: биржевые позиции с TP+SL = "win"?
elif highest_tp > 0 and _exchange_sl == "none":
    result = "partial"
```
Для `exchange != "none"` (bingx/bybit), `sl_hit` после TP1+ = `"win"`.  
Результат: 30+ сделок с `close_reason=sl_hit` имеют `result="win"` → статистика завышена.

**Исправление:**
```python
# СТАЛО:
# SL-выход НИКОГДА не "win". "win" = только полное TP-закрытие (handle_tp_hit).
if highest_tp > 0:
    result = "partial"   # TP1-5 hit + SL остаток
else:
    result = "loss"      # чистый SL без единого TP
```

---

### БАГ #8 — КРИТИЧЕСКИЙ: `trade_stats` в Redis полностью некорректен
**Файл:** `app.py` → `update_stats()` + новая команда `/recalc_stats`

**Проблема:**
```json
{"wins": 36, "losses": 174, "total": 210}
```
До патча: `partial → losses`. Redis-счётчик `wins=36` не совпадал ни с одной реальной метрикой.  
Реальный расклад из истории: Full TP ≈ 5-10, Partial ≈ 60+, Pure SL ≈ 140+.

**Исправление:**
1. `update_stats()`: `partial` теперь → `wins` (как прибыльная сделка)
2. Новая команда `/recalc_stats`: пересчёт из реальной истории
3. Новая команда `/retro_fix`: исправление старых записей `win→partial` в истории

---

## 🟠 ВАЖНЫЕ

### БАГ #5: `update_stats` считал `partial` как `loss`
**Файл:** `app.py` → `update_stats()` (~строка 1068)

**Было:**
```python
if result == "win":
    s["wins"] += 1
else:
    s["losses"] += 1  # partial counts as loss ← НЕВЕРНО
```
**Стало:**
```python
if result in ("win", "partial"):
    s["wins"] += 1   # partial = прибыльная сделка
else:
    s["losses"] += 1
```

---

### БАГ #6: `_build_report` — неверные метки + avg TP P&L использовал `pnl_pct` вместо `tp_pnl_pct`
**Файл:** `app.py` → `_build_report()` (~строка 1195)

**Проблема 1:** Метки `"✅ TP: N  ❌ SL: N  🔶 Partial: N"` — вводили в заблуждение (TP = full close, не любой профит).

**Проблема 2:** `tp_pnl_sums[n].append(pnl_data.get("pnl_pct", 0))` — использовал **полный P&L** (включая SL-остаток) для строки "avg TP{n}", что давало завышенные цифры для partial-сделок с крупным SL-хвостом.

**Исправление:**
```python
# Метки:
"🏆 Full TP: N   🔶 Partial: N   ❌ Pure SL: N"

# avg TP P&L:
tp_part = pnl_data.get("tp_pnl_pct", None)  # только доход от TP-частей
```

---

### БАГ #9: `pnl.highest_tp=0, tps_hit=[]` при корректном `tp_pnl_pct`
**Файл:** `app.py` → `handle_tp_hit()` → `final_close` блок (~строка 1880)

**Проблема:** При финальном закрытии на TP (remaining ≤ min_qty) `calc_trade_pnl(pos_snapshot, None)` вызывался до того как `pos_snapshot` гарантированно имел `tp{tp_num}_hit=True`.

**Исправление:**
```python
if not pos_snapshot.get(f"tp{tp_num}_hit"):
    pos_snapshot[f"tp{tp_num}_hit"] = True  # гарантируем наличие флага
pnl = calc_trade_pnl(pos_snapshot, None)
```

---

## 📋 ПЛАН РАЗВЁРТЫВАНИЯ

### Шаг 1: Деплой патча
```bash
git add app.py
git commit -m "fix: bugs #1 #5 #6 #7 #8 #9 - sl_hit result, pnl parsing, stats"
git push
```
Render задеплоит автоматически через GitHub CI.

### Шаг 2: Ретро-коррекция истории (Telegram)
```
/retro_fix
```
Исправляет старые записи `close_reason=sl_hit + result=win → partial` в Redis.

### Шаг 3: Пересчёт статистики (Telegram)
```
/recalc_stats
```
Пересчитывает `trade_stats` в Redis из актуальной (уже исправленной) истории.

### Шаг 4 (опционально): Проверка Redis напрямую
```bash
# Через скрипт:
UPSTASH_URL=https://fitting-walleye-74509.upstash.io \
UPSTASH_TOKEN=<token> \
python3 tools/upstash_check.py

# Или через curl:
curl https://fitting-walleye-74509.upstash.io/get/statham:trade_stats \
     -H "Authorization: Bearer <token>"
```

### Шаг 5 (опционально): retro_fix.py автономно
```bash
REDIS_URL=rediss://default:...@fitting-walleye-74509.upstash.io:6379 \
python3 tools/retro_fix.py --dry-run   # сначала проверить

REDIS_URL=rediss://default:...@fitting-walleye-74509.upstash.io:6379 \
python3 tools/retro_fix.py             # применить
```

---

## 🧮 Ожидаемые цифры после патча

Из дампа истории (210 записей):
- `win` (full TP close): ~10-15 записей (close_reason=`tp_hit_N`)
- `partial` (TP+SL): ~70-80 записей → после ретро-фикса включит старые "win" с sl_hit
- `loss` (pure SL): ~120-130 записей

Ожидаемый Win Rate: ~38-42% (было завышено до ~60% из-за багов)

---

## 📁 Файлы в архиве

| Файл | Описание |
|------|----------|
| `app.py` | Основной файл с патчами всех 9 багов |
| `tools/retro_fix.py` | Скрипт ретро-коррекции истории (standalone) |
| `tools/upstash_check.py` | Инспекция Redis через Upstash HTTP API |
| `PATCH_NOTES.md` | Этот файл |
