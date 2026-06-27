# Statham Trading Dashboard

Дашборд для анализа торговой статистики бота Statham.  
Читает данные напрямую из Upstash Redis — без дублирования логики.

## Структура проекта

```
statham-bot-main/
├── app.py                    ← бот (без изменений)
├── dashboard/
│   ├── app.py                ← дашборд (self-contained)
│   ├── requirements.txt
│   ├── .env.example
│   └── README.md
├── render.yaml               ← два сервиса
├── requirements.txt
└── gunicorn_config.py
```

## Установка

### Шаг 1 — Структура папок

```bash
cd statham-bot-main
mkdir dashboard
# Скопируй файлы дашборда в папку dashboard/
```

### Шаг 2 — Локальный запуск (для теста)

```bash
cd dashboard
cp .env.example .env
# Заполни .env реальными значениями

pip install flask redis gunicorn requests
python app.py
# Открой http://localhost:10000/?secret=your_secret_token_here
```

### Шаг 3 — Деплой на Render

1. Скопируй `render.yaml` в корень репозитория (заменив старый)
2. Commit + push:
   ```bash
   git add dashboard/ render.yaml
   git commit -m "feat: add trading dashboard"
   git push
   ```
3. В Render Console → Blueprint → создастся два сервиса:
   - `statham-bot` (уже существует, не изменится)
   - `statham-dashboard` (новый)
4. В настройках `statham-dashboard` установи переменные:
   - `REDIS_URL` — тот же что у бота
   - `DASHBOARD_SECRET` — скопируй из "Environment" вкладки (Render генерирует автоматически)
   - `TG_TOKEN`, `TG_ADMIN_ID` — опционально для алертов

### Шаг 4 — Доступ

URL дашборда: `https://statham-dashboard-XXXX.onrender.com/?secret=YOUR_SECRET`

> **Совет**: добавь закладку с секретом в URL — каждый раз вводить не придётся.

## Функционал

### 📊 Statistics (вкладка 1)
- KPI: Total P&L%, Win Rate, Open trades, Today P&L, SL/BE Today
- TP1-6 count + SL + BE badges
- Кумулятивный P&L график (bar + line)
- Breakdown таблица Long / Short / Both

Фильтры: Period (1д/2д/3д/1н/1м/ALL) × Direction (LONG/SHORT/BOTH)

### 📈 Open Positions (вкладка 2)
- Real-time список открытых позиций
- Столбцы: пара, направление, вход, SL, TP1-6, сигнал (STRONG/NORMAL), режим входа (SMC/ICT/...), BE/Trail, ТФ, биржа, время открытия
- Экспорт CSV / JSON

### 📜 History (вкладка 3)
- Вся история сделок с фильтрами Period × Direction
- Столбцы: дата, пара, направление, результат, вход, SL, TP#, P&L%, TP P&L, длительность, режим, стиль, ТФ, паттерн, плечо, биржа, SMC/ICT/Wyckoff/AMD теги
- Пагинация (50 записей/страница)
- Экспорт CSV / JSON

### Telegram алерты
При дневном P&L ≤ `PNL_ALERT_THRESHOLD` (дефолт -5%) — бот пришлёт тебе личное сообщение.  
Настраивается через `TG_ADMIN_ID` и `PNL_ALERT_THRESHOLD` в ENV.

## Обновление данных

Дашборд не кеширует данные — каждый API запрос идёт напрямую в Redis.  
Фронтенд автообновляется каждые **30 минут**.  
Кнопка «↻ Обновить» — мгновенное обновление текущей вкладки.

## Расширенные метаданные (SMC/ICT/AMD/Wyckoff)

Для отображения этих полей нужно:
1. Добавить в `handle_entry()` в `app.py` парсинг полей из payload (см. `last.txt`)
2. Твой Pine Script должен слать эти поля в webhook JSON

Без патча дашборд работает — просто показывает «—» в соответствующих столбцах.

## Roadmap / Масштабирование

| Фича | Сложность | Описание |
|------|-----------|----------|
| WebSocket | Medium | Push-обновления вместо polling |
| Heatmap по парам | Easy | Какая пара самая прибыльная |
| Фильтр по entry_mode | Easy | Сравнить WR у SMC vs ICT |
| Telegram /stats ссылка | Easy | Кнопка в боте → открыть дашборд |
| PostgreSQL | Hard | Для истории > 10k записей |
| Auth через Telegram | Medium | Login with Telegram OAuth |
| Embedded blueprint | Easy | `/dashboard` роут внутри app.py — без второго сервиса |

## Данные в Redis (что читает дашборд)

| Ключ | Что хранит |
|------|-----------|
| `statham:trade_history` | Список закрытых сделок (list) |
| `statham:positions` | Активные позиции (dict) |
| `statham:trade_stats` | Агрегированная статистика (dict) |
| `statham:active_trades` | Активные сделки (dict) |

Дашборд работает **только на чтение** — данные в Redis не изменяет.
