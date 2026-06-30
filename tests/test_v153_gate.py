import copy
import importlib.util
import pathlib
import ast
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
PINE_ROOT = ROOT if (ROOT / "last.txt").exists() else ROOT.parent


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BotScenarioGate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bot = load_module("statham_v153_app", ROOT / "app.py")

    def setUp(self):
        m = self.bot
        self.positions = {}
        self.trades = {}
        self.history = []
        self.closed = {}
        self.stats = {"wins": 0, "losses": 0, "total": 0}

        m.load_positions = lambda: copy.deepcopy(self.positions)
        m.save_positions = lambda value: self._replace_dict(self.positions, value)
        m.load_trades = lambda: copy.deepcopy(self.trades)
        m.save_trades = lambda value: self._replace_dict(self.trades, value)
        m.load_history = lambda: copy.deepcopy(self.history)
        m.save_history = lambda value: self._replace_list(self.history, value)
        m.load_closed_trades = lambda: copy.deepcopy(self.closed)
        m.save_closed_trades = lambda value: self._replace_dict(self.closed, value)
        m.load_stats = lambda: copy.deepcopy(self.stats)
        m.save_stats = lambda value: self._replace_dict(self.stats, value)
        m.send_signals = lambda *args, **kwargs: {}
        m.cancel_own_orders = lambda *args, **kwargs: None
        m.ex_min_qty = lambda *args, **kwargs: 0.0
        m.write_log = lambda *args, **kwargs: None

    @staticmethod
    def _replace_dict(target, value):
        target.clear()
        target.update(copy.deepcopy(value))

    @staticmethod
    def _replace_list(target, value):
        target[:] = copy.deepcopy(value)

    def seed(self, ticker="TESTUSDT", direction="BUY", trade_id="trade-1"):
        m = self.bot
        pkey = m.pos_key(ticker, direction)
        pos = {
            "symbol": ticker,
            "ticker": ticker,
            "direction": direction,
            "opp_side": "Sell" if direction == "BUY" else "Buy",
            "exchange": "none",
            "trade_mode": "telegram_only",
            "entry_price": 100.0,
            "total_qty": 10.0,
            "remaining_qty": 10.0,
            "leverage": 1,
            "sl_price": 95.0 if direction == "BUY" else 105.0,
            "tp1_price": 101.0 if direction == "BUY" else 99.0,
            "tp2_price": 102.0 if direction == "BUY" else 98.0,
            "tp3_price": 103.0 if direction == "BUY" else 97.0,
            "tp4_price": 104.0 if direction == "BUY" else 96.0,
            "use_exchange_tps": False,
            "trade_id": trade_id,
            "trade_key": trade_id,
            "instance_id": trade_id,
            "timeframe": "15",
            "created_at": 1_700_000_000,
            "trail_active": False,
            "be_active": False,
            "strategy_version": "v153",
            "schema_version": 2,
            "tp_contract": "4TP_20_25_30_25",
        }
        self.positions[pkey] = copy.deepcopy(pos)
        self.trades[trade_id] = {
            "trade_id": trade_id,
            "trade_key": trade_id,
            "instance_id": trade_id,
            "ticker": ticker,
            "direction": direction,
            "timeframe": "15",
            "created_at": pos["created_at"],
            "entry_price": 100.0,
            "strategy_version": "v153",
            "schema_version": 2,
            "tp_contract": "4TP_20_25_30_25",
        }
        return {"ticker": ticker, "direction": direction, "timeframe": "15", "trade_id": trade_id}

    def test_sl_only(self):
        payload = self.seed()
        payload.update({"event": "sl_hit", "text": "Цена выхода: 95"})
        self.bot.handle_sl_hit(payload)
        self.assertEqual(self.history[-1]["result"], "loss")
        self.assertEqual(self.history[-1]["highest_tp_hit"], 0)
        self.assertFalse(self.positions)

    def test_entry_analytics_metadata_reaches_history(self):
        payload = self.seed()
        pkey = next(iter(self.positions))
        self.positions[pkey].update({
            "entry_mode": "TBS_RETEST", "score": 47.5,
            "confirmations": 4, "atr_pct": 1.25, "amd_phase": "ACCUMULATION",
        })
        self.trades["trade-1"].update({
            "entry_mode": "TBS_RETEST", "score": 47.5,
            "confirmations": 4, "atr_pct": 1.25, "amd_phase": "ACCUMULATION",
        })
        self.bot.handle_sl_hit({**payload, "event": "sl_hit", "text": "Цена выхода: 95"})
        row = self.history[-1]
        self.assertEqual(row["entry_mode"], "TBS_RETEST")
        self.assertEqual(row["score"], 47.5)
        self.assertEqual(row["confirmations"], 4)
        self.assertEqual(row["atr_pct"], 1.25)
        self.assertEqual(row["amd_phase"], "ACCUMULATION")

    def test_missing_trade_record_is_recovered_from_position(self):
        self.seed(trade_id="recover-me")
        self.trades.clear()
        recovered = self.bot._recover_missing_trade_records()
        self.assertEqual(recovered, 1)
        self.assertIn("recover-me", self.trades)
        self.assertTrue(self.trades["recover-me"]["state_recovered"])

    def test_cleanup_preserves_old_trade_with_live_position(self):
        self.seed(trade_id="old-live")
        self.trades["old-live"]["created_at"] = 1
        removed = self.bot.cleanup_old_trades()
        self.assertEqual(removed, 0)
        self.assertIn("old-live", self.trades)

    def test_tp1_then_sl_is_partial_but_not_implicit_be(self):
        payload = self.seed()
        self.bot.handle_tp_hit({**payload, "event": "tp_hit", "tp_num": 1})
        pos = next(iter(self.positions.values()))
        self.assertAlmostEqual(pos["remaining_qty"], 8.0)
        self.assertFalse(pos["be_active"])
        self.bot.handle_sl_hit({**payload, "event": "sl_hit", "text": "Цена выхода: 95"})
        self.assertEqual(self.history[-1]["result"], "partial")
        self.assertFalse(self.history[-1]["be_active"])

    def test_tp1_to_tp4_terminal_and_idempotent(self):
        payload = self.seed()
        for level in (1, 2, 3, 4):
            self.bot.handle_tp_hit({**payload, "event": "tp_hit", "tp_num": level})
        self.assertFalse(self.positions)
        self.assertEqual(len(self.history), 1)
        self.assertEqual(self.history[0]["highest_tp_hit"], 4)
        self.assertEqual(self.history[0]["close_reason"], "tp_hit_4")
        self.bot.handle_tp_hit({**payload, "event": "tp_hit", "tp_num": 4})
        self.assertEqual(len(self.history), 1)

    def test_missing_tp_events_are_recovered(self):
        payload = self.seed()
        self.bot.handle_tp_hit({**payload, "event": "tp_hit", "tp_num": 3})
        pos = next(iter(self.positions.values()))
        self.assertTrue(all(pos[f"tp{n}_hit"] for n in (1, 2, 3)))
        self.assertAlmostEqual(pos["remaining_qty"], 2.5)

    def test_explicit_be_only_when_sl_moves_to_entry(self):
        payload = self.seed()
        self.bot.handle_sl_moved({
            **payload, "event": "sl_moved", "new_sl": 99.0,
            "be_active": True, "trail_active": False, "sl_reason": "not entry",
        })
        self.assertFalse(next(iter(self.positions.values()))["be_active"])
        self.bot.handle_sl_moved({
            **payload, "event": "sl_moved", "new_sl": 100.0,
            "be_active": True, "trail_active": False, "sl_reason": "TP1→Entry",
        })
        self.assertTrue(next(iter(self.positions.values()))["be_active"])

    def test_trail_requires_sl_moved_event(self):
        payload = self.seed()
        self.bot.handle_tp_hit({**payload, "event": "tp_hit", "tp_num": 2})
        self.assertFalse(next(iter(self.positions.values()))["trail_active"])
        self.bot.handle_sl_moved({
            **payload, "event": "sl_moved", "new_sl": 101.5,
            "be_active": False, "trail_active": True, "sl_reason": "ATR Trail",
        })
        self.assertTrue(next(iter(self.positions.values()))["trail_active"])

    def test_move_sl_preserves_tp_orders(self):
        m = self.bot
        cancelled = []
        m.bingx_cancel_order_by_id = lambda ticker, order_id: cancelled.append(str(order_id))
        m.ex_place_stop = lambda *args, **kwargs: {"data": {"order": {"orderId": "new-sl"}}}
        m.time.sleep = lambda *_: None
        pos = {
            "exchange": "bingx", "symbol": "TESTUSDT", "opp_side": "Sell",
            "remaining_qty": 8.0, "total_qty": 10.0, "sl_order_id": "old-sl",
            "tp_order_ids": {1: "tp-1", 2: "tp-2", 3: "tp-3", 4: "tp-4"},
        }
        self.assertEqual(m.move_sl(pos, 100.0), "new-sl")
        self.assertEqual(cancelled, ["old-sl"])
        self.assertEqual(len(pos["tp_order_ids"]), 4)

    def test_flip_directions_have_independent_terminal_ids(self):
        long_payload = self.seed(direction="BUY", trade_id="flip-long")
        short_payload = self.seed(direction="SELL", trade_id="flip-short")
        self.bot.handle_sl_hit({**long_payload, "event": "sl_hit", "text": "Цена выхода: 95"})
        self.bot.handle_tp_hit({**short_payload, "event": "tp_hit", "tp_num": 4})
        self.assertEqual(len(self.history), 2)
        self.assertEqual({r["instance_id"] for r in self.history}, {"flip-long", "flip-short"})

    def test_tp5_is_rejected(self):
        payload = self.seed()
        self.bot.handle_tp_hit({**payload, "event": "tp_hit", "tp_num": 5})
        self.assertTrue(self.positions)
        self.assertFalse(self.history)

    def test_legacy_entry_is_not_upgraded_by_v153_close_payload(self):
        payload = self.seed(trade_id="legacy-open")
        pkey = next(iter(self.positions))
        for field in ("strategy_version", "schema_version", "tp_contract"):
            self.positions[pkey].pop(field, None)
            self.trades["legacy-open"].pop(field, None)
        self.bot.handle_sl_hit({
            **payload, "event": "sl_hit", "text": "Цена выхода: 95",
            "strategy_version": "v153", "schema_version": 2,
            "tp_contract": "4TP_20_25_30_25",
        })
        self.assertEqual(self.history[-1]["strategy_version"], "legacy")
        self.assertEqual(self.history[-1]["schema_version"], 1)


class StaticPineGate(unittest.TestCase):
    def test_contract_in_both_pine_files(self):
        for filename in ("last.txt", "2.txt"):
            source = (PINE_ROOT / filename).read_text(encoding="utf-8")
            self.assertTrue(source.startswith("//@version=6"))
            self.assertIn("SMC v153", source)
            self.assertIn('"strategy_version\\\":\\\"v153', source)
            self.assertIn('"schema_version\\\":2', source)
            self.assertIn('"tp_contract\\\":\\\"4TP_20_25_30_25', source)
            self.assertNotIn('"tp5\\\":"', source)
            self.assertNotIn('"tp6\\\":"', source)
            self.assertIn('qty_percent=20', source)
            self.assertIn('qty_percent=25', source)
            self.assertIn('qty_percent=30', source)
            self.assertNotIn('not use_6_tp_levels and _hit_tp_num >= 1', source)
            self.assertIn('f_tg_emit_sl_moved', source)
            self.assertIn('"entry_mode\\\":\\\"" + _entry_mode', source)
            self.assertIn('"amd_phase\\\":\\\"" + f_json_escape(amd_phase)', source)

    def test_all_declared_inputs_are_referenced(self):
        import re
        for filename in ("last.txt", "2.txt"):
            source = (PINE_ROOT / filename).read_text(encoding="utf-8")
            names = re.findall(
                r"(?m)^\s*(?:bool|int|float|string|color)\s+(\w+)\s*=\s*input\.",
                source,
            )
            unused = [name for name in names if len(re.findall(rf"\b{re.escape(name)}\b", source)) < 2]
            self.assertEqual(unused, [], f"{filename}: unused inputs {unused}")

    def test_same_bar_tp_sl_uses_conservative_sl_priority(self):
        for filename in ("last.txt", "2.txt"):
            source = (PINE_ROOT / filename).read_text(encoding="utf-8")
            self.assertIn("float sl_at_bar_start = active_sl", source)
            self.assertIn("bool sl_touched_before_tp", source)
            self.assertIn("if not sl_touched_before_tp", source)
            self.assertIn("bool sl_hit = sl_touched_before_tp", source)

    def test_active_levels_have_midbar_reload_watchdog(self):
        for filename in ("last.txt", "2.txt"):
            source = (PINE_ROOT / filename).read_text(encoding="utf-8")
            self.assertIn("var int last_trade_draw_bar = na", source)
            self.assertIn("na(last_trade_draw_bar) or bar_index != last_trade_draw_bar", source)
            self.assertIn("last_trade_draw_bar := bar_index", source)

    def test_tbs_visuals_only_mark_new_candidates(self):
        for filename in ("last.txt", "2.txt"):
            source = (PINE_ROOT / filename).read_text(encoding="utf-8")
            self.assertIn("bool tbs_bull_new = tbs_bull_signal and not tbs_bull_signal[1]", source)
            self.assertIn("bool tbs_bear_new = tbs_bear_signal and not tbs_bear_signal[1]", source)
            self.assertIn("if tbs_bull_new", source)
            self.assertIn("if tbs_bear_new", source)

    def test_two_txt_has_no_out_of_scope_legacy_tp_draw(self):
        source = (PINE_ROOT / "2.txt").read_text(encoding="utf-8")
        reset_at = source.index("if barstate.islast and (na(active_sl) or not lines_active)")
        execution_at = source.index("if not na(active_sl) and lines_active", reset_at)
        between = source[reset_at:execution_at]
        self.assertNotIn("tp5_line  := line.new", between)
        self.assertNotIn("tp6_line  := line.new", between)


class AdminAclGate(unittest.TestCase):
    def test_every_telegram_command_has_admin_guard(self):
        source = (ROOT / "app.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        unguarded = []
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            is_command = any(
                isinstance(dec, ast.Call)
                and isinstance(dec.func, ast.Attribute)
                and dec.func.attr == "message_handler"
                and any(kw.arg == "commands" for kw in dec.keywords)
                for dec in node.decorator_list
            )
            if is_command and "is_admin_user" not in (ast.get_source_segment(source, node) or ""):
                unguarded.append(node.name)
        self.assertEqual(unguarded, [])

    def test_admin_user_ids_env_is_supported(self):
        source = (ROOT / "app.py").read_text(encoding="utf-8")
        self.assertIn('os.environ.get("ADMIN_USER_IDS"', source)
        self.assertIn("_EXCHANGE_SYNC_INTERVAL = 3600", source)


class DashboardGate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dashboard = load_module("statham_v153_dashboard", ROOT / "dashboard" / "app.py")

    def test_version_filter_and_duplicate_terminal_quality(self):
        records = [
            {"instance_id": "a", "strategy_version": "legacy", "close_time": 10, "direction": "BUY", "result": "loss", "close_reason": "sl_hit", "pnl": {"pnl_pct": -1}},
            {"instance_id": "b", "strategy_version": "v153", "close_time": 11, "direction": "BUY", "result": "win", "close_reason": "tp_hit_4", "highest_tp_hit": 4, "pnl": {"pnl_pct": 4, "highest_tp": 4}},
            {"instance_id": "b", "strategy_version": "v153", "close_time": 12, "direction": "BUY", "result": "win", "close_reason": "tp_hit_4", "highest_tp_hit": 4, "pnl": {"pnl_pct": 4, "highest_tp": 4}},
        ]
        filtered = self.dashboard._filter_history(records, "all", "BOTH", "v153")
        self.assertEqual(len(filtered), 2)
        stats = self.dashboard._calc_stats(filtered)
        self.assertEqual(stats["quality"]["duplicate_terminal_events"], 1)
        self.assertEqual(stats["quality"]["strategy_versions"], {"v153": 2})

    def test_stats_alias_is_registered(self):
        rules = {rule.rule for rule in self.dashboard.app.url_map.iter_rules()}
        self.assertIn("/stats", rules)
        self.assertIn("/api/stats", rules)

    def test_entry_mode_filter_and_quality_breakdown(self):
        records = [
            {"close_time": 1, "direction": "BUY", "result": "loss", "entry_mode": "TBS_RETEST", "amd_phase": "ACCUMULATION", "pnl": {"pnl_pct": -1}},
            {"close_time": 2, "direction": "BUY", "result": "win", "entry_mode": "STRONG", "amd_phase": "DISTRIBUTION", "pnl": {"pnl_pct": 2}},
        ]
        filtered = self.dashboard._filter_history(records, "all", "BOTH", "", "TBS_RETEST")
        self.assertEqual(len(filtered), 1)
        stats = self.dashboard._calc_stats(records)
        self.assertEqual(stats["quality"]["entry_modes"], {"TBS_RETEST": 1, "STRONG": 1})
        self.assertEqual(stats["quality"]["amd_phases"], {"ACCUMULATION": 1, "DISTRIBUTION": 1})

    def test_dashboard_renders_entry_mode_and_analytics_directly(self):
        source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")
        self.assertIn("const modeParts = [p.entry_mode", source)
        self.assertIn("Score / ATR", source)
        self.assertNotIn("let modeStr = p.entry_mode || p.strategy || p.smc ?", source)


class OperationalApiSecurityGate(unittest.TestCase):
    def test_operational_endpoints_require_secret(self):
        bot = load_module("statham_v153_security", ROOT / "app.py")
        bot.RENDER_SECRET = "gate-secret"
        client = bot.app.test_client()
        self.assertEqual(client.get("/trades").status_code, 403)
        self.assertEqual(client.get("/positions").status_code, 403)
        self.assertEqual(client.get("/trades?secret=wrong").status_code, 403)
        self.assertEqual(client.get("/trades?secret=gate-secret").status_code, 200)
        self.assertEqual(client.get("/health").status_code, 200)


if __name__ == "__main__":
    unittest.main()
