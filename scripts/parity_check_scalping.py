"""Paridad reglas VIVAS (unified_brain/src/scalping_rules.py) vs backtest validado.

Corre el backtest real (bsa.simular_ventana, sin modificar) y, sobre los mismos
datos, una replica del mismo loop que delega guardas de entrada, whitelist de
patrones y gestion de la posicion en scalping_rules (lo que usa el bot en
vivo). Compara trade por trade. Uso:
    python scripts/parity_check_scalping.py            # escenario validado ($50k, paso 0.001)
    python scripts/parity_check_scalping.py --small    # escenario $200 / 6.25% / paso 0.01
"""
from __future__ import annotations

import argparse
import importlib.util
import math
import sys
from pathlib import Path
from types import MethodType

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

spec = importlib.util.spec_from_file_location("scalping_rules", REPO_ROOT / "unified_brain" / "src" / "scalping_rules.py")
sr = importlib.util.module_from_spec(spec)
sys.modules["scalping_rules"] = sr
spec.loader.exec_module(sr)

import backtest_dynamic_risk as bdr  # noqa: E402
import backtest_standard_account as bsa  # noqa: E402
import backtest_sweep_orderflow as bso  # noqa: E402
from config import constants as c  # noqa: E402

ofs = bsa.ofs


class LiveRealisticBracket(sr.Bracket):
    """Como Bracket, pero si tras un TIME_STOP_TP1 el breakeven queda del lado
    equivocado del precio (el broker no admite ese SL), el resto se cierra a
    mercado en ese mismo cierre en vez de 'rellenar' en el BE (supuesto del backtest)."""

    def on_bar(self, ts, high, low, close):
        ev = super().on_bar(ts, high, low, close)
        if any(e.kind == "TIME_STOP_TP1" for e in ev) and not self.closed and self.qty_open > 0:
            wrong = self.be_price >= close if self.direction == "LONG" else self.be_price <= close
            if wrong:
                ev.append(sr.Event("BE_WRONG_SIDE", close, self.qty_open))
                self.closed = True
        return ev


class IntendedRulesBracket(sr.Bracket):
    """Regla tal como la describe decide()::management_notes: el SL pasa a BE
    SOLO al tocar TP1. Tras el cierre del 50% por time-stop (sin TP1) el
    resto conserva su SL original (ejecutable en cualquier broker)."""

    def on_bar(self, ts, high, low, close):
        sl0 = self.sl
        ev = super().on_bar(ts, high, low, close)
        if any(e.kind == "TIME_STOP_TP1" for e in ev) and not any(e.kind == "TP1_PARCIAL" for e in ev):
            self.sl = sl0
        return ev


def simular_con_reglas_vivas(df, risk_manager, bracket_cls=None):
    bracket_cls = bracket_cls or sr.Bracket
    ofs._ZONE_COOLDOWNS.clear()
    gate = sr.EntryGate()
    params = sr.BracketParams(
        be_buffer=c.BREAKEVEN_BUFFER_PIPS, time_stop_tp1_min=c.TIME_STOP_TP1_MINUTES,
        time_stop_close_min=c.TIME_STOP_CLOSE_MINUTES,
    )
    equity = bsa.CAPITAL_BASE
    trades, posicion, daily_pnl = [], None, {}

    filas = df.to_dict("records")
    for i, fila in enumerate(filas):
        ts = fila["open_time"]
        vol_regime = "HIGH" if (bsa.pd.notna(fila.get("atr14_15m")) and bsa.pd.notna(fila.get("atr14_15m_median200"))
                                 and fila["atr14_15m_median200"] > 0
                                 and fila["atr14_15m"] > 1.5 * fila["atr14_15m_median200"]) else "NORMAL"

        if posicion is not None:
            b = posicion["_bracket"]
            costs = bsa.get_dynamic_costs_usdt(ts, vol_regime, fila["close"])
            exit_cost = costs["exit_cost_usdt"]
            direccion = posicion["direccion"]
            for ev in b.on_bar(ts.timestamp(), fila["high"], fila["low"], fila["close"]):
                precio_real = ev.price - exit_cost if direccion == "LONG" else ev.price + exit_cost
                signo = 1 if direccion == "LONG" else -1
                posicion["pnl_usdt"] += signo * (precio_real - posicion["entry_price"]) * ev.qty
                posicion["costos_usdt"] += exit_cost * ev.qty
                if ev.kind in ("TP1_PARCIAL", "TIME_STOP_TP1"):
                    posicion["motivo_cierre_parcial"] = ev.kind
                    posicion.pop("motivo_cierre", None)
                else:
                    posicion.setdefault("motivo_cierre", ev.kind)
            posicion["tp1_hit"] = b.tp1_hit
            posicion["qty_abierta"] = b.qty_open
            posicion["sl_price"] = b.sl
            if b.closed:
                posicion["cerrada"] = True
                posicion["exit_ts"] = ts
                equity += posicion["pnl_usdt"]
                day = ts.date()
                daily_pnl[day] = daily_pnl.get(day, 0.0) + posicion["pnl_usdt"]
                if posicion.get("motivo_cierre") == "SL":
                    gate.record_sl_exit(ts.timestamp())
                trades.append(posicion)
                posicion = None
            continue

        ok, _ = gate.allow(ts.hour, ts.timestamp(), position_open=False)
        if not ok:
            continue
        day = ts.date()
        if daily_pnl.get(day, 0.0) <= -bsa.DAILY_LOSS_LIMIT * bsa.CAPITAL_BASE:
            continue
        if bsa.pd.isna(fila.get("poc")) or bsa.pd.isna(fila.get("atr14_15m")):
            continue

        costs = bsa.get_dynamic_costs_usdt(ts, vol_regime, fila["close"])
        if costs["mult"] > bsa.MAX_SPREAD_MULT:
            continue

        htf = {
            "poc": fila.get("poc"), "vah": fila.get("vah"), "val": fila.get("val"),
            "swing_high_h4": fila.get("swing_high_h4"), "swing_low_h4": fila.get("swing_low_h4"),
            "cvd_trend": fila.get("cvd_trend"),
        }
        payload = {"htf_ready": True, "snapshot": {"htf": htf, "ltf_1m": bsa._build_ltf_1m(filas, i)}}
        senal = ofs.decide(payload, equity, risk_manager)
        if senal["decision"] not in ("LONG", "SHORT"):
            continue
        if senal["setup_type"] not in sr.ALLOWED_PATTERNS:
            continue

        tps = senal["tp_levels"]
        if len(tps) < 2 or i + 1 >= len(filas):
            continue

        fila_fill = filas[i + 1]
        direccion = senal["decision"]
        entry_real = fila_fill["open"] + costs["entry_cost_usdt"] if direccion == "LONG" else fila_fill["open"] - costs["entry_cost_usdt"]
        qty_total = round((senal["position_size_usdt"] / entry_real) / c.STEP_SIZE) * c.STEP_SIZE
        if qty_total <= 0:
            continue
        qty_tp1 = sr.split_qty(qty_total, tps[0].get("size_pct", 50) / 100, c.STEP_SIZE)

        posicion = {
            "direccion": direccion, "setup_type": senal["setup_type"], "conviction": senal["conviction"],
            "entry_price": entry_real, "sl_price": senal["stop_loss_price"], "tp1_price": tps[0]["price"],
            "tp2_price": tps[1]["price"], "qty_total": qty_total, "qty_tp1": qty_tp1, "qty_abierta": qty_total,
            "tp1_hit": False, "pnl_usdt": 0.0, "costos_usdt": costs["entry_cost_usdt"] * qty_total,
            "entry_ts": fila_fill["open_time"], "cerrada": False,
            "position_size_usdt": senal["position_size_usdt"],
            "risk_usdt": abs(entry_real - senal["stop_loss_price"]) * qty_total,
            "_bracket": bracket_cls(direccion, entry_real, senal["stop_loss_price"], tps[0]["price"], tps[1]["price"],
                                   qty_total, qty_tp1, fila_fill["open_time"].timestamp(), params),
        }
        gate.record_entry(fila_fill["open_time"].timestamp())

    return [t for t in trades if t.get("motivo_cierre")]


def comparar(ref: list, vivo: list) -> int:
    print(f"trades backtest={len(ref)}  reglas_vivas={len(vivo)}")
    diffs = 0
    for k in range(max(len(ref), len(vivo))):
        if k >= len(ref) or k >= len(vivo):
            diffs += 1
            print(f"  #{k}: falta en {'reglas_vivas' if k >= len(vivo) else 'backtest'}")
            continue
        a, b = ref[k], vivo[k]
        mismo = (
            a["entry_ts"] == b["entry_ts"] and a["exit_ts"] == b["exit_ts"] and a["direccion"] == b["direccion"]
            and a["setup_type"] == b["setup_type"] and a["motivo_cierre"] == b["motivo_cierre"]
            and a["tp1_hit"] == b["tp1_hit"] and math.isclose(a["qty_total"], b["qty_total"], abs_tol=1e-9)
            and math.isclose(a["pnl_usdt"], b["pnl_usdt"], abs_tol=1e-6)
            and math.isclose(a["costos_usdt"], b["costos_usdt"], abs_tol=1e-6)
        )
        if not mismo:
            diffs += 1
            if diffs <= 5:
                print(f"  #{k} DIFIERE\n    backtest : {a['entry_ts']} {a['direccion']} {a['setup_type']} {a['motivo_cierre']} pnl={a['pnl_usdt']:.6f}\n    vivo     : {b['entry_ts']} {b['direccion']} {b['setup_type']} {b['motivo_cierre']} pnl={b['pnl_usdt']:.6f}")
    return diffs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live-realistic", action="store_true", help="sensibilidad: BE inejecutable tras time-stop -> cierre a mercado")
    ap.add_argument("--intended", action="store_true", help="sensibilidad: SL a BE solo al tocar TP1 (management_notes de decide())")
    ap.add_argument("--small", action="store_true", help="$200, riesgo 6.25%%, paso 0.01 (regimen real del bot)")
    args = ap.parse_args()

    df_1m, windows = bso.precompute("EXNESS_ZERO")
    bsa._override_tp_targets()

    previos_bsa = bso.apply_config(bdr.PROD_CONFIG)
    previos_sr = sr.apply_validated_constants(c)  # debe ser no-op sobre lo validado
    try:
        cambiados = {k: (previos_sr[k], v) for k, v in sr.VALIDATED_CONSTANTS.items() if previos_sr[k] != v}
        print(f"constantes validadas que CAMBIAN respecto de las locales del backtest: {cambiados or 'ninguna'}")

        if args.small:
            capital, risk_pct = 200.0, 0.0625
            bsa.CAPITAL_BASE = capital
            c.STEP_SIZE = bdr.STEP_SIZE_REAL_EXNESS
            rm = bsa.RiskManager(None, capital_inicial=capital)
            rm.calcular_tamano_posicion = MethodType(bdr._tamano_dinamico_factory(risk_pct), rm)
            print(f"escenario: ${capital:.0f}, riesgo {risk_pct:.2%}, paso {c.STEP_SIZE}")
        else:
            rm = bsa.RiskManager(None, capital_inicial=bsa.CAPITAL_BASE)
            print(f"escenario: ${bsa.CAPITAL_BASE:.0f}, paso {c.STEP_SIZE}")

        ref, vivo = [], []
        for (ini, fin) in windows:
            dfw = df_1m[(df_1m["open_time"] >= ini) & (df_1m["open_time"] < fin)].reset_index(drop=True)
            ref.extend(bsa.simular_ventana(dfw, rm, regime_atr_pctl=None, vd_threshold=None, trailing_mult=None)[0])
            vivo.extend(simular_con_reglas_vivas(dfw, rm, LiveRealisticBracket if args.live_realistic else (IntendedRulesBracket if args.intended else None)))

        if args.live_realistic or args.intended:
            dias = (windows[-1][1] - windows[0][0]).total_seconds() / 86400
            for nombre, tr in (("backtest validado", ref), ("variante", vivo)):
                m = bsa.calcular_metricas(tr, dias)
                cnt = {}
                for t in tr:
                    cnt[t["motivo_cierre"]] = cnt.get(t["motivo_cierre"], 0) + 1
                print(f"{nombre:18s}: trades={m['total_trades']} WR={m['win_rate']:.4f} PF={m['profit_factor']:.4f} DD={m['max_dd_pct']:.4f} pnl_total={sum(t['pnl_usdt'] for t in tr):.2f} cierres={cnt}")
            return 0

        diffs = comparar(ref, vivo)
        m = bsa.calcular_metricas(vivo, (windows[-1][1] - windows[0][0]).total_seconds() / 86400)
        print(f"reglas_vivas: trades={m['total_trades']} WR={m['win_rate']:.4f} PF={m['profit_factor']:.4f} DD={m['max_dd_pct']:.4f}")
        print("PARIDAD OK: 0 diferencias" if diffs == 0 else f"PARIDAD FALLA: {diffs} diferencias")
        return 0 if diffs == 0 else 1
    finally:
        bso.restore_config(previos_bsa)


if __name__ == "__main__":
    sys.exit(main())
