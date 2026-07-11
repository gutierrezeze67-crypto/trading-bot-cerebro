"""
╔══════════════════════════════════════════════════════════╗
║  SCANNER SENSEI V3 - Smart Money Concepts              ║
║  Detecta: CHoCH, BOS, FVG, OB, SVP Fractales           ║
╚══════════════════════════════════════════════════════════╝
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import SCANNER_CONFIG, SYMBOLS, PRIMARY_TF

class ScannerSensei:
    def __init__(self, config: dict = None):
        self.cfg = config or SCANNER_CONFIG
        self.data = None
        self.levels = {}
        self.signals = []
        
    def load_csv(self, filepath: str) -> pd.DataFrame:
        """Carga CSV de datos OHLCV."""
        df = pd.read_csv(filepath)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y.%m.%d %H:%M')
        df.set_index('Timestamp', inplace=True)
        self.data = df
        print(f"✅ Datos cargados: {len(df)} velas | {df.index[0]} → {df.index[-1]}")
        return df
    
    def detect_swings(self, period: int = None) -> Tuple[List, List]:
        """Detecta swing highs y swing lows (HH, HL, LH, LL)."""
        period = period or self.cfg.get("hh_ll_period", 70)
        df = self.data
        
        highs = []
        lows = []
        
        for i in range(period, len(df) - period):
            # Swing High
            if df['High'].iloc[i] == df['High'].iloc[i-period:i+period+1].max():
                highs.append((df.index[i], df['High'].iloc[i]))
            # Swing Low
            if df['Low'].iloc[i] == df['Low'].iloc[i-period:i+period+1].min():
                lows.append((df.index[i], df['Low'].iloc[i]))
        
        self.levels['highs'] = highs
        self.levels['lows'] = lows
        print(f"🔍 Swings: {len(highs)} highs, {len(lows)} lows")
        return highs, lows
    
    def detect_bos(self, lookback: int = None) -> List[Dict]:
        """Detecta Break of Structure (BOS)."""
        lookback = lookback or self.cfg.get("bos_lookback", 20)
        highs = self.levels.get('highs', [])
        lows = self.levels.get('lows', [])
        bos_signals = []
        
        # BOS Alcista: rompe ultimo HH
        if len(highs) >= 2:
            for i in range(1, len(highs)):
                if highs[i][1] > highs[i-1][1]:
                    bos_signals.append({
                        'type': 'BOS_BULLISH',
                        'time': highs[i][0],
                        'price': highs[i][1],
                        'prev_high': highs[i-1][1]
                    })
        
        # BOS Bajista: rompe ultimo LL
        if len(lows) >= 2:
            for i in range(1, len(lows)):
                if lows[i][1] < lows[i-1][1]:
                    bos_signals.append({
                        'type': 'BOS_BEARISH',
                        'time': lows[i][0],
                        'price': lows[i][1],
                        'prev_low': lows[i-1][1]
                    })
        
        self.signals.extend(bos_signals)
        print(f"🔍 BOS: {len(bos_signals)} detectados")
        return bos_signals
    
    def detect_choch(self, window: int = None) -> List[Dict]:
        """Detecta Change of Character (CHoCH)."""
        window = window or self.cfg.get("choch_window", 20)
        df = self.data
        choch_signals = []
        
        for i in range(window, len(df) - window):
            segment = df.iloc[i-window:i+window]
            
            # CHoCH Alcista: rompe estructura bajista
            if df['Close'].iloc[i] > segment['High'].iloc[:window].max():
                prev_low = segment['Low'].iloc[:window].min()
                choch_signals.append({
                    'type': 'CHoCH_BULLISH',
                    'time': df.index[i],
                    'price': df['Close'].iloc[i],
                    'break_level': prev_low
                })
            
            # CHoCH Bajista: rompe estructura alcista
            if df['Close'].iloc[i] < segment['Low'].iloc[:window].min():
                prev_high = segment['High'].iloc[:window].max()
                choch_signals.append({
                    'type': 'CHoCH_BEARISH',
                    'time': df.index[i],
                    'price': df['Close'].iloc[i],
                    'break_level': prev_high
                })
        
        self.signals.extend(choch_signals)
        print(f"🔍 CHoCH: {len(choch_signals)} detectados")
        return choch_signals
    
    def detect_fvg(self) -> List[Dict]:
        """Detecta Fair Value Gaps (FVG)."""
        df = self.data
        fvg_list = []
        
        for i in range(2, len(df)):
            # FVG Alcista: Low[2] > High[0]
            if df['Low'].iloc[i] > df['High'].iloc[i-2]:
                fvg_list.append({
                    'type': 'FVG_BULLISH',
                    'time': df.index[i],
                    'gap_high': df['Low'].iloc[i],
                    'gap_low': df['High'].iloc[i-2],
                    'size': df['Low'].iloc[i] - df['High'].iloc[i-2]
                })
            
            # FVG Bajista: High[2] < Low[0]
            if df['High'].iloc[i] < df['Low'].iloc[i-2]:
                fvg_list.append({
                    'type': 'FVG_BEARISH',
                    'time': df.index[i],
                    'gap_high': df['Low'].iloc[i-2],
                    'gap_low': df['High'].iloc[i],
                    'size': df['Low'].iloc[i-2] - df['High'].iloc[i]
                })
        
        # Filtrar FVGs relevantes (gap significativo)
        atr = self.calc_atr()
        fvg_filtered = [f for f in fvg_list if f['size'] > atr * 0.3]
        
        self.signals.extend(fvg_filtered)
        print(f"🔍 FVG: {len(fvg_filtered)} relevantes (de {len(fvg_list)} totales)")
        return fvg_filtered
    
    def detect_order_blocks(self) -> List[Dict]:
        """Detecta Order Blocks (OB)."""
        df = self.data
        ob_list = []
        
        for i in range(2, len(df)):
            # OB Alcista: ultima vela bajista antes de impulso alcista
            if df['Close'].iloc[i] > df['Open'].iloc[i] and df['Close'].iloc[i-1] < df['Open'].iloc[i-1]:
                if df['Close'].iloc[i] > df['High'].iloc[i-1]:
                    ob_list.append({
                        'type': 'OB_BULLISH',
                        'time': df.index[i-1],
                        'ob_high': df['High'].iloc[i-1],
                        'ob_low': df['Low'].iloc[i-1],
                        'broken_time': df.index[i]
                    })
            
            # OB Bajista: ultima vela alcista antes de impulso bajista
            if df['Close'].iloc[i] < df['Open'].iloc[i] and df['Close'].iloc[i-1] > df['Open'].iloc[i-1]:
                if df['Close'].iloc[i] < df['Low'].iloc[i-1]:
                    ob_list.append({
                        'type': 'OB_BEARISH',
                        'time': df.index[i-1],
                        'ob_high': df['High'].iloc[i-1],
                        'ob_low': df['Low'].iloc[i-1],
                        'broken_time': df.index[i]
                    })
        
        self.signals.extend(ob_list)
        print(f"🔍 OB: {len(ob_list)} detectados")
        return ob_list
    
    def calc_atr(self, length: int = None) -> float:
        """Calcula ATR promedio."""
        length = length or self.cfg.get("atr_length", 14)
        df = self.data
        high, low, close = df['High'], df['Low'], df['Close']
        tr = pd.DataFrame({
            'tr1': high - low,
            'tr2': abs(high - close.shift()),
            'tr3': abs(low - close.shift())
        }).max(axis=1)
        return tr.rolling(length).mean().iloc[-1]
    
    def detect_svp_fractals(self) -> Dict:
        """Detecta fractales SVP (Session Volume Profile)."""
        df = self.data
        # POC (Point of Control) simplificado
        poc = df['Close'].mode().iloc[0] if len(df['Close'].mode()) > 0 else df['Close'].median()
        vah = df['High'].quantile(0.70)
        val = df['Low'].quantile(0.30)
        
        svp = {
            'POC': round(poc, 2),
            'VAH': round(vah, 2),
            'VAL': round(val, 2),
            'range': f"{round(val, 2)} - {round(vah, 2)}"
        }
        
        print(f"🔍 SVP: POC={svp['POC']}, VAH={svp['VAH']}, VAL={svp['VAL']}")
        return svp
    
    def run_full_scan(self, filepath: str) -> Dict:
        """Ejecuta escaneo completo y devuelve todos los resultados."""
        self.load_csv(filepath)
        
        # PRIMERO detectar swings (necesarios para BOS)
        self.detect_swings()
        
        results = {
            'symbol': Path(filepath).stem,
            'timestamp_scan': datetime.now().isoformat(),
            'total_velas': len(self.data),
            'atr': round(self.calc_atr(), 2),
            'swings': {'highs': len(self.levels.get('highs', [])), 'lows': len(self.levels.get('lows', []))},
            'bos': self.detect_bos(),
            'choch': self.detect_choch(),
            'fvg': self.detect_fvg(),
            'order_blocks': self.detect_order_blocks(),
            'svp': self.detect_svp_fractals(),
        }
        
        # Resumen
        bos_bull = sum(1 for b in results['bos'] if 'BULLISH' in b['type'])
        bos_bear = sum(1 for b in results['bos'] if 'BEARISH' in b['type'])
        choch_bull = sum(1 for c in results['choch'] if 'BULLISH' in c['type'])
        choch_bear = sum(1 for c in results['choch'] if 'BEARISH' in c['type'])
        fvg_bull = sum(1 for f in results['fvg'] if 'BULLISH' in f['type'])
        fvg_bear = sum(1 for f in results['fvg'] if 'BEARISH' in f['type'])
        ob_bull = sum(1 for o in results['order_blocks'] if 'BULLISH' in o['type'])
        ob_bear = sum(1 for o in results['order_blocks'] if 'BEARISH' in o['type'])
        
        results['resumen'] = {
            'bos': f"{bos_bull} alcistas, {bos_bear} bajistas",
            'choch': f"{choch_bull} alcistas, {choch_bear} bajistas",
            'fvg': f"{fvg_bull} alcistas, {fvg_bear} bajistas",
            'order_blocks': f"{ob_bull} alcistas, {ob_bear} bajistas",
            'svp_poc': results['svp']['POC']
        }
        
        print(f"\n✅ ESCANEO COMPLETO:")
        for k, v in results['resumen'].items():
            print(f"   {k}: {v}")
        
        return results


# ══════════════════════════════════════════════════════════
# PRUEBA RAPIDA
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    scanner = ScannerSensei()
    
    # Probar con XAUUSD
    ruta = r"C:\Users\ezequiel\OneDrive\Desktop\DATA CSV\XAUUSD_M5_5Y.csv"
    resultados = scanner.run_full_scan(ruta)
    
    print("\n📊 ULTIMOS 5 CHoCH:")
    for c in resultados['choch'][-5:]:
        print(f"   {c['type']} @ {c['time']} | Precio: {c['price']}")
    
    print("\n📊 ULTIMOS 5 FVG:")
    for f in resultados['fvg'][-5:]:
        print(f"   {f['type']} @ {f['time']} | Gap: {f['gap_low']:.2f} - {f['gap_high']:.2f}")
    
    print("\n📊 ULTIMOS 5 BOS:")
    for b in resultados['bos'][-5:]:
        print(f"   {b['type']} @ {b['time']} | Precio: {b['price']}")
