"""
CEREBRO GEMINI - Motor de análisis de mercado con Smart Money Concepts (SMC)
Usa el SDK nuevo `google-genai` (google.generativeai quedó deprecated).
"""
import os
import sys
import asyncio
import logging
import argparse
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

import numpy as np
import pandas as pd

from google import genai
from google.genai import types, errors

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# En consolas Windows con code page heredado (cp1252/cp850), imprimir emojis
# puede lanzar UnicodeEncodeError y tirar el proceso. Forzamos UTF-8.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

from config import settings

try:
    from src.notifier import enviar_telegram
except ImportError:
    async def enviar_telegram(mensaje: str) -> bool:
        print("\n" + "=" * 60)
        print("MENSAJE (Telegram no disponible):")
        print("=" * 60)
        print(mensaje)
        print("=" * 60 + "\n")
        return True

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TIMESTAMP_FORMAT = "%Y.%m.%d %H:%M"
TELEGRAM_MAX_LEN = 4000


class ModoAnalisis(Enum):
    MONITOREO = "monitoreo"
    MENTORIA = "mentoria"
    ANALISIS_PROFUNDO = "analisis_profundo"


VELAS_POR_MODO = {
    ModoAnalisis.MONITOREO.value: 200,
    ModoAnalisis.MENTORIA.value: 200,
    ModoAnalisis.ANALISIS_PROFUNDO.value: 8000,  # suficiente para reconstruir H4/D1 por resample
}


@dataclass
class AnalisisMercado:
    timestamp: str
    simbolo: str
    modo: str
    resumen: str
    estructura: str
    zonas_ob: List[str] = field(default_factory=list)
    fvg: List[str] = field(default_factory=list)
    liquidity: List[str] = field(default_factory=list)
    sentimiento: str = ""
    acciones: List[str] = field(default_factory=list)
    riesgo: str = ""


class CerebroGemini:
    def __init__(self, modo: str = ModoAnalisis.MONITOREO.value):
        self.modo = modo
        self.api_key = getattr(settings, "GEMINI_API_KEY", "") or ""

        if not self.api_key or "PON_TU" in self.api_key:
            logger.error("❌ GEMINI_API_KEY no configurada en config/settings.py")
        elif not (self.api_key.startswith("AIza") or self.api_key.startswith("AQ.")):
            logger.warning(
                "⚠️ La API Key no tiene ninguno de los prefijos conocidos de Google AI "
                "Studio ('AIzaSy...' o 'AQ...'). Generá una en https://aistudio.google.com/apikey. "
                "Si las llamadas fallan con error de autenticación, esa suele ser la causa."
            )

        self._client: Optional[genai.Client] = None
        if self.api_key:
            try:
                self._client = genai.Client(api_key=self.api_key)
                logger.info("✅ Cliente google-genai inicializado")
            except Exception as e:
                logger.error(f"❌ No se pudo inicializar el cliente de Gemini: {e}")

        modelos_candidatos = [getattr(settings, "GEMINI_MODEL", None), "gemini-2.5-flash", "gemini-2.0-flash"]
        self.modelos = list(dict.fromkeys(m for m in modelos_candidatos if m))

        self.data_dir = Path("C:/Users/ezequiel/OneDrive/Desktop/DATA CSV")
        self.symbols = ["XAUUSD_M5_5Y.csv", "BTCUSD_M5_5Y.csv"]

        logger.info(f"✅ CerebroGemini inicializado en modo: {modo}")

    # ------------------------------------------------------------------
    # Carga de datos
    # ------------------------------------------------------------------

    def _resolver_archivo(self, nombre_archivo: str) -> Optional[Path]:
        """Encuentra el CSV aunque esté en una subcarpeta (p.ej. BTCUSD/BTCUSD_M5_5Y.csv)."""
        candidato = self.data_dir / nombre_archivo
        if candidato.exists():
            return candidato

        coincidencias = list(self.data_dir.rglob(nombre_archivo))
        if coincidencias:
            return coincidencias[0]

        prefijo = nombre_archivo.split('_')[0]
        coincidencias = list(self.data_dir.rglob(f"{prefijo}_M5_5Y.csv"))
        if coincidencias:
            return coincidencias[0]

        coincidencias = list(self.data_dir.rglob(f"{prefijo}*.csv"))
        return coincidencias[0] if coincidencias else None

    @staticmethod
    def _contar_lineas(path: Path) -> int:
        with open(path, 'rb') as f:
            return sum(chunk.count(b'\n') for chunk in iter(lambda: f.read(1024 * 1024), b''))

    def cargar_datos(self, simbolo: str, velas: int = 200) -> Optional[pd.DataFrame]:
        """Lee solo la cola del CSV necesaria (evita cargar 400k filas para pedir 200 velas)."""
        try:
            archivo = self._resolver_archivo(simbolo)
            if not archivo:
                logger.error(f"❌ Archivo no encontrado para {simbolo} en {self.data_dir}")
                return None

            total_lineas = self._contar_lineas(archivo)
            total_datos = total_lineas - 1  # sin header
            if total_datos <= 0:
                logger.error(f"❌ Archivo vacío: {archivo}")
                return None

            margen = min(total_datos, velas + 100)
            filas_a_saltar = max(0, total_datos - margen)
            skiprows = range(1, filas_a_saltar + 1) if filas_a_saltar > 0 else None

            df = pd.read_csv(
                archivo,
                skiprows=skiprows,
                dtype={
                    'Open': 'float64', 'High': 'float64', 'Low': 'float64',
                    'Close': 'float64', 'Volume': 'float64',
                },
            )

            required_cols = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in df.columns for col in required_cols):
                logger.error(f"❌ Columnas faltantes en {archivo.name}: tiene {list(df.columns)}")
                return None

            df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=TIMESTAMP_FORMAT, errors='coerce')
            filas_antes = len(df)
            df = df.dropna(subset=['Timestamp']).sort_values('Timestamp').reset_index(drop=True)
            if len(df) < filas_antes:
                logger.warning(f"⚠️ {filas_antes - len(df)} filas con timestamp inválido descartadas en {archivo.name}")

            df = df.tail(velas).reset_index(drop=True)
            logger.info(f"✅ Cargadas {len(df)} velas de {archivo.name} (de {total_datos} totales en el archivo)")
            return df

        except Exception as e:
            logger.error(f"❌ Error cargando {simbolo}: {e}", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Detección de estructura SMC
    # ------------------------------------------------------------------

    @staticmethod
    def _pivots(high: np.ndarray, low: np.ndarray, izq: int = 2, der: int = 2):
        n = len(high)
        ph = np.zeros(n, dtype=bool)
        pl = np.zeros(n, dtype=bool)
        for i in range(izq, n - der):
            if high[i] == high[i - izq:i + der + 1].max():
                ph[i] = True
            if low[i] == low[i - izq:i + der + 1].min():
                pl[i] = True
        return ph, pl

    @staticmethod
    def _bloque(titulo: str, items: List[str], maximo: int = 3) -> str:
        lineas = [f"{titulo}: {len(items)}"]
        if items:
            lineas.extend(f"- {i}" for i in items[-maximo:])
        else:
            lineas.append("- Ninguno detectado")
        return "\n".join(lineas)

    def _identificar_estructura_smc(self, df: pd.DataFrame) -> str:
        if len(df) < 10:
            return "Datos insuficientes para análisis SMC"

        high = df['High'].to_numpy()
        low = df['Low'].to_numpy()
        close = df['Close'].to_numpy()
        open_ = df['Open'].to_numpy()
        ts = df['Timestamp'].tolist()
        n = len(df)

        ph, pl = self._pivots(high, low)

        # --- BOS / CHoCH: ruptura de swing en la dirección de tendencia vs en contra ---
        eventos_bos, eventos_choch = [], []
        tendencia = None
        ultimo_swing_high = None
        ultimo_swing_low = None
        for i in range(n):
            if ph[i]:
                ultimo_swing_high = high[i]
            if pl[i]:
                ultimo_swing_low = low[i]

            if ultimo_swing_high is not None and close[i] > ultimo_swing_high:
                etiqueta = f"en {ts[i].strftime('%Y-%m-%d %H:%M')} (rompe {ultimo_swing_high:.4f})"
                if tendencia == "bajista":
                    eventos_choch.append(f"CHoCH alcista {etiqueta}")
                else:
                    eventos_bos.append(f"BOS alcista {etiqueta}")
                tendencia = "alcista"
                ultimo_swing_high = None

            if ultimo_swing_low is not None and close[i] < ultimo_swing_low:
                etiqueta = f"en {ts[i].strftime('%Y-%m-%d %H:%M')} (rompe {ultimo_swing_low:.4f})"
                if tendencia == "alcista":
                    eventos_choch.append(f"CHoCH bajista {etiqueta}")
                else:
                    eventos_bos.append(f"BOS bajista {etiqueta}")
                tendencia = "bajista"
                ultimo_swing_low = None

        # --- FVG: gap de 3 velas estilo ICT ---
        fvgs = []
        for i in range(2, n):
            if low[i] > high[i - 2]:
                fvgs.append(f"FVG alcista {high[i-2]:.4f}-{low[i]:.4f} en {ts[i-1].strftime('%Y-%m-%d %H:%M')}")
            elif high[i] < low[i - 2]:
                fvgs.append(f"FVG bajista {high[i]:.4f}-{low[i-2]:.4f} en {ts[i-1].strftime('%Y-%m-%d %H:%M')}")

        # --- Order Blocks: última vela opuesta antes de un impulso ---
        rango = high - low
        rango_medio = rango[rango > 0].mean() if np.any(rango > 0) else 0.0
        obs = []
        for i in range(1, n):
            if rango_medio <= 0 or rango[i] <= rango_medio * 1.5:
                continue
            j = i - 1
            if close[i] > open_[i] and close[j] < open_[j]:
                obs.append(f"OB alcista en {ts[j].strftime('%Y-%m-%d %H:%M')} zona {low[j]:.4f}-{high[j]:.4f}")
            elif close[i] < open_[i] and close[j] > open_[j]:
                obs.append(f"OB bajista en {ts[j].strftime('%Y-%m-%d %H:%M')} zona {low[j]:.4f}-{high[j]:.4f}")

        # --- Zonas de Oferta/Demanda: base de consolidación antes de un impulso ---
        zonas_sd = []
        ventana = 3
        for i in range(ventana, n):
            if rango_medio <= 0 or rango[i] <= rango_medio * 1.5:
                continue
            base_high = high[i - ventana:i].max()
            base_low = low[i - ventana:i].min()
            if close[i] > open_[i] and close[i] > base_high:
                zonas_sd.append(f"Zona de Demanda ~{ts[i-1].strftime('%Y-%m-%d %H:%M')} ({base_low:.4f}-{base_high:.4f})")
            elif close[i] < open_[i] and close[i] < base_low:
                zonas_sd.append(f"Zona de Oferta ~{ts[i-1].strftime('%Y-%m-%d %H:%M')} ({base_low:.4f}-{base_high:.4f})")

        # --- Liquidez: equal highs/lows + extremos del rango ---
        tol = close.mean() * 0.0005 if n else 0.0
        swing_highs = sorted(set(round(h, 4) for i, h in enumerate(high) if ph[i]))
        swing_lows = sorted(set(round(l, 4) for i, l in enumerate(low) if pl[i]))

        def agrupar(vals):
            grupos = []
            for v in vals:
                if grupos and abs(v - grupos[-1][-1]) <= tol:
                    grupos[-1].append(v)
                else:
                    grupos.append([v])
            return [g for g in grupos if len(g) > 1]

        liquidez = []
        for g in agrupar(swing_highs):
            liquidez.append(f"Liquidez compradora (equal highs) ~{sum(g)/len(g):.4f}")
        for g in agrupar(swing_lows):
            liquidez.append(f"Liquidez vendedora (equal lows) ~{sum(g)/len(g):.4f}")
        liquidez.append(f"Extremo superior del rango: {high.max():.4f}")
        liquidez.append(f"Extremo inferior del rango: {low.min():.4f}")

        secciones = [
            self._bloque("BOS detectados", eventos_bos),
            self._bloque("CHoCH detectados", eventos_choch),
            self._bloque("FVGs detectados", fvgs),
            self._bloque("Order Blocks detectados", obs),
            self._bloque("Zonas Oferta/Demanda detectadas", zonas_sd),
            self._bloque("Liquidez", liquidez, maximo=len(liquidez) or 1),
        ]
        return "\n\n".join(secciones)

    def _resumen_multi_timeframe(self, df_m5: pd.DataFrame) -> str:
        """Reconstruye M15/H1/H4/D1 por resample desde M5 y resume estructura de cada uno."""
        base = df_m5.set_index('Timestamp')
        timeframes = [("M15", "15min"), ("H1", "1h"), ("H4", "4h"), ("D1", "1D")]
        bloques = []
        for nombre, regla in timeframes:
            try:
                velas_tf = base.resample(regla).agg({
                    'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum',
                }).dropna()
            except Exception as e:
                logger.warning(f"⚠️ No se pudo resamplear a {nombre}: {e}")
                continue

            if len(velas_tf) < 5:
                continue

            velas_tf = velas_tf.reset_index()
            estructura = self._identificar_estructura_smc(velas_tf.tail(150).reset_index(drop=True))
            bloques.append(
                f"### Temporalidad {nombre} ({len(velas_tf)} velas reconstruidas)\n"
                f"Último cierre: {velas_tf.iloc[-1]['Close']:.4f} | "
                f"Máx periodo: {velas_tf['High'].max():.4f} | Mín periodo: {velas_tf['Low'].min():.4f}\n"
                f"{estructura}"
            )
        return "\n\n".join(bloques) if bloques else "Datos insuficientes para análisis multi-temporalidad"

    def preparar_datos_para_gemini(self, df: pd.DataFrame, incluir_multi_tf: bool = False) -> str:
        if df is None or len(df) == 0:
            return "Datos insuficientes"

        latest = df.iloc[-1]
        previous = df.iloc[-2] if len(df) > 1 else latest
        precio_actual = latest['Close']
        precio_anterior = previous['Close']
        cambio = ((precio_actual - precio_anterior) / precio_anterior) * 100 if precio_anterior else 0.0

        max_precio = df['High'].max()
        min_precio = df['Low'].min()
        volumen_promedio = df['Volume'].mean()
        volumen_actual = latest['Volume']

        if len(df) >= 20:
            sma_20 = df['Close'].tail(20).mean()
            sma_50 = df['Close'].tail(50).mean() if len(df) >= 50 else sma_20
            tendencia = "alcista" if sma_20 > sma_50 else "bajista"
        else:
            sma_20 = sma_50 = precio_actual
            tendencia = "neutral"

        ultimas_velas = "\n".join(
            f"{row.Timestamp.strftime('%Y-%m-%d %H:%M')} | O:{row.Open:.4f} H:{row.High:.4f} "
            f"L:{row.Low:.4f} C:{row.Close:.4f} V:{row.Volume:.0f}"
            for row in df.tail(5).itertuples()
        )

        estructura = self._identificar_estructura_smc(df)

        texto = f"""DATOS DE MERCADO (M5):
Rango de datos: {df.iloc[0]['Timestamp'].strftime('%Y-%m-%d %H:%M')} a {latest['Timestamp'].strftime('%Y-%m-%d %H:%M')}
Velas analizadas: {len(df)}
Precio Actual: {precio_actual:.4f}
Precio Anterior: {precio_anterior:.4f}
Cambio: {cambio:.2f}%
Máximo del periodo: {max_precio:.4f}
Mínimo del periodo: {min_precio:.4f}

VOLUMEN:
Volumen Actual: {volumen_actual:.0f}
Volumen Promedio: {volumen_promedio:.0f}
Ratio Volumen: {(volumen_actual / volumen_promedio):.2f}x

TENDENCIA:
Tendencia (SMA20 vs SMA50): {tendencia}
SMA 20: {sma_20:.4f}
SMA 50: {sma_50:.4f}

ESTRUCTURA SMC (M5):
{estructura}

ÚLTIMAS 5 VELAS:
{ultimas_velas}
"""
        if incluir_multi_tf:
            texto += f"\nANÁLISIS MULTI-TEMPORALIDAD:\n{self._resumen_multi_timeframe(df)}\n"

        return texto

    # ------------------------------------------------------------------
    # Llamada a Gemini (google-genai)
    # ------------------------------------------------------------------

    async def llamar_gemini(
        self, prompt: str, system_prompt: str = "", temperature: float = 0.3, max_tokens: int = 800,
    ) -> Optional[str]:
        if not self._client:
            logger.error("❌ Cliente de Gemini no inicializado (revisa GEMINI_API_KEY en config/settings.py)")
            return None

        config = types.GenerateContentConfig(
            temperature=temperature,
            max_output_tokens=max_tokens,
            top_p=0.95,
            top_k=40,
            system_instruction=system_prompt or None,
        )

        ultimo_error = None
        for modelo in self.modelos:
            for intento in range(1, 4):
                try:
                    respuesta = await asyncio.wait_for(
                        self._client.aio.models.generate_content(
                            model=modelo,
                            contents=prompt,
                            config=config,
                        ),
                        timeout=45,
                    )
                    texto = getattr(respuesta, "text", None)
                    if texto:
                        return texto
                    logger.error(f"❌ Respuesta sin texto de {modelo}: {respuesta}")
                    return None

                except asyncio.TimeoutError:
                    ultimo_error = "timeout"
                    logger.warning(f"⏰ Timeout llamando a {modelo} (intento {intento}/3)")
                    await asyncio.sleep(2 * intento)

                except errors.ClientError as e:
                    ultimo_error = e
                    if e.code == 404:
                        logger.warning(f"📡 Modelo {modelo} no disponible, probando el siguiente...")
                        break
                    elif e.code == 429:
                        logger.warning(f"⏰ Cuota excedida en {modelo} (intento {intento}/3)")
                        await asyncio.sleep(2 ** intento)
                        continue
                    elif e.code == 403:
                        logger.error(f"🔑 API Key sin permisos para {modelo}: {e.message}")
                        return None
                    else:
                        logger.error(f"❌ Error de cliente Gemini ({e.code}): {e.message}")
                        return None

                except errors.ServerError as e:
                    ultimo_error = e
                    logger.warning(f"❌ Error de servidor Gemini ({e.code}), reintentando... (intento {intento}/3)")
                    await asyncio.sleep(2 ** intento)

                except Exception as e:
                    ultimo_error = e
                    logger.error(f"❌ Error inesperado llamando a Gemini: {e}")
                    return None

        logger.error(f"❌ No se pudo obtener respuesta de Gemini tras probar todos los modelos: {ultimo_error}")
        return None

    # ------------------------------------------------------------------
    # Prompts por modo
    # ------------------------------------------------------------------

    def _construir_prompt_monitoreo(self, datos: str) -> str:
        return f"""Eres un analista de Smart Money Concepts (SMC) experto en trading.

Analiza estos datos de mercado y da un resumen ejecutivo:

{datos}

Responde EXACTAMENTE con estas secciones:

1. RESUMEN EJECUTIVO (1-2 frases)
2. ESTRUCTURA SMC (menciona BOS, CHoCH, OB, FVG y Liquidez detectados)
3. SENTIMIENTO ACTUAL (Alcista/Bajista/Neutral y por qué)
4. ACCIONES RECOMENDADAS (qué hacer ahora)
5. NIVEL DE RIESGO (Alto/Medio/Bajo)

Sé breve, directo y accionable.
"""

    def _construir_prompt_mentoria(self, datos: str) -> str:
        return f"""Eres un mentor de trading explicando Smart Money Concepts.

Analiza estos datos y explica educativamente:

{datos}

Responde EXACTAMENTE con estas secciones:

1. ANALISIS DIDACTICO (explica qué está pasando)
2. PATRONES SMC (identifica y explica BOS, CHoCH, OB, FVG y Liquidez)
3. CONTEXTO DE MERCADO (dónde estamos en el ciclo)
4. OPORTUNIDADES (qué buscar)
5. CONSEJOS PRACTICOS

Sé educativo y detallado, como si le enseñaras a alguien que recién aprende SMC.
"""

    def _construir_prompt_profundo(self, datos: str) -> str:
        return f"""Eres un analista cuantitativo experto en SMC con enfoque multi-temporalidad.

Realiza un análisis exhaustivo de estos datos, cruzando la información entre temporalidades:

{datos}

Responde EXACTAMENTE con estas secciones:

1. ESTRUCTURA DE MERCADO (BOS, CHoCH, OB, FVG por temporalidad)
2. ANALISIS DE VOLUMEN (qué dice sobre la intención institucional)
3. CONFLUENCIAS ENTRE TEMPORALIDADES (dónde coinciden M5/M15/H1/H4/D1)
4. ZONAS CLAVE (niveles de oferta/demanda y liquidez más relevantes)
5. PROYECCION (posibles escenarios alcista/bajista)
6. RECOMENDACIONES ESTRATEGICAS (con niveles concretos)

Sé exhaustivo y específico con números.
"""

    # ------------------------------------------------------------------
    # Parseo de la respuesta y envío
    # ------------------------------------------------------------------

    def _parsear_respuesta_gemini(self, texto: str, simbolo: str) -> AnalisisMercado:
        texto_limpio = texto.replace('**', '').replace('*', '').replace('#', '').strip()
        lineas = texto_limpio.split('\n')

        resumen = estructura = sentimiento = riesgo = ""
        zonas_ob, fvg, liquidity, acciones = [], [], [], []
        seccion_actual = None

        for linea in lineas:
            linea = linea.strip()
            if not linea:
                continue

            linea_upper = linea.upper()
            if "RESUMEN" in linea_upper:
                seccion_actual = "resumen"
                continue
            elif "ESTRUCTURA" in linea_upper:
                seccion_actual = "estructura"
                continue
            elif "SENTIMIENTO" in linea_upper:
                seccion_actual = "sentimiento"
                continue
            elif "ACCIONES" in linea_upper or "RECOMENDACIONES" in linea_upper:
                seccion_actual = "acciones"
                continue
            elif "RIESGO" in linea_upper:
                seccion_actual = "riesgo"
                continue
            elif "OB" in linea_upper and len(linea) < 30:
                seccion_actual = "ob"
                continue
            elif "FVG" in linea_upper and len(linea) < 30:
                seccion_actual = "fvg"
                continue
            elif "LIQUIDEZ" in linea_upper or "LIQUIDITY" in linea_upper:
                seccion_actual = "liquidity"
                continue

            if linea.startswith(('1.', '2.', '3.', '4.', '5.', '6.', '-')):
                linea = linea[2:].strip()

            if seccion_actual == "resumen":
                resumen += linea + " "
            elif seccion_actual == "estructura":
                estructura += linea + " "
            elif seccion_actual == "sentimiento":
                sentimiento += linea + " "
            elif seccion_actual == "acciones":
                if linea and not linea.startswith('•'):
                    acciones.append(linea)
            elif seccion_actual == "riesgo":
                riesgo += linea + " "
            elif seccion_actual == "ob":
                if linea and len(linea) > 5:
                    zonas_ob.append(linea)
            elif seccion_actual == "fvg":
                if linea and len(linea) > 5:
                    fvg.append(linea)
            elif seccion_actual == "liquidity":
                if linea and len(linea) > 5:
                    liquidity.append(linea)

        if not resumen:
            resumen = texto_limpio[:200]
            estructura = "Análisis detallado disponible"
            sentimiento = "Neutral"
            acciones = ["Revisar análisis completo"]
            riesgo = "Moderado"

        acciones = [a for a in acciones if a and len(a) > 5] or ["Mantener posición actual"]

        return AnalisisMercado(
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            simbolo=simbolo.split('_')[0],
            modo=self.modo,
            resumen=resumen.strip()[:400],
            estructura=estructura.strip()[:300],
            zonas_ob=zonas_ob[:5],
            fvg=fvg[:5],
            liquidity=liquidity[:5],
            sentimiento=sentimiento.strip()[:100],
            acciones=acciones[:5],
            riesgo=riesgo.strip()[:100],
        )

    @staticmethod
    def _dividir_mensaje(mensaje: str, limite: int = TELEGRAM_MAX_LEN) -> List[str]:
        if len(mensaje) <= limite:
            return [mensaje]
        partes, actual = [], ""
        for linea in mensaje.split("\n"):
            if len(actual) + len(linea) + 1 > limite:
                if actual:
                    partes.append(actual)
                actual = linea
            else:
                actual = f"{actual}\n{linea}" if actual else linea
        if actual:
            partes.append(actual)
        return partes

    async def enviar_analisis_telegram(self, analisis: AnalisisMercado):
        mensaje = f"""🧠 ANÁLISIS GEMINI (SMC)
━━━━━━━━━━━━━━━━━━━━━

📊 Símbolo: {analisis.simbolo}
🕐 Timestamp: {analisis.timestamp}
📌 Modo: {analisis.modo.upper()}

━━━━━━━━━━━━━━━━━━━━━

📝 RESUMEN EJECUTIVO:
{analisis.resumen}

🏗️ ESTRUCTURA SMC:
{analisis.estructura}

🎯 SENTIMIENTO:
{analisis.sentimiento}

⚡ ACCIONES RECOMENDADAS:
{chr(10).join(['• ' + a for a in analisis.acciones])}

⚠️ NIVEL DE RIESGO:
{analisis.riesgo}

━━━━━━━━━━━━━━━━━━━━━
🔍 DETALLES SMC:

📌 Order Blocks (OBs):
{chr(10).join(['• ' + ob for ob in analisis.zonas_ob]) if analisis.zonas_ob else '• Ninguno detectado'}

📌 Fair Value Gaps (FVGs):
{chr(10).join(['• ' + f for f in analisis.fvg]) if analisis.fvg else '• Ninguno detectado'}

📌 Zonas de Liquidez:
{chr(10).join(['• ' + l for l in analisis.liquidity]) if analisis.liquidity else '• Ninguna detectada'}
"""
        try:
            for parte in self._dividir_mensaje(mensaje):
                ok = await enviar_telegram(parte)
                if not ok:
                    logger.warning("⚠️ Telegram reportó fallo al enviar un fragmento del análisis")
                await asyncio.sleep(0.5)
            logger.info("✅ Análisis enviado por Telegram")
        except Exception as e:
            logger.error(f"❌ Error enviando por Telegram: {e}")
            print("\n" + "=" * 60)
            print("📨 MENSAJE (no se pudo enviar por Telegram):")
            print("=" * 60)
            print(mensaje)
            print("=" * 60 + "\n")

    # ------------------------------------------------------------------
    # Orquestación
    # ------------------------------------------------------------------

    async def analizar_mercado(self, simbolo: str = "XAUUSD_M5_5Y.csv", velas: Optional[int] = None) -> Optional[AnalisisMercado]:
        try:
            logger.info(f"🧠 Analizando {simbolo} en modo {self.modo}...")

            if velas is None:
                velas = VELAS_POR_MODO.get(self.modo, 200)

            df = self.cargar_datos(simbolo, velas)
            if df is None or df.empty:
                logger.error(f"❌ No hay datos disponibles para {simbolo}")
                return None

            incluir_multi_tf = self.modo == ModoAnalisis.ANALISIS_PROFUNDO.value
            prompt_data = self.preparar_datos_para_gemini(df, incluir_multi_tf=incluir_multi_tf)

            modo_cfg = getattr(settings, "GEMINI_MODES", {}).get(self.modo, {})
            temperature = modo_cfg.get("temperature", 0.3)
            max_tokens = modo_cfg.get("max_tokens", 800)
            system_prompt = modo_cfg.get("system_prompt", "")

            if self.modo == ModoAnalisis.MONITOREO.value:
                prompt = self._construir_prompt_monitoreo(prompt_data)
            elif self.modo == ModoAnalisis.MENTORIA.value:
                prompt = self._construir_prompt_mentoria(prompt_data)
            else:
                prompt = self._construir_prompt_profundo(prompt_data)

            texto_respuesta = await self.llamar_gemini(
                prompt, system_prompt=system_prompt, temperature=temperature, max_tokens=max_tokens,
            )

            if not texto_respuesta:
                logger.error(f"❌ Respuesta vacía de Gemini para {simbolo}")
                return None

            logger.info(f"✅ Análisis completado para {simbolo}")
            analisis = self._parsear_respuesta_gemini(texto_respuesta, simbolo)
            await self.enviar_analisis_telegram(analisis)
            return analisis

        except Exception as e:
            logger.error(f"❌ Error en análisis de {simbolo}: {e}", exc_info=True)
            return None

    async def analizar_multiple(self, simbolos: Optional[List[str]] = None) -> List[Optional[AnalisisMercado]]:
        if simbolos is None:
            simbolos = self.symbols

        resultados = []
        for simbolo in simbolos:
            resultados.append(await self.analizar_mercado(simbolo))
            await asyncio.sleep(2)
        return resultados

    async def ciclo_continuo(self, intervalo_minutos: int = 60):
        logger.info(f"🔄 Iniciando monitoreo continuo cada {intervalo_minutos} minutos")
        while True:
            try:
                logger.info(f"🚀 Nueva ronda de análisis - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                await self.analizar_multiple()
                logger.info(f"⏳ Ronda completada. Esperando {intervalo_minutos} minutos...")
                await asyncio.sleep(intervalo_minutos * 60)
            except Exception as e:
                logger.error(f"❌ Error en ciclo de monitoreo: {e}", exc_info=True)
                await asyncio.sleep(60)


def _parse_args():
    parser = argparse.ArgumentParser(description="Cerebro Gemini - Análisis SMC de mercado")
    parser.add_argument("--modo", choices=[m.value for m in ModoAnalisis], default=ModoAnalisis.MONITOREO.value)
    parser.add_argument("--simbolo", default="XAUUSD_M5_5Y.csv")
    parser.add_argument("--todos", action="store_true", help="Analiza todos los símbolos configurados")
    parser.add_argument("--continuo", action="store_true", help="Ejecuta en bucle cada --intervalo minutos")
    parser.add_argument("--intervalo", type=int, default=60, help="Minutos entre rondas en modo continuo")
    return parser.parse_args()


async def main():
    args = _parse_args()

    print("\n" + "=" * 60)
    print("🚀 CEREBRO GEMINI - SMART MONEY CONCEPTS")
    print(f"   Modo: {args.modo}")
    print("=" * 60 + "\n")

    cerebro = CerebroGemini(modo=args.modo)

    if not cerebro.data_dir.exists():
        logger.error(f"❌ Directorio de datos no encontrado: {cerebro.data_dir}")
        return

    if args.continuo:
        await cerebro.ciclo_continuo(intervalo_minutos=args.intervalo)
        return

    if args.todos:
        resultados = await cerebro.analizar_multiple()
        exitosos = sum(1 for r in resultados if r)
        print(f"\n✅ {exitosos}/{len(resultados)} análisis completados. Revisa Telegram.\n")
        return

    resultado = await cerebro.analizar_mercado(args.simbolo)
    if resultado:
        print(f"\n✅ ANÁLISIS COMPLETADO para {resultado.simbolo}")
        print(f"🎯 Sentimiento: {resultado.sentimiento}")
        print(f"⚠️ Riesgo: {resultado.riesgo}")
        print("📨 Revisa tu Telegram para el análisis completo!\n")
    else:
        print("\n❌ El análisis falló. Revisa los logs para más detalle.")
        print("🔧 Verifica: GEMINI_API_KEY en config/settings.py, y que sea una clave de AI Studio ('AIzaSy...' o 'AQ...').\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Programa detenido por el usuario")
