"""
Prueba de conexión: genera un latest_signal.json de prueba y verifica que
server.py lo está sirviendo correctamente con CORS.

IMPORTANTE: corré `python server.py` en otra ventana ANTES de ejecutar esto.

Uso: python test_connection.py
"""
import sys
import json
import urllib.request
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

from src.order_flow_signal import save_latest_signal
from test_formato import SIGNAL_EJEMPLO

URL = "http://localhost:8000/latest_signal.json"


def main():
    print("1. Generando latest_signal.json de prueba...")
    save_latest_signal(SIGNAL_EJEMPLO)
    print("   OK\n")

    print(f"2. Pidiendo {URL} (simulando el origen de AI Studio)...")
    try:
        req = urllib.request.Request(URL, headers={"Origin": "https://aistudio.google.com"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read().decode("utf-8")
            headers = dict(resp.headers)
            status = resp.status
    except urllib.error.URLError as e:
        print(f"   ❌ FALLO: no se pudo conectar - {e}")
        print("   Verificá que 'python server.py' esté corriendo en otra ventana.\n")
        return

    print(f"   Status: {status}")
    cors = headers.get("Access-Control-Allow-Origin")
    print(f"   Access-Control-Allow-Origin: {cors or 'AUSENTE'}")
    if cors != "*":
        print("   ⚠️ Falta el header CORS - una app externa no va a poder leer esto.\n")
        return

    try:
        signal = json.loads(body)
    except json.JSONDecodeError as e:
        print(f"   ❌ FALLO: la respuesta no es JSON válido: {e}\n")
        return

    print(f"   ✅ JSON válido. decision={signal.get('decision')}, timestamp={signal.get('timestamp')}\n")
    print("=" * 60)
    print("✅ El servidor sirve latest_signal.json correctamente con CORS.")
    print("   Si el JS de tu app de AI Studio corre en TU navegador (no en un")
    print("   servidor de Google), el fetch hacia http://localhost:8000 debería")
    print("   funcionar igual que en esta prueba.")
    print("=" * 60)


if __name__ == "__main__":
    main()
