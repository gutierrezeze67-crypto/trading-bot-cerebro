"""
Servidor HTTP local con CORS habilitado, para que una app externa que corra en tu
navegador (p.ej. una app de Google AI Studio) pueda leer latest_signal.json.

OJO: esto solo funciona si el fetch lo hace JS corriendo en TU navegador. Si tu app
ya está desplegada y el fetch corre en el servidor de Google (Cloud Run), esto no
puede funcionar - ese servidor no tiene forma de alcanzar tu PC. Además, Chrome
puede bloquear o pedir permiso (Private Network Access) cuando un sitio público
intenta acceder a localhost; ya se manda el header que ese chequeo pide, pero
puede seguir requiriendo confirmación del navegador según la versión.

Uso: python server.py [puerto]
"""
import sys
import http.server
import socketserver
from pathlib import Path

PUERTO = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
DIRECTORIO = Path(__file__).parent.resolve()


class CORSHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(DIRECTORIO), **kwargs)

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def log_message(self, format, *args):
        # Un log por request es ruido si el polling es cada pocos segundos.
        if "signal" in (args[0] if args else ""):
            return
        super().log_message(format, *args)


def main():
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass

    with socketserver.TCPServer(("", PUERTO), CORSHandler) as httpd:
        print(f"📡 Sirviendo {DIRECTORIO}")
        print(f"🔗 http://localhost:{PUERTO}/latest_signal.json")
        print("Ctrl+C para detener\n")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n🛑 Servidor detenido")


if __name__ == "__main__":
    main()
