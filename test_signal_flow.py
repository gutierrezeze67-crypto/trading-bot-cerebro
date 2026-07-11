"""
Prueba el flujo completo hacia Firebase: escribe una señal de ejemplo en la
colección 'live_signals' de Firestore y la vuelve a leer para confirmar.

Requiere que exista config/firebase-service-account.json (ver instrucciones
que te dio Claude). Sin ese archivo, el script avisa y no falla.

Uso: python test_signal_flow.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

from src.order_flow_signal import (
    save_signal_to_firestore,
    _get_firestore_client,
    FIREBASE_CRED_PATH,
    FIRESTORE_COLLECTION,
)
from test_formato import SIGNAL_EJEMPLO


def main():
    print("\n" + "=" * 60)
    print("PRUEBA DE FLUJO: cerebro local -> Firestore -> panel de AI Studio")
    print("=" * 60 + "\n")

    print(f"1. Buscando credencial en {FIREBASE_CRED_PATH}...")
    if not FIREBASE_CRED_PATH.exists():
        print("   ❌ No existe. Descargala desde Firebase Console > Configuración")
        print("      del proyecto > Cuentas de servicio > Generar nueva clave privada,")
        print(f"      y guardala exactamente en: {FIREBASE_CRED_PATH}\n")
        return
    print("   ✅ Encontrada\n")

    print("2. Escribiendo señal de ejemplo en Firestore...")
    ok = save_signal_to_firestore(SIGNAL_EJEMPLO)
    if not ok:
        print("   ❌ Falló. Revisá los logs de arriba (credencial inválida, permisos, etc.)\n")
        return
    print(f"   ✅ Escrita en la colección '{FIRESTORE_COLLECTION}'\n")

    print("3. Leyendo de vuelta para confirmar...")
    db = _get_firestore_client()
    docs = list(
        db.collection(FIRESTORE_COLLECTION)
        .order_by("created_at", direction="DESCENDING")
        .limit(1)
        .stream()
    )
    if not docs:
        print("   ❌ No se encontró ningún documento en la colección.\n")
        return

    doc = docs[0]
    data = doc.to_dict()
    print(f"   ✅ Documento {doc.id}: decision={data.get('decision')}, entry_price={data.get('entry_price')}\n")

    print("=" * 60)
    print("✅ El flujo funciona. Andá a tu app de AI Studio y fijate si el")
    print("   panel de confluencias ya muestra esta señal de prueba en vivo.")
    print(f"   (También la podés ver en Firebase Console > Firestore Database > {FIRESTORE_COLLECTION})")
    print("=" * 60)


if __name__ == "__main__":
    main()
