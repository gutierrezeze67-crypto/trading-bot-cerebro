"""Bootstrap: hace resolvible el paquete 'config' de la raiz de trading-platform
(config/constants.py, config/settings.py) desde adentro de unified_brain, sin
duplicar esos archivos aca. 'src' en si mismo NO se toca - siempre resuelve a
este paquete (unified_brain/src), nunca al src/ de la raiz, para evitar el
choque de nombres entre los dos arboles 'src' del repo."""
from __future__ import annotations

import sys
from pathlib import Path

_TRADING_PLATFORM_ROOT = Path(__file__).resolve().parents[2]
if str(_TRADING_PLATFORM_ROOT) not in sys.path:
    sys.path.append(str(_TRADING_PLATFORM_ROOT))
