@echo off
cd /d "%~dp0"

echo ============================================================
echo   ORDER FLOW MASTER - Iniciando cerebro + dashboard
echo ============================================================
echo.

echo [1/3] Iniciando el cerebro (genera senales cada 5 min)...
start "CEREBRO - Order Flow" cmd /k python -m src.order_flow_signal --loop --interval 5

echo [2/3] Iniciando el servidor del dashboard...
start "DASHBOARD - Servidor" cmd /k python -m http.server 8000

echo [3/3] Esperando 3 segundos y abriendo el dashboard en el navegador...
timeout /t 3 /nobreak > nul
start http://localhost:8000/dashboard.html

echo.
echo Listo. Quedaron 2 ventanas abiertas (cerebro y servidor) - no las cierres.
echo Esta ventana ya se puede cerrar.
pause
