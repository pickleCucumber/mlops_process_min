@echo off
setlocal

REM === ПАРАМЕТРЫ ПЕРЕДАЧИ ===
REM %1 = путь к скриптам 
REM %2 = ID лога 
set "SCRIPT_PATH=%~1"
set "LOG_ID=%~2"

REM === КОНФИГУРАЦИЯ ===
set "WORKING_DIR=C:\Users\Test_app"
set "RESTART_DELAY=120"
set "PYTHON_EXE=python"

echo ===============================================
echo Monitoring started
echo File: %SCRIPT_PATH%
echo ID log: %LOG_ID%
echo ===============================================

:loop
cd /d "%WORKING_DIR%"

echo.
echo =========================================================================
echo [%DATE% %TIME%] Start new process [%LOG_ID%]...
echo =========================================================================

REM === ЗАПУСК СКРИПТА ===
REM передаем %LOG_ID% 

"%PYTHON_EXE%" "%SCRIPT_PATH%" "%LOG_ID%"

if %errorlevel% equ 0 (
    echo.
    echo [%DATE% %TIME%] Script [%LOG_ID%] shutdown **success** (Code 0).
	goto :end_monitor
) else (
    echo.
    echo [%DATE% %TIME%] Script [%LOG_ID%] shutdown **error** (Code %errorlevel%).
	echo -------------------------------------------------------------------------
    
    echo.
    echo [%DATE% %TIME%] Restart [%LOG_ID%] in %RESTART_DELAY% sec...
    echo -------------------------------------------------------------------------
    timeout /t %RESTART_DELAY% >nul
    goto loop  
)

:end_monitor
echo.
echo Monitoring [%LOG_ID%] stop.
endlocal
