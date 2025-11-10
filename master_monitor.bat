@echo off
setlocal

REM === КОНФИГУРАЦИЯ ГЛАВНОГО МОНИТОРИНГА ===

set PYTHON_EXE=python

REM рабочая директория (CWD)
set WORKING_DIR=C:\Users\Test_app

REM Задержка перед запуском следующего скрипта
set START_DELAY=5


REM === ЗАПУСК МОНИТОРИНГА ДЛЯ КАЖДОГО СКРИПТА ===


REM Запуск Antifraud 
start "Monitoring: model Antifraud" cmd /c call slave_monitor.bat D:\GITREPO\console_test\console_antifraud\explore.py "antifraud"
timeout /t %START_DELAY% >nul

REM Запуск ALL
start "Monitoring: model ALL" cmd /c call slave_monitor.bat D:\GITREPO\console_test\console_all\explore.py "ALL"
timeout /t %START_DELAY% >nul

REM Запуск Крым
start "Monitoring: model Crimea" cmd /c call slave_monitor.bat D:\GITREPO\console_test\console_crimea\explore.py "crimea"
timeout /t %START_DELAY% >nul

REM Запуск повторников
start "Monitoring: model Repeated" cmd /c call slave_monitor.bat D:\GITREPO\console_test\console\explore.py "repeated"
timeout /t %START_DELAY% >nul

REM Запуск нерезов
start "Monitoring: model Nerez" cmd /c call slave_monitor.bat D:\GITREPO\console_test\console_nerez\explore.py "nerez"
timeout /t %START_DELAY% >nul


endlocal
