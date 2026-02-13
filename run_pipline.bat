@echo off
REM Wechsle in das Verzeichnis, in dem die .bat-Datei liegt (FinDataWarehouse)
cd /D "%~dp0"
ECHO "Batch-Datei gestartet im Verzeichnis: %CD%" >> startup_test.log 2>&1

REM 
REM --- HIER IST DER FIX ---
REM 

REM 1. Setze den PYTHONPATH, damit Python deine 'utils', 'extract' etc. findet.
set PYTHONPATH=%CD%\src

REM 2. Rufe main.py auf, das sich im 'src'-Ordner befindet.
".\.venv\Scripts\python.exe" src\main.py >> pipeline_output.log 2>&1

REM ------------------------------
REM 

ECHO "Python-Skript beendet." >> startup_test.log 2>&1