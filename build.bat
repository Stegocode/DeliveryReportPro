@echo off
REM ─────────────────────────────────────────────────────────────────
REM DeliveryReportPro Build Script
REM Run this from the DeliveryReportPro\ directory on your dev machine.
REM Produces: installer_output\DeliveryReportPro_Setup.exe
REM ─────────────────────────────────────────────────────────────────

setlocal
set PYTHON=python
set INNO="%LOCALAPPDATA%\Programs\Inno Setup 6\ISCC.exe"

echo.
echo  ══════════════════════════════════════════════
echo    DeliveryReportPro Build
echo  ══════════════════════════════════════════════
echo.

REM ── Step 1: Install / update dependencies ─────────────────────
echo  [1/3] Installing dependencies...
%PYTHON% -m pip install -r requirements.txt --quiet
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo  *** pip install failed ***
    pause & exit /b 1
)
echo       Done.
echo.

REM ── Step 2: PyInstaller ───────────────────────────────────────
echo  [2/3] Building executable with PyInstaller...
%PYTHON% -m PyInstaller DeliveryReportPro.spec --clean --noconfirm
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo  *** PyInstaller failed — check output above ***
    pause & exit /b 1
)
echo       Done.
echo.

REM ── Step 3: Inno Setup installer ─────────────────────────────
echo  [3/3] Building installer with Inno Setup...
if not exist %INNO% (
    echo.
    echo  *** Inno Setup not found at %INNO% ***
    echo  *** Download from: https://jrsoftware.org/isinfo.php ***
    echo  *** Skipping installer — distributable is in dist\DeliveryReportPro\ ***
    echo.
    pause & exit /b 0
)

mkdir installer_output 2>nul
%INNO% inno_setup.iss
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo  *** Inno Setup failed — check output above ***
    pause & exit /b 1
)

echo.
echo  ══════════════════════════════════════════════
echo    Build complete!
echo.
echo    Installer: installer_output\DeliveryReportPro_Setup.exe
echo    Raw dist:  dist\DeliveryReportPro\DeliveryReportPro.exe
echo  ══════════════════════════════════════════════
echo.
pause
