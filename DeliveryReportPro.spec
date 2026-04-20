# -*- mode: python ; coding: utf-8 -*-
"""
DeliveryReportPro.spec
======================
PyInstaller build spec for DeliveryReportPro.
Produces a single-folder distribution (not one-file, for faster startup).

Build command — run from the DeliveryReportPro\\ directory:
  python -m PyInstaller DeliveryReportPro.spec --clean

Output: dist\\DeliveryReportPro\\DeliveryReportPro.exe
"""

import sys
from pathlib import Path

# SPECPATH is set by PyInstaller to the directory containing this .spec file,
# so the build works no matter where the repo is checked out.
PROJECT_DIR = SPECPATH

block_cipher = None

a = Analysis(
    [str(Path(PROJECT_DIR) / 'app.py')],
    pathex=[PROJECT_DIR],
    binaries=[],
    datas=[
        (str(Path(PROJECT_DIR) / 'templates'), 'templates'),
        (str(Path(PROJECT_DIR) / 'static'),    'static'),
    ],
    hiddenimports=[
        # Flask + SocketIO — correct package names
        'flask',
        'flask_socketio',
        'python_socketio',
        'python_engineio',
        'engineio',
        'engineio.async_drivers',
        'engineio.async_drivers.threading',
        'socketio',
        'socketio.async_drivers',
        'jinja2',
        'jinja2.ext',
        'itsdangerous',
        'werkzeug',
        'werkzeug.routing',
        'werkzeug.serving',
        # Data processing
        'pandas',
        'openpyxl',
        'openpyxl.styles',
        'openpyxl.utils',
        'pdfplumber',
        # Selenium
        'selenium',
        'selenium.webdriver',
        'selenium.webdriver.chrome.options',
        'selenium.webdriver.chrome.service',
        'selenium.webdriver.common.by',
        'selenium.webdriver.support.ui',
        'selenium.webdriver.support.expected_conditions',
        # Crypto
        'cryptography',
        'cryptography.fernet',
        'cryptography.hazmat.primitives',
        'cryptography.hazmat.primitives.hashes',
        'cryptography.hazmat.primitives.kdf.pbkdf2',
        # Requests
        'requests',
        'python_dotenv',
        # Our app modules — use _app suffix versions
        'config',
        'pipeline',
        'financial_scraper_app',
        'financial_generator_app',
        'chromedriver_manager',
        # winreg for ChromeDriver detection
        'winreg',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['tkinter', 'matplotlib', 'scipy', 'numpy.testing'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='DeliveryReportPro',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
    version=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='DeliveryReportPro',
)
