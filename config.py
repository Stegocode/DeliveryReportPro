# -*- coding: utf-8 -*-
"""
config.py
=========
AES-256 encrypted configuration storage for DeliveryReportPro.
Encryption key is derived from the Windows machine GUID — config file
is machine-bound and useless if copied to another system.

Storage: %APPDATA%\\DeliveryReportPro\\config.enc
"""

import os
import json
import base64
import hashlib
import platform
import subprocess
from pathlib import Path
from typing import Optional

try:
    from cryptography.fernet import Fernet, InvalidToken
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

CONFIG_DIR  = Path(os.environ.get("APPDATA", os.path.expanduser("~"))) / "DeliveryReportPro"
CONFIG_FILE = CONFIG_DIR / "config.enc"
SALT        = b"DeliveryReportPro_v1_salt_2026"  # Fixed salt — key entropy comes from machine GUID

REQUIRED_KEYS = [
    "HS_USERNAME",
    "HS_PASSWORD",
    "HS_BASE_URL",
    "GOOGLE_API_KEY",
    "MONDAY_API_TOKEN",
    "ORS_API_KEY",
]


# ── Machine fingerprint ────────────────────────────────────────────

def _get_machine_guid() -> str:
    """
    Returns a stable machine-specific string for key derivation.
    Uses Windows MachineGuid from registry; falls back to hostname.

    Raises:
        RuntimeError: if platform is not Windows and no fallback works
    """
    if platform.system() == "Windows":
        try:
            result = subprocess.check_output(
                r'reg query HKLM\SOFTWARE\Microsoft\Cryptography /v MachineGuid',
                shell=True, stderr=subprocess.DEVNULL
            ).decode()
            for line in result.splitlines():
                if "MachineGuid" in line:
                    return line.split()[-1].strip()
        except Exception:
            pass
    # Fallback: hostname (less unique but functional for dev/non-Windows)
    return platform.node() or "DeliveryReportPro_fallback_key"


def _derive_key(machine_guid: str) -> bytes:
    """
    Derives a 32-byte Fernet-compatible key from the machine GUID using PBKDF2.

    Args:
        machine_guid: stable machine-specific string

    Returns:
        URL-safe base64-encoded 32-byte key
    """
    if not HAS_CRYPTO:
        raise RuntimeError(
            "CONFIG_ERROR: 'cryptography' package not installed. "
            "Run: pip install cryptography"
        )
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=SALT,
        iterations=480_000,
    )
    raw = kdf.derive(machine_guid.encode())
    return base64.urlsafe_b64encode(raw)


# ── Public API ─────────────────────────────────────────────────────

def is_configured() -> bool:
    """Returns True if an encrypted config file exists and is readable."""
    return CONFIG_FILE.exists()


def load_config() -> dict:
    """
    Decrypts and returns the stored configuration dictionary.

    Returns:
        dict with keys matching REQUIRED_KEYS

    Raises:
        FileNotFoundError: config file does not exist
        RuntimeError:      decryption failed (wrong machine or corrupted file)
    """
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            f"CONFIG_ERROR: No config file at {CONFIG_FILE}. "
            "Run initial setup first."
        )
    key      = _derive_key(_get_machine_guid())
    fernet   = Fernet(key)
    raw      = CONFIG_FILE.read_bytes()
    try:
        decrypted = fernet.decrypt(raw)
    except InvalidToken:
        raise RuntimeError(
            "CONFIG_ERROR: Could not decrypt config. "
            "File may be from a different machine or corrupted."
        )
    return json.loads(decrypted.decode())


def save_config(data: dict) -> None:
    """
    Encrypts and saves configuration to disk.

    Args:
        data: dict containing at minimum all REQUIRED_KEYS

    Raises:
        ValueError: missing required keys
        RuntimeError: encryption failure
    """
    missing = [k for k in REQUIRED_KEYS if not data.get(k)]
    if missing:
        raise ValueError(
            f"CONFIG_ERROR: Missing required keys: {', '.join(missing)}"
        )
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    key     = _derive_key(_get_machine_guid())
    fernet  = Fernet(key)
    payload = json.dumps(data).encode()
    CONFIG_FILE.write_bytes(fernet.encrypt(payload))


def wipe_config() -> None:
    """
    Securely removes the config file.
    Called on uninstall or explicit reset.
    """
    if CONFIG_FILE.exists():
        # Overwrite with zeros before deleting
        size = CONFIG_FILE.stat().st_size
        CONFIG_FILE.write_bytes(b'\x00' * size)
        CONFIG_FILE.unlink()


def validate_config(data: dict) -> list[str]:
    """
    Returns a list of missing or empty required keys.

    Args:
        data: config dict to validate

    Returns:
        list of missing key names (empty list = all good)
    """
    return [k for k in REQUIRED_KEYS if not data.get(k, "").strip()]
