# -*- coding: utf-8 -*-
"""
chromedriver_manager.py
========================
Detects the installed Chrome version from the Windows registry,
then downloads the matching ChromeDriver binary to:
  %APPDATA%\\DeliveryReportPro\\drivers\\chromedriver.exe

No PATH modification, no admin rights required.
Called once at app startup; re-runs if version mismatch detected.

Error codes:
  CHROME_NOT_FOUND     — Chrome not installed or not detectable
  DRIVER_DOWNLOAD_FAIL — Network error fetching ChromeDriver
  DRIVER_UNZIP_FAIL    — Archive extraction failed
"""

import os
import re
import sys
import json
import zipfile
import platform
import subprocess
import winreg
from pathlib import Path
from typing import Optional

import requests

DRIVER_DIR  = Path(os.environ.get("APPDATA", "")) / "DeliveryReportPro" / "drivers"
DRIVER_PATH = DRIVER_DIR / "chromedriver.exe"
VERSION_FILE = DRIVER_DIR / "chromedriver_version.txt"

# Chrome for Testing JSON endpoint (Google's official source)
CFT_ENDPOINT = "https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json"


# ── Chrome detection ───────────────────────────────────────────────

def _chrome_version_from_registry() -> Optional[str]:
    """
    Reads Chrome version from Windows registry.
    Checks both HKCU and HKLM, and both 32/64-bit hives.

    Returns:
        Version string e.g. "124.0.6367.91" or None if not found.

    Error code: CHROME_NOT_FOUND
    """
    reg_paths = [
        (winreg.HKEY_CURRENT_USER,
         r"Software\Google\Chrome\BLBeacon"),
        (winreg.HKEY_LOCAL_MACHINE,
         r"Software\Google\Chrome\BLBeacon"),
        (winreg.HKEY_LOCAL_MACHINE,
         r"Software\Wow6432Node\Google\Chrome\BLBeacon"),
        (winreg.HKEY_CURRENT_USER,
         r"Software\Google\Update\Clients\{8A69D345-D564-463c-AFF1-A69D9E530F96}"),
        (winreg.HKEY_LOCAL_MACHINE,
         r"Software\Google\Update\Clients\{8A69D345-D564-463c-AFF1-A69D9E530F96}"),
    ]
    for hive, path in reg_paths:
        try:
            with winreg.OpenKey(hive, path) as key:
                version, _ = winreg.QueryValueEx(key, "version")
                if version and re.match(r'\d+\.\d+\.\d+\.\d+', str(version)):
                    return str(version).strip()
        except (FileNotFoundError, OSError, Exception):
            continue
    return None


def _chrome_version_from_exe() -> Optional[str]:
    """
    Fallback: finds chrome.exe and reads its file version.
    Checks common installation paths.
    """
    paths = [
        Path(os.environ.get("PROGRAMFILES", "")) / "Google/Chrome/Application/chrome.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Google/Chrome/Application/chrome.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Google/Chrome/Application/chrome.exe",
    ]
    for p in paths:
        if p.exists():
            try:
                result = subprocess.check_output(
                    f'wmic datafile where name="{str(p).replace(chr(92), chr(92)*2)}" get Version /value',
                    shell=True, stderr=subprocess.DEVNULL, timeout=10
                ).decode()
                m = re.search(r'Version=(\d+\.\d+\.\d+\.\d+)', result)
                if m:
                    return m.group(1)
            except Exception:
                continue
    return None


def get_chrome_version() -> Optional[str]:
    """
    Returns the installed Chrome major.minor.build.patch version string.
    Tries registry first, falls back to exe inspection.

    Returns:
        "124.0.6367.91" style string, or None.
    """
    return _chrome_version_from_registry() or _chrome_version_from_exe()


def _chrome_major(version: str) -> int:
    """Extracts the major version number from a version string."""
    return int(version.split('.')[0])


# ── Driver version matching ────────────────────────────────────────

def _find_matching_driver_url(chrome_version: str) -> Optional[str]:
    """
    Queries the Chrome for Testing JSON endpoint to find the ChromeDriver
    download URL that matches the installed Chrome version.

    Matching strategy:
      1. Exact version match
      2. Same major.minor.build (last segment may differ)
      3. Same major version, closest build

    Args:
        chrome_version: full version string e.g. "124.0.6367.91"

    Returns:
        Download URL for chromedriver-win64.zip, or None.

    Error code: DRIVER_DOWNLOAD_FAIL
    """
    try:
        r = requests.get(CFT_ENDPOINT, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        raise RuntimeError(
            f"DRIVER_DOWNLOAD_FAIL: Could not fetch ChromeDriver manifest: {e}"
        )

    versions = data.get("versions", [])
    chrome_major = _chrome_major(chrome_version)
    chrome_parts = tuple(int(x) for x in chrome_version.split('.'))

    # Build lookup: version_string → win64 download url
    candidates = []
    for entry in versions:
        v = entry.get("version", "")
        downloads = entry.get("downloads", {}).get("chromedriver", [])
        for dl in downloads:
            if dl.get("platform") == "win64":
                try:
                    v_parts = tuple(int(x) for x in v.split('.'))
                    if v_parts[0] == chrome_major:
                        candidates.append((v_parts, dl["url"]))
                except Exception:
                    continue

    if not candidates:
        raise RuntimeError(
            f"DRIVER_DOWNLOAD_FAIL: No ChromeDriver found for Chrome major version {chrome_major}"
        )

    # Sort by version, pick closest to chrome_version
    candidates.sort(key=lambda x: x[0])

    # Prefer exact match
    for v_parts, url in candidates:
        if v_parts == chrome_parts:
            return url

    # Prefer same major.minor.build
    for v_parts, url in candidates:
        if v_parts[:3] == chrome_parts[:3]:
            return url

    # Fall back to highest version in same major
    return candidates[-1][1]


# ── Download + extract ─────────────────────────────────────────────

def _download_driver(url: str) -> None:
    """
    Downloads and extracts chromedriver.exe to DRIVER_DIR.

    Args:
        url: download URL for chromedriver-win64.zip

    Raises:
        RuntimeError: DRIVER_DOWNLOAD_FAIL or DRIVER_UNZIP_FAIL
    """
    DRIVER_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = DRIVER_DIR / "chromedriver_tmp.zip"

    try:
        r = requests.get(url, timeout=60, stream=True)
        r.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception as e:
        raise RuntimeError(f"DRIVER_DOWNLOAD_FAIL: Download failed: {e}")

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # The zip contains chromedriver-win64/chromedriver.exe
            for member in z.namelist():
                if member.endswith('chromedriver.exe'):
                    data = z.read(member)
                    DRIVER_PATH.write_bytes(data)
                    break
            else:
                raise RuntimeError(
                    "DRIVER_UNZIP_FAIL: chromedriver.exe not found in archive"
                )
    except zipfile.BadZipFile as e:
        raise RuntimeError(f"DRIVER_UNZIP_FAIL: Bad zip archive: {e}")
    finally:
        try:
            zip_path.unlink()
        except Exception:
            pass


# ── Version cache ──────────────────────────────────────────────────

def _cached_driver_version() -> Optional[str]:
    """Returns the version of the currently cached ChromeDriver, or None."""
    if VERSION_FILE.exists():
        return VERSION_FILE.read_text().strip()
    return None


def _save_driver_version(chrome_version: str) -> None:
    VERSION_FILE.write_text(chrome_version)


def _driver_needs_update(chrome_version: str) -> bool:
    """
    Returns True if no driver exists or if the cached driver's major version
    doesn't match the installed Chrome major version.
    """
    if not DRIVER_PATH.exists():
        return True
    cached = _cached_driver_version()
    if not cached:
        return True
    try:
        return _chrome_major(cached) != _chrome_major(chrome_version)
    except Exception:
        return True


# ── Public API ─────────────────────────────────────────────────────

def ensure_chromedriver(emit_log=None) -> str:
    """
    Ensures a matching ChromeDriver is available at DRIVER_PATH.
    Downloads/updates if needed.

    Args:
        emit_log: optional callable(message, level) for UI progress

    Returns:
        Absolute path to chromedriver.exe as a string.

    Raises:
        RuntimeError: with error code prefix if Chrome not found or download fails.
    """
    def log(msg, level='info'):
        if emit_log:
            emit_log(msg, level)

    # Detect Chrome
    chrome_version = get_chrome_version()
    if not chrome_version:
        raise RuntimeError(
            "CHROME_NOT_FOUND: Google Chrome does not appear to be installed. "
            "Please install Chrome and try again."
        )

    log(f"Chrome detected: v{chrome_version}")

    # Check if update needed
    if not _driver_needs_update(chrome_version):
        log(f"ChromeDriver up to date (v{_cached_driver_version()})")
        return str(DRIVER_PATH)

    log(f"Downloading ChromeDriver for Chrome {chrome_version}…")

    url = _find_matching_driver_url(chrome_version)
    _download_driver(url)
    _save_driver_version(chrome_version)

    log(f"ChromeDriver ready at {DRIVER_PATH}", 'good')
    return str(DRIVER_PATH)


def get_driver_path() -> str:
    """
    Returns path to the cached ChromeDriver without checking for updates.
    Call ensure_chromedriver() first on startup.
    """
    return str(DRIVER_PATH)
