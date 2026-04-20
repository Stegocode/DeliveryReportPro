# DeliveryReportPro

A distributable Windows desktop application that automates a dealer's end-of-day financial reporting. Scrapes exports from the Homesource dealer management system, calculates per-stop delivery costs across two different carrier pricing models, and produces a formatted Excel financial report with routing visualization.

Built as a single-file `.exe` installer with no admin rights required, encrypted credential storage, a web-based UI running on localhost, and a live-progress WebSocket feed so the operator sees exactly what the pipeline is doing and why something failed.

---

## What it does

Replaces an 8–10 minute manual workflow that an operations coordinator ran every morning: logging into Homesource, exporting four reports, pulling crate status from Monday.com, looking up diesel prices, calculating per-stop fuel and labor costs for both third-party-carrier routes and the dealer's own-fleet routes, and manually assembling a margin analysis spreadsheet.

Now it's one click. The user picks a delivery date, optionally adjusts the diesel price, and hits Run. The pipeline runs through ten steps, streaming progress to the UI. Eight to ten minutes later, a formatted Excel report lands in `%APPDATA%\DeliveryReportPro\exports\`.

Pipeline stages:

| Step    | What it does                                          | Approx time |
|---------|-------------------------------------------------------|-------------|
| login   | Signs in to Homesource                                | ~10s        |
| bulk    | Downloads batch invoice (all trucks, selected date)   | ~15s        |
| serial  | Downloads full serial inventory (~88k rows)           | ~5 min      |
| orders  | Downloads orders detail CSV                           | ~20s        |
| route   | Downloads route sheet PDF                             | ~5s         |
| monday  | Fetches crate status from Monday Delivery Scheduler   | ~15s        |
| diesel  | EIA API → Google fallback → manual entry              | ~3s         |
| charges | Geocodes addresses, calculates fuel/labor/piece costs | ~2 min      |
| build   | Assembles report rows (products, services, RMA, etc.) | ~5s         |
| excel   | Writes formatted `.xlsx` to exports folder            | ~5s         |

The Excel report color-codes rows by bucket type:

| Bucket          | Color  | Meaning                                      |
|-----------------|--------|----------------------------------------------|
| Delivery        | White  | Standard delivery stop                       |
| Will Call       | Green  | Customer pickup / drop ship / transfer       |
| Storage Release | Yellow | Previously invoiced, releasing from storage  |
| RMA             | Purple | Return + replacement (vendor credit assumed) |

---

## Why it's interesting

This is a real operational tool solving a real bottleneck, but it's also a deliberate exercise in shipping something a non-technical operator can actually use day-to-day. A few things worth a closer look:

**Packaged as a real distributable.** PyInstaller builds a single-folder Windows executable, and Inno Setup wraps that into an installer that drops into `%LOCALAPPDATA%` — no admin rights, no UAC prompts, no IT ticket. The installer also checks for Chrome at pre-install and creates the APPDATA config directory post-install. Zero Python dependency on the target machine.

**Machine-bound encrypted credentials.** API keys and Homesource login live in `%APPDATA%\DeliveryReportPro\config.enc`, encrypted with Fernet (AES-128-CBC + HMAC). The key isn't stored anywhere — it's derived via PBKDF2 (SHA256, 480k iterations) from the Windows machine GUID. If the config file gets copied to another machine, it won't decrypt. Credentials are also zeroed from `os.environ` after each pipeline run.

**Live progress with interactive prompts.** The Flask server streams every step's status over WebSocket. When the pipeline needs an answer (e.g. the diesel-price API failed, operator needs to confirm manually), it emits a `prompt` event and blocks on a `threading.Event` until the UI responds. No polling, no page reloads.

**Dual pricing models for mixed delivery operations.** The business logic handles two distinct carrier models in the same report: a third-party carrier (fuel surcharge + piece charges + crate fees via Monday.com) and the dealer's own-fleet trucks (per-route diesel cost distributed across stops, with a day-rate fallback for multi-truck days). The report surfaces margin at the stop level under either model.

**Structured error reporting.** Every pipeline step reports into a typed `ErrorRegistry` with FATAL / WARNING / INFO severity and machine-readable error codes (`SERIAL_DOWNLOAD_TIMEOUT`, `LOGIN_FAIL`, etc.). When something fails, the UI shows a copy-to-clipboard report the operator can send to the developer — instead of a screenshot of a crashed terminal.

---

## Install (end user, on a Windows machine)

1. Download `DeliveryReportPro_Setup.exe` from the latest release.
2. Double-click to install. No admin rights needed.
3. Launch DeliveryReportPro from the Start Menu or Desktop shortcut.
4. On first run, enter credentials (Homesource login, Google API key, Monday.com token, OpenRouteService key). These are encrypted and saved to `%APPDATA%\DeliveryReportPro\config.enc`.
5. Pick a date, click Run.

Requirements:
- Windows 10 or 11
- Google Chrome installed (any recent version)

---

## Run from source (developer)

```
git clone https://github.com/Stegocode/DeliveryReportPro.git
cd DeliveryReportPro
pip install -r requirements.txt
python app.py
```

The app opens automatically at http://localhost:5173. First run shows the credentials setup screen.

---

## Build the distributable (developer)

From the `DeliveryReportPro\` directory on a Windows dev machine:

```
build.bat
```

This runs three steps:
1. Installs/updates dependencies via pip
2. Packages the app into `dist\DeliveryReportPro\DeliveryReportPro.exe` via PyInstaller
3. Builds `installer_output\DeliveryReportPro_Setup.exe` via Inno Setup 6

If Inno Setup isn't installed, step 3 is skipped and you can still ship the `dist\DeliveryReportPro\` folder as a zip.

Before running `build.bat` for the first time, edit `inno_setup.iss` and set `AppPublisher` and `AppURL` to your actual publisher name and URL.

---

## Run tests

```
# Unit tests only — no input files needed
python -m pytest test_generator.py -v -k "not Integration"

# Full integration tests — requires export files in tests/fixtures/
python -m pytest test_generator.py -v
```

---

## Project structure

```
DeliveryReportPro/
├── app.py                        Flask + SocketIO server, HTTP routes, pipeline state
├── pipeline.py                   Orchestrator, error registry, progress emitter
├── config.py                     AES-256 encrypted config, machine-key bound
├── financial_scraper_app.py      Homesource scraper (Selenium)
├── financial_generator_app.py    Report builder (pandas + openpyxl)
├── chromedriver_manager.py       ChromeDriver version-matching + download
├── test_generator.py             Unit + integration tests
├── requirements.txt
├── DeliveryReportPro.spec        PyInstaller build config
├── inno_setup.iss                Windows installer config
├── build.bat                     Three-stage build script
├── templates/                    Jinja2 templates (UI)
└── static/                       CSS / JS for the UI
```

---

## Configuration

On first run, the setup screen collects six values stored encrypted on disk:

| Key                | What it's for |
| ------------------ | ------------- |
| `HS_USERNAME`      | Homesource login email |
| `HS_PASSWORD`      | Homesource password |
| `HS_BASE_URL`      | Your Homesource subdomain, e.g. `https://acme1.homesourcesystems.com` |
| `GOOGLE_API_KEY`   | Geocoding + fallback diesel price lookup |
| `MONDAY_API_TOKEN` | Monday.com GraphQL access for crate status |
| `ORS_API_KEY`      | OpenRouteService for routing distance |

Business-logic constants (own-fleet truck numbers, will-call types, third-party carrier list, minimum charges, etc.) live near the top of `financial_generator_app.py` and are meant to be edited directly for a different dealer.

Credentials live at `%APPDATA%\DeliveryReportPro\config.enc`. To reset: delete the file and re-enter credentials on next launch.

---

## Known issues / not yet checked in

The initial commit contains the Python backend, the installer scripts, and the build config. A few pieces of the repo still need to be added before the app runs end-to-end:

- **`templates/index.html`** — single-page UI that the Flask app renders at `/`. `app.py` calls `render_template("index.html", ...)` on the root route, so the server will 500 on the home page until this exists.
- **`static/`** — CSS and JS for the UI. The `DeliveryReportPro.spec` PyInstaller build config and Flask's default static routing both expect this directory to exist. PyInstaller will fail at build time if it's missing.
- **`update_credentials` function in `config.py`** — referenced by `app.py`'s `/api/update-credentials` endpoint but not yet defined. The UI's credentials-update flow is non-functional until it's added.
- **`tests/fixtures/`** — integration tests expect export files dropped here. Unit tests (`-k "not Integration"`) run without it.

---

## License

MIT — see [LICENSE](LICENSE).
